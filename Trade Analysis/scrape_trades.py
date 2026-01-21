#!/usr/bin/env python3
 
import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
 
try:
    import requests
except ImportError as exc:
    print("Missing dependency: requests. Install with: pip install requests", file=sys.stderr)
    raise SystemExit(1) from exc
 
 
SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
ACTIVITY_URL = "https://data-api.polymarket.com/activity"
MARKET_SLUG_URL = "https://gamma-api.polymarket.com/markets/slug/{}"
 
 
def fetch_search(query: str, timeout: int = 30) -> dict:
    response = requests.get(SEARCH_URL, params={"q": query}, timeout=timeout)
    response.raise_for_status()
    return response.json()
 
 
def extract_markets(payload: dict) -> List[Dict[str, str]]:
    markets: List[Dict[str, str]] = []
    for event in payload.get("events", []):
        event_title = event.get("title") or ""
        event_id = event.get("id") or ""
        for market in event.get("markets", []):
            condition_id = market.get("conditionId")
            if not condition_id:
                continue
            markets.append(
                {
                    "event_id": event_id,
                    "event_title": event_title,
                    "market_id": market.get("id") or "",
                    "condition_id": condition_id,
                    "question": market.get("question") or "",
                    "slug": market.get("slug") or "",
                }
            )
    return markets
 
 
def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."
 
 
def print_markets_table(markets: List[Dict[str, str]]) -> None:
    if not markets:
        print("No markets found for that query.")
        return
 
    max_idx = len(str(len(markets)))
    max_condition = min(66, max(len(m.get("condition_id", "")) for m in markets))
    max_question = min(72, max(len(m.get("question", "")) for m in markets))
    max_event = max(len(m.get("event_title", "")) for m in markets)
 
    header = (
        f"{'Idx':>{max_idx}}  "
        f"{'ConditionId':<{max_condition}}  "
        f"{'Question':<{max_question}}  "
        f"{'Event':<{max_event}}"
    )
    print(header)
    print("-" * len(header))
 
    for idx, market in enumerate(markets, start=1):
        condition_id = truncate(market.get("condition_id", ""), max_condition)
        question = truncate(market.get("question", ""), max_question)
        event_title = market.get("event_title", "")
        print(
            f"{idx:>{max_idx}}  "
            f"{condition_id:<{max_condition}}  "
            f"{question:<{max_question}}  "
            f"{event_title:<{max_event}}"
        )
 
 
def parse_selection(selection: str, markets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    selection = (selection or "").strip()
    if not selection:
        return []
 
    if selection.lower() in {"all", "*"}:
        return list(markets)
 
    by_condition = {m["condition_id"]: m for m in markets}
    chosen = []
    seen = set()
 
    for part in selection.split(","):
        token = part.strip()
        if not token:
            continue
        if token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(markets):
                market = markets[idx - 1]
                condition_id = market["condition_id"]
                if condition_id not in seen:
                    chosen.append(market)
                    seen.add(condition_id)
            else:
                print(f"Skipping invalid index: {token}")
        else:
            market = by_condition.get(token)
            if market:
                if token not in seen:
                    chosen.append(market)
                    seen.add(token)
            else:
                print(f"Skipping unknown conditionId: {token}")
 
    return chosen
 
 
def parse_activity_response(data) -> Tuple[List[dict], Optional[str], Optional[bool]]:
    if isinstance(data, list):
        return data, None, None
 
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected activity response type: {type(data)}")
 
    items = None
    for key in ("activity", "items", "results", "data"):
        if isinstance(data.get(key), list):
            items = data[key]
            break
    if items is None and isinstance(data.get("result"), list):
        items = data["result"]
 
    if items is None:
        raise RuntimeError(
            f"Unexpected activity response shape. Keys: {', '.join(sorted(data.keys()))}"
        )
 
    cursor = None
    for key in ("nextCursor", "next_cursor", "cursor", "next"):
        if data.get(key):
            cursor = data.get(key)
            break
 
    pagination = data.get("pagination") if isinstance(data.get("pagination"), dict) else {}
    if not cursor and pagination:
        for key in ("nextCursor", "next_cursor", "cursor", "next", "nextPageToken"):
            if pagination.get(key):
                cursor = pagination.get(key)
                break
 
    has_more = None
    for key in ("hasMore", "has_more"):
        if key in data:
            has_more = bool(data[key])
            break
    if has_more is None and pagination:
        for key in ("hasMore", "has_more"):
            if key in pagination:
                has_more = bool(pagination[key])
                break
 
    return items, cursor, has_more
 
 
def fetch_activity(
    user: str,
    condition_id: str,
    limit: int = 50,
    max_pages: Optional[int] = None,
    sleep: float = 0.2,
) -> List[dict]:
    all_items = []
    offset = 0
    cursor = None
    seen_cursors = set()
    page = 0
    seen_fingerprints = set()
 
    def page_fingerprint(items: List[dict]):
        if not items:
            return None
        if isinstance(items[0], dict):
            def pick_key(item: dict):
                for key in ("transactionHash", "txHash", "hash", "id", "timestamp"):
                    if key in item:
                        return item.get(key)
                return None
 
            return (pick_key(items[0]), pick_key(items[-1]), len(items))
        return (str(items[0])[:80], str(items[-1])[:80], len(items))
 
    while True:
        params = {
            "limit": limit,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "user": user,
            "market": condition_id,
        }
        if cursor:
            params["cursor"] = cursor
        else:
            params["offset"] = offset
 
        response = requests.get(ACTIVITY_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
 
        items, next_cursor, has_more = parse_activity_response(data)
        if not items:
            break
 
        fingerprint = page_fingerprint(items)
        if cursor is None and fingerprint in seen_fingerprints:
            print("Pagination page repeated; stopping to avoid a loop.")
            break
        seen_fingerprints.add(fingerprint)
 
        all_items.extend(items)
        page += 1
        print(
            f"  page {page}: +{len(items)} rows (total {len(all_items)})",
            flush=True,
        )
 
        if next_cursor:
            if next_cursor in seen_cursors:
                print("Pagination cursor repeated; stopping to avoid a loop.")
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            cursor = None
            if len(items) < limit:
                break
            offset += len(items)
 
        if has_more is False:
            break
 
        if max_pages is not None and page >= max_pages:
            break
 
        if sleep:
            time.sleep(sleep)
 
    return all_items
 
 
def prompt(value: str, default: Optional[str] = None) -> str:
    if default:
        prompt_value = f"{value} [{default}]: "
    else:
        prompt_value = f"{value}: "
    response = input(prompt_value).strip()
    return response if response else (default or "")
 
 
def is_hex_address(value: str) -> bool:
    value = value.strip()
    if not value.startswith("0x") or len(value) != 42:
        return False
    try:
        int(value[2:], 16)
    except ValueError:
        return False
    return True
 
 
def prompt_user_address(default: Optional[str] = None) -> str:
    while True:
        user = prompt("User address", default=default).strip()
        if not user:
            print("User address is required.")
            continue
        if user.lower() in {"q", "quit", "exit"}:
            raise SystemExit(0)
        if is_hex_address(user):
            return user
        print("Invalid user address. Expected a 0x... hex address (e.g. 0xabc...123).")
 
 
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search Polymarket markets, select conditionIds, and pull user activity."
    )
    parser.add_argument("--query", help="Search query for public-search.")
    parser.add_argument("--user", help="User address (0x...).")
    parser.add_argument("--select", help="Comma-separated indices or conditionIds, or 'all'.")
    parser.add_argument("--limit", type=int, default=50, help="Page size for activity requests.")
    parser.add_argument("--max-pages", type=int, help="Optional max pages per market.")
    parser.add_argument(
        "--out",
        default="activity-exports",
        help="Output directory for saved JSON files.",
    )
    parser.add_argument(
        "--combined-out",
        default="activity-exports/all-trades.csv",
        help="Output CSV path for combined activity.",
    )
    parser.add_argument(
        "--no-combined", action="store_true", help="Skip writing the combined CSV."
    )
    parser.add_argument(
        "--no-save", action="store_true", help="Do not write activity files."
    )
    return parser.parse_args()
 
 
def normalize_csv_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    return value
 
 
def timestamp_to_iso(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    return ""
 
 
def parse_iso_datetime(value: str):
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if len(raw) >= 3 and raw[-3] in "+-" and raw[-2:].isdigit():
        raw = raw + ":00"
    elif len(raw) >= 5 and raw[-5] in "+-" and raw[-4:].isdigit() and ":" not in raw[-5:]:
        raw = raw[:-2] + ":" + raw[-2:]
    for fmt in (None, "%Y-%m-%d %H:%M:%S%z"):
        try:
            if fmt:
                return datetime.strptime(raw, fmt)
            return datetime.fromisoformat(raw)
        except ValueError:
            continue
    return None
 
 
def normalize_iso(value: str) -> Optional[str]:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
 
 
def iso_to_epoch(value: str):
    parsed = parse_iso_datetime(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())
 
 
def normalize_trade_timestamp(value):
    if isinstance(value, (int, float)):
        ts = float(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            ts = float(raw)
        else:
            try:
                ts = float(raw)
            except ValueError:
                parsed = parse_iso_datetime(raw)
                if not parsed:
                    return None
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp())
    else:
        return None
 
    if ts > 1_000_000_000_000:
        ts = ts / 1000.0
    if ts <= 0:
        return None
    return int(ts)
 
 
def fetch_market_info_by_slug(market_slug: str, timeout: int = 30) -> Optional[dict]:
    slug = (market_slug or "").strip()
    if not slug:
        return None
    url = MARKET_SLUG_URL.format(quote(slug))
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        print(f"Warning: market info fetch failed for slug {slug}: {exc}")
        return None
 
 
def pick_market_start_time(market_payload: dict) -> Tuple[Optional[str], Optional[str]]:
    if not market_payload:
        return None, None
    for key in ("gameStartTime", "eventStartTime", "startDate", "startDateIso"):
        value = market_payload.get(key)
        if value:
            return value, key
    events = market_payload.get("events")
    if isinstance(events, list) and events:
        event = events[0] if isinstance(events[0], dict) else {}
        for key in ("startTime", "startDate", "eventDate"):
            value = event.get(key)
            if value:
                return value, f"events[0].{key}"
    return None, None
 
 
def pick_market_end_time(market_payload: dict) -> Tuple[Optional[str], Optional[str]]:
    if not market_payload:
        return None, None
    for key in ("endDate", "endDateIso"):
        value = market_payload.get(key)
        if value:
            return value, key
    events = market_payload.get("events")
    if isinstance(events, list) and events:
        event = events[0] if isinstance(events[0], dict) else {}
        for key in ("endDate",):
            value = event.get(key)
            if value:
                return value, f"events[0].{key}"
    return None, None
 
 
def build_csv_columns(rows: List[dict]) -> List[str]:
    preferred = [
        "searchQuery",
        "userAddress",
        "timestamp",
        "timestampIso",
        "tradeTiming",
        "type",
        "side",
        "outcome",
        "outcomeIndex",
        "size",
        "usdcSize",
        "price",
        "marketQuestion",
        "conditionId",
        "marketSlug",
        "marketId",
        "marketStartTime",
        "marketStartTimeIso",
        "marketStartTimeSource",
        "marketEndTime",
        "marketEndTimeIso",
        "marketEndTimeSource",
        "marketEventTitle",
        "transactionHash",
        "asset",
        "proxyWallet",
        "name",
        "pseudonym",
        "title",
        "slug",
        "eventSlug",
        "icon",
        "profileImage",
        "profileImageOptimized",
        "bio",
    ]
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    columns = [key for key in preferred if key in all_keys]
    for key in sorted(all_keys):
        if key not in columns:
            columns.append(key)
    return columns
 
 
def main() -> None:
    args = parse_args()
 
    query = args.query or prompt("Search query")
    if not query:
        print("Search query is required.")
        raise SystemExit(1)
 
    payload = fetch_search(query)
    markets = extract_markets(payload)
    if not markets:
        print("No markets found.")
        raise SystemExit(0)
 
    print_markets_table(markets)
 
    selection = args.select or prompt(
        "Select markets by index or conditionId (comma-separated, or 'all')"
    )
    selected = parse_selection(selection, markets)
    if not selected:
        print("No markets selected.")
        raise SystemExit(0)
 
    if args.user:
        if not is_hex_address(args.user):
            print("Invalid --user value. Expected a 0x... hex address.")
            raise SystemExit(1)
        user = args.user
    else:
        user = prompt_user_address()
    search_query = query
 
    output_dir = Path(args.out)
    if not args.no_save:
        output_dir.mkdir(parents=True, exist_ok=True)
 
    combined_rows = []
    market_info_cache = {}
 
    print(f"Fetching activity for {len(selected)} market(s)...")
    for market in selected:
        condition_id = market["condition_id"]
        question = market.get("question", "")
        event_title = market.get("event_title", "")
        market_id = market.get("market_id", "")
        market_slug = market.get("slug", "")
 
        market_info = None
        cache_key = market_slug.strip()
        if cache_key:
            market_info = market_info_cache.get(cache_key)
            if market_info is None:
                market_info = fetch_market_info_by_slug(cache_key)
                market_info_cache[cache_key] = market_info
        else:
            print(f"Warning: missing market slug for {condition_id}")
 
        start_raw, start_source = pick_market_start_time(market_info or {})
        end_raw, end_source = pick_market_end_time(market_info or {})
        start_iso = normalize_iso(start_raw) if start_raw else None
        end_iso = normalize_iso(end_raw) if end_raw else None
        start_epoch = iso_to_epoch(start_iso) if start_iso else None
        end_epoch = iso_to_epoch(end_iso) if end_iso else None
 
        print(f"- {condition_id} | {question}")
        items = fetch_activity(
            user=user,
            condition_id=condition_id,
            limit=args.limit,
            max_pages=args.max_pages,
        )
        print(f"  Retrieved {len(items)} activity rows.")
 
        for item in items:
            row = dict(item) if isinstance(item, dict) else {"raw": item}
            row["marketQuestion"] = question
            row["marketEventTitle"] = event_title
            row["marketId"] = market_id
            row["marketSlug"] = market_slug
            row["searchQuery"] = search_query
            row["userAddress"] = user
            trade_epoch = normalize_trade_timestamp(row.get("timestamp"))
            row["timestampIso"] = (
                datetime.fromtimestamp(trade_epoch, tz=timezone.utc).isoformat()
                if trade_epoch is not None
                else timestamp_to_iso(row.get("timestamp"))
            )
            row["marketStartTime"] = start_raw or ""
            row["marketStartTimeIso"] = start_iso or ""
            row["marketStartTimeSource"] = start_source or ""
            row["marketEndTime"] = end_raw or ""
            row["marketEndTimeIso"] = end_iso or ""
            row["marketEndTimeSource"] = end_source or ""
 
            if trade_epoch is not None and start_epoch is not None:
                if trade_epoch < start_epoch:
                    trade_timing = "before_start"
                else:
                    if (
                        end_epoch is not None
                        and start_epoch is not None
                        and end_epoch > start_epoch
                        and trade_epoch > end_epoch
                    ):
                        trade_timing = "after_end"
                    else:
                        trade_timing = "during"
            else:
                trade_timing = "unknown"
            row["tradeTiming"] = trade_timing
 
            combined_rows.append(row)
 
        if not args.no_save:
            output_path = output_dir / f"{condition_id}.json"
            payload = {
                "user": user,
                "conditionId": condition_id,
                "question": question,
                "eventTitle": event_title,
                "activity": items,
            }
            output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
 
    if combined_rows and not args.no_combined:
        combined_rows.sort(
            key=lambda row: row.get("timestamp") if isinstance(row.get("timestamp"), (int, float)) else -1,
            reverse=True,
        )
        combined_path = Path(args.combined_out)
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        columns = build_csv_columns(combined_rows)
        with combined_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in combined_rows:
                writer.writerow({key: normalize_csv_value(row.get(key)) for key in columns})
        print(f"Combined CSV written to {combined_path}")
 
    print("Done.")
 
 
if __name__ == "__main__":
    main()