import argparse
import asyncio
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

import websockets

DEFAULT_ENDPOINT = "wss://spro.agency/api/playbyplay"
GAMES_ENDPOINT = "https://spro.agency/api/get_games"
API_KEY_ENV = "BOLTODDS_API_KEY"
# Run: BOLTODDS_API_KEY=... python main.py


def load_env_file(path: str = ".env") -> None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        return


def build_ws_url(endpoint: str, api_key: str) -> str:
    separator = "&" if "?" in endpoint else "?"
    return f"{endpoint}{separator}key={api_key}"


def format_new_play(message: str) -> str | None:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if payload.get("action") != "new_play":
        return None

    home = payload.get("home")
    away = payload.get("away")
    score = payload.get("score")
    play_info = payload.get("play_info")

    if (
        not home
        or not away
        or not isinstance(score, dict)
        or not isinstance(play_info, list)
        or not play_info
    ):
        return None

    home_score = score.get("home")
    away_score = score.get("away")
    play = play_info[0]
    if not isinstance(play, dict):
        return None

    play_type = play.get("type")
    play_team = play.get("team")
    play_points = play.get("points")
    play_seconds = play.get("seconds")
    play_name = play.get("name")

    if (
        home_score is None
        or away_score is None
        or play_type is None
        or play_team is None
        or play_points is None
        or play_seconds is None
        or play_name is None
    ):
        return None

    return (
        f"{home}: {home_score}, {away}: {away_score} | "
        f"{play_type} {play_team} {play_points} {play_seconds} {play_name}"
    )


def fetch_games(api_key: str) -> list[str]:
    url = f"{GAMES_ENDPOINT}?key={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to fetch games: {exc}") from exc

    if isinstance(payload, dict) and "games" in payload:
        games = payload["games"]
    else:
        games = payload

    if not isinstance(games, list):
        raise RuntimeError("Unexpected games payload format")

    return [str(game) for game in games]


def choose_game(games: list[str] | None, exact_name: str | None) -> str:
    if exact_name:
        if not games or exact_name in games:
            return exact_name
        raise RuntimeError("Game not found. Use exact name from the list.")

    if games:
        print("Available games:", flush=True)
        for game in games:
            print(f"- {game}", flush=True)
    else:
        print(
            "Unable to fetch game list; enter the exact game name anyway.",
            flush=True,
        )

    selection = input("Type the exact game name to subscribe: ").strip()
    if not games or selection in games:
        return selection

    raise RuntimeError("Game not found. Use exact name from the list.")


async def stream_scores(ws_url: str, game_name: str | None) -> None:
    backoff = 0.5
    backoff_max = 10.0
    policy_backoff = 30.0

    while True:
        err: Exception | None = None
        try:
            print(f"Connecting to {ws_url} ...", flush=True)
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                compression=None,
            ) as websocket:
                print("Connected. Streaming live updates...", flush=True)
                backoff = 0.5
                if game_name:
                    subscribe_message = {
                        "action": "subscribe",
                        "filters": {"games": [game_name]},
                    }
                    await websocket.send(json.dumps(subscribe_message))
                async for message in websocket:
                    formatted = format_new_play(message)
                    if formatted:
                        print(formatted, flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            err = exc
            print(f"Connection error: {exc}", file=sys.stderr, flush=True)

        if err is None:
            continue

        error_text = str(err).lower()
        if "too many concurrent connections" in error_text or "http 503" in error_text:
            sleep_for = policy_backoff + random.uniform(0, policy_backoff)
        else:
            sleep_for = backoff + random.uniform(0, backoff)
        time.sleep(sleep_for)
        backoff = min(backoff * 2, backoff_max)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BoltOdds live scores stream")
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"WebSocket endpoint (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--key",
        default=os.getenv(API_KEY_ENV, ""),
        help=f"API key (default: from {API_KEY_ENV})",
    )
    parser.add_argument(
        "--game",
        default="",
        help="Exact game name to subscribe (if omitted, prompt)",
    )
    return parser.parse_args()


def main() -> None:
    load_env_file()
    args = parse_args()
    if not args.key:
        print(
            f"Missing API key. Set {API_KEY_ENV} or pass --key.",
            file=sys.stderr,
        )
        sys.exit(1)

    ws_url = build_ws_url(args.endpoint, args.key)
    try:
        games = fetch_games(args.key)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        games = None

    try:
        game_name = choose_game(games, args.game or None)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    try:
        asyncio.run(stream_scores(ws_url, game_name))
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)


if __name__ == "__main__":
    main()
