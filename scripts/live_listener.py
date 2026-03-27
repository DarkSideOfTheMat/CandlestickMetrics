"""
Live pitch-by-pitch listener using MLB-StatsAPI.
Polls active games and writes new pitch events to Parquet files.
"""

import argparse
import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

import pandas as pd
import statsapi

from mlb_config import LIVE_DATA_DIR, STATSAPI_ID_TO_KEY, TEAM_MAPPING, SEASON

POLL_INTERVAL = 15  # seconds between pitch polls per game
SCHEDULE_INTERVAL = 300  # seconds between schedule refreshes
MAX_CONCURRENT = 3


def parse_args():
    parser = argparse.ArgumentParser(description="Live MLB pitch-by-pitch listener")
    parser.add_argument("--team", type=str, help="Team key to filter (e.g. SFG)")
    parser.add_argument(
        "--date", type=str, help="Date to monitor (YYYY-MM-DD, default: today)"
    )
    parser.add_argument("--game", type=int, help="Specific gamePk to monitor")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Rewrite all existing parquet files using the current schema",
    )
    return parser.parse_args()


def flatten_pitch_event(game_id: int, play: dict, event: dict) -> dict:
    about = play.get("about", {})
    matchup = play.get("matchup", {})
    result = play.get("result", {})
    details = event.get("details", {})
    pitch_data = event.get("pitchData", {})
    coordinates = pitch_data.get("coordinates", {})
    breaks = pitch_data.get("breaks", {})
    count = event.get("count", {})
    hit_data = event.get("hitData", {})
    pitch_type = details.get("type", {})
    call = details.get("call", {})
    review = event.get("reviewDetails", {})

    return {
        "game_id": game_id,
        "play_index": play.get("atBatIndex"),
        "pitch_index": event.get("index"),
        "pitch_number": event.get("pitchNumber"),
        "timestamp": event.get("startTime"),
        "inning": about.get("inning"),
        "half_inning": about.get("halfInning"),
        "batter_id": matchup.get("batter", {}).get("id"),
        "batter_name": matchup.get("batter", {}).get("fullName"),
        "pitcher_id": matchup.get("pitcher", {}).get("id"),
        "pitcher_name": matchup.get("pitcher", {}).get("fullName"),
        "bat_side": matchup.get("batSide", {}).get("code"),
        "pitch_hand": matchup.get("pitchHand", {}).get("code"),
        "pitch_type": pitch_type.get("code"),
        "pitch_description": pitch_type.get("description"),
        "call_code": call.get("code"),
        "call_description": call.get("description"),
        "start_speed": pitch_data.get("startSpeed"),
        "end_speed": pitch_data.get("endSpeed"),
        "zone": pitch_data.get("zone"),
        "plate_x": coordinates.get("pX"),
        "plate_z": coordinates.get("pZ"),
        "pfx_x": coordinates.get("pfxX"),
        "pfx_z": coordinates.get("pfxZ"),
        "extension": pitch_data.get("extension"),
        "spin_rate": breaks.get("spinRate"),
        "spin_direction": breaks.get("spinDirection"),
        "break_angle": breaks.get("breakAngle"),
        "break_length": breaks.get("breakLength"),
        "break_vertical": breaks.get("breakVertical"),
        "break_vertical_induced": breaks.get("breakVerticalInduced"),
        "break_horizontal": breaks.get("breakHorizontal"),
        "balls": count.get("balls"),
        "strikes": count.get("strikes"),
        "outs": count.get("outs"),
        "is_in_play": details.get("isInPlay"),
        "is_strike": details.get("isStrike"),
        "is_ball": details.get("isBall"),
        "launch_speed": hit_data.get("launchSpeed"),
        "launch_angle": hit_data.get("launchAngle"),
        "hit_distance": hit_data.get("totalDistance"),
        "hit_trajectory": hit_data.get("trajectory"),
        "at_bat_event": result.get("event"),
        "at_bat_event_type": result.get("eventType"),
        "at_bat_description": result.get("description"),
        "has_review": details.get("hasReview", False),
        "review_overturned": review.get("isOverturned"),
        "review_in_progress": review.get("inProgress"),
        "challenge_team_id": review.get("challengeTeamId"),
        "challenger_id": review.get("player", {}).get("id"),
        "challenger_name": review.get("player", {}).get("fullName"),
    }


def get_parquet_path(game_id: int):
    return LIVE_DATA_DIR / f"game_{game_id}.parquet"


def load_known_pitch_keys(game_id: int) -> set:
    path = get_parquet_path(game_id)
    if path.exists():
        df = pd.read_parquet(path, columns=["play_index", "pitch_index"])
        return set(zip(df["play_index"], df["pitch_index"]))
    return set()


def write_pitches_to_parquet(game_id: int, new_rows: list[dict]):
    if not new_rows:
        return
    path = get_parquet_path(game_id)
    tmp_path = path.with_suffix(".parquet.tmp")
    new_df = pd.DataFrame(new_rows)
    if path.exists():
        existing_df = pd.read_parquet(path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    combined_df.to_parquet(tmp_path, index=False)
    tmp_path.rename(path)  # atomic on same filesystem


def fetch_pitches_full(game_id: int, known_keys: set) -> list[dict]:
    data = statsapi.get("game_playByPlay", {"gamePk": game_id})
    new_rows = []
    for play in data.get("allPlays", []):
        for event in play.get("playEvents", []):
            if not event.get("isPitch"):
                continue
            key = (play.get("atBatIndex"), event.get("index"))
            if key in known_keys:
                continue
            new_rows.append(flatten_pitch_event(game_id, play, event))
    return new_rows


def fetch_all_pitches(game_id: int) -> list[dict]:
    """Fetch every pitch for a game, ignoring any existing data."""
    data = statsapi.get("game_playByPlay", {"gamePk": game_id})
    rows = []
    for play in data.get("allPlays", []):
        for event in play.get("playEvents", []):
            if not event.get("isPitch"):
                continue
            rows.append(flatten_pitch_event(game_id, play, event))
    return rows


def backfill_parquet_files():
    """Rewrite every existing parquet file using the current schema."""
    files = sorted(LIVE_DATA_DIR.glob("game_*.parquet"))
    if not files:
        print("No parquet files found to backfill.")
        return

    print(f"Backfilling {len(files)} parquet file(s)...")
    for path in files:
        game_id = int(path.stem.replace("game_", ""))
        try:
            rows = fetch_all_pitches(game_id)
            if rows:
                tmp_path = path.with_suffix(".parquet.tmp")
                pd.DataFrame(rows).to_parquet(tmp_path, index=False)
                tmp_path.rename(path)
                print(f"  game_{game_id}: rewrote {len(rows)} pitches")
            else:
                print(f"  game_{game_id}: no pitches returned from API, skipping")
        except Exception as e:
            print(f"  game_{game_id}: ERROR - {e}")

    print("Backfill complete.")


def fetch_schedule(target_date: str, team_filter: str | None = None) -> list[dict]:
    games = statsapi.schedule(date=target_date)
    if team_filter:
        team_info = TEAM_MAPPING.get(team_filter)
        if not team_info:
            print(f"Unknown team key: {team_filter}")
            sys.exit(1)
        statsapi_id = team_info["statsapi_id"]
        games = [
            g for g in games if g["home_id"] == statsapi_id or g["away_id"] == statsapi_id
        ]
    return games


class GameMonitor:
    def __init__(self, game_info: dict):
        self.game_id = game_info["game_id"]
        self.home = game_info["home_name"]
        self.away = game_info["away_name"]
        self.status = game_info["status"]
        self.known_keys = load_known_pitch_keys(self.game_id)
        self.total_new = 0
        self.errors = 0

    @property
    def label(self):
        return f"[{self.game_id}] {self.away} @ {self.home}"

    @property
    def is_active(self):
        return self.status in ("In Progress", "Warmup", "Pre-Game", "Manager Challenge")

    @property
    def is_final(self):
        return self.status in ("Final", "Game Over", "Completed Early")

    def update_status(self, new_status: str):
        if new_status != self.status:
            print(f"  {self.label}: {self.status} -> {new_status}")
            self.status = new_status

    def poll(self) -> int:
        try:
            new_rows = fetch_pitches_full(self.game_id, self.known_keys)
            if new_rows:
                write_pitches_to_parquet(self.game_id, new_rows)
                for row in new_rows:
                    self.known_keys.add((row["play_index"], row["pitch_index"]))
                self.total_new += len(new_rows)
                print(
                    f"  {self.label}: +{len(new_rows)} pitches "
                    f"({len(self.known_keys)} total)"
                )
            return len(new_rows)
        except Exception as e:
            self.errors += 1
            print(f"  {self.label}: ERROR polling: {e}")
            return 0


async def poll_game(monitor: GameMonitor, executor: ThreadPoolExecutor):
    loop = asyncio.get_event_loop()
    print(f"  {monitor.label}: Starting poll (status: {monitor.status})")

    while not monitor.is_final:
        await loop.run_in_executor(executor, monitor.poll)
        await asyncio.sleep(POLL_INTERVAL)

    # Final fetch to catch any remaining pitches
    await loop.run_in_executor(executor, monitor.poll)
    print(
        f"  {monitor.label}: FINAL - {monitor.total_new} new pitches, "
        f"{len(monitor.known_keys)} total, {monitor.errors} errors"
    )


async def main():
    args = parse_args()
    target_date = args.date or date.today().isoformat()

    LIVE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.backfill:
        backfill_parquet_files()
        return
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
    active_tasks: dict[int, asyncio.Task] = {}
    monitors: dict[int, GameMonitor] = {}
    shutdown = asyncio.Event()

    def handle_signal():
        print("\nShutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    if args.game:
        # Single game mode
        print(f"Monitoring game {args.game}...")
        game_info = {"game_id": args.game, "home_name": "?", "away_name": "?", "status": "In Progress"}
        # Try to get actual game info from schedule
        games = statsapi.schedule(game_id=args.game)
        if games:
            game_info = games[0]
        monitor = GameMonitor(game_info)
        monitors[args.game] = monitor
        task = asyncio.create_task(poll_game(monitor, executor))
        active_tasks[args.game] = task
        try:
            await asyncio.wait([task, asyncio.create_task(shutdown.wait())], return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in active_tasks.values():
                t.cancel()
    else:
        # Schedule monitoring mode
        print(f"Monitoring games for {target_date}")
        if args.team:
            print(f"  Filtering to team: {args.team}")

        while not shutdown.is_set():
            games = fetch_schedule(target_date, args.team)

            for game_info in games:
                gid = game_info["game_id"]

                if gid in monitors:
                    monitors[gid].update_status(game_info["status"])
                    continue

                monitor = GameMonitor(game_info)
                monitors[gid] = monitor

                if monitor.is_active or monitor.is_final:
                    print(f"  Found game: {monitor.label} ({monitor.status})")
                    task = asyncio.create_task(poll_game(monitor, executor))
                    active_tasks[gid] = task
                else:
                    print(f"  Found game: {monitor.label} ({monitor.status}) - waiting")

            # Start any games that just became active
            for gid, monitor in monitors.items():
                if monitor.is_active and gid not in active_tasks:
                    print(f"  {monitor.label}: Game is now active, starting poll")
                    task = asyncio.create_task(poll_game(monitor, executor))
                    active_tasks[gid] = task

            # Check if all games are done
            all_done = monitors and all(m.is_final for m in monitors.values())
            if all_done:
                print("\nAll games are final.")
                break

            # Wait for next schedule check
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=SCHEDULE_INTERVAL)
                break  # shutdown was triggered
            except asyncio.TimeoutError:
                pass  # normal — time for next schedule check

    # Wait for active tasks to finish
    if active_tasks:
        pending = [t for t in active_tasks.values() if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    executor.shutdown(wait=False)

    # Print summary
    print("\n=== Summary ===")
    total_pitches = 0
    total_errors = 0
    for monitor in monitors.values():
        total_pitches += monitor.total_new
        total_errors += monitor.errors
        print(
            f"  {monitor.label}: {monitor.total_new} pitches, "
            f"{monitor.errors} errors ({monitor.status})"
        )
    print(f"\nTotal: {total_pitches} pitches across {len(monitors)} games, {total_errors} errors")
    print(f"Parquet files: {LIVE_DATA_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
