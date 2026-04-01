"""
Live pitch-by-pitch listener using mlb-statsapi-pydantic async client.
Polls active games and writes new pitch events to Parquet files.

Currently uses HTTP polling. The async architecture is designed so that
switching to a websocket transport in the future requires minimal changes.
"""

import argparse
import asyncio
import signal
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from mlb_statsapi import AsyncMlbClient
from mlb_statsapi.models.livefeed import LiveFeedResponse, Play, PlayEvent

from mlb_config import LIVE_DATA_DIR, TEAM_MAPPING

POLL_INTERVAL = 15  # seconds between pitch polls per game
SCHEDULE_INTERVAL = 300  # seconds between schedule refreshes


# ---------------------------------------------------------------------------
# Pitch flattening
# ---------------------------------------------------------------------------

def flatten_pitch(game_id: int, play: Play, event: PlayEvent) -> dict:
    """Convert a typed pitch event into a flat dict for Parquet storage."""
    about = play.about
    matchup = play.matchup
    result = play.result
    details = event.details
    pd_ = event.pitch_data
    coordinates = pd_.coordinates if pd_ else None
    breaks = pd_.breaks if pd_ else None
    count = event.count
    hit = event.hit_data

    # reviewDetails is not yet modeled on PlayEvent — access via model_extra
    # https://github.com/DarkSideOfTheMat/mlb-statsapi-pydantic/issues
    review = (event.model_extra or {}).get("reviewDetails", {})

    pitch_type = details.type if details else None
    call = details.call if details else None

    return {
        "game_id": game_id,
        "play_index": play.at_bat_index,
        "pitch_index": event.index,
        "pitch_number": event.pitch_number,
        "timestamp": event.start_time.isoformat() if event.start_time else None,
        "inning": about.inning if about else None,
        "half_inning": str(about.half_inning) if about and about.half_inning else None,
        "batter_id": matchup.batter.id if matchup else None,
        "batter_name": matchup.batter.full_name if matchup else None,
        "pitcher_id": matchup.pitcher.id if matchup else None,
        "pitcher_name": matchup.pitcher.full_name if matchup else None,
        "bat_side": matchup.bat_side.code if matchup and matchup.bat_side else None,
        "pitch_hand": matchup.pitch_hand.code if matchup and matchup.pitch_hand else None,
        "pitch_type": pitch_type.code if pitch_type else None,
        "pitch_description": pitch_type.description if pitch_type else None,
        "call_code": call.code if call else None,
        "call_description": call.description if call else None,
        "start_speed": pd_.start_speed if pd_ else None,
        "end_speed": pd_.end_speed if pd_ else None,
        "zone": pd_.zone if pd_ else None,
        "plate_x": coordinates.p_x if coordinates else None,
        "plate_z": coordinates.p_z if coordinates else None,
        "pfx_x": coordinates.pfx_x if coordinates else None,
        "pfx_z": coordinates.pfx_z if coordinates else None,
        "extension": pd_.extension if pd_ else None,
        "spin_rate": breaks.spin_rate if breaks else None,
        "spin_direction": breaks.spin_direction if breaks else None,
        "break_angle": breaks.break_angle if breaks else None,
        "break_length": breaks.break_length if breaks else None,
        "break_vertical": breaks.break_vertical if breaks else None,
        "break_vertical_induced": breaks.break_vertical_induced if breaks else None,
        "break_horizontal": breaks.break_horizontal if breaks else None,
        "balls": count.balls if count else None,
        "strikes": count.strikes if count else None,
        "outs": count.outs if count else None,
        "is_in_play": details.is_in_play if details else None,
        "is_strike": details.is_strike if details else None,
        "is_ball": details.is_ball if details else None,
        "launch_speed": hit.launch_speed if hit else None,
        "launch_angle": hit.launch_angle if hit else None,
        "hit_distance": hit.total_distance if hit else None,
        "hit_trajectory": str(hit.trajectory) if hit and hit.trajectory else None,
        "at_bat_event": result.event if result else None,
        "at_bat_event_type": str(result.event_type) if result and result.event_type else None,
        "at_bat_description": result.description if result else None,
        "has_review": details.has_review if details else False,
        "review_overturned": review.get("isOverturned"),
        "review_in_progress": review.get("inProgress"),
        "challenge_team_id": review.get("challengeTeamId"),
        "challenger_id": review.get("player", {}).get("id"),
        "challenger_name": review.get("player", {}).get("fullName"),
    }


def extract_pitches(feed: LiveFeedResponse, game_id: int) -> list[dict]:
    """Extract all pitch events from a live feed response."""
    rows = []
    if feed.live_data and feed.live_data.plays:
        for play in feed.live_data.plays.all_plays:
            for pitch in play.pitches:
                rows.append(flatten_pitch(game_id, play, pitch))
    return rows


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def parquet_path(game_id: int) -> Path:
    return LIVE_DATA_DIR / f"game_{game_id}.parquet"


def load_known_keys(game_id: int) -> set[tuple[int, int]]:
    path = parquet_path(game_id)
    if path.exists():
        df = pd.read_parquet(path, columns=["play_index", "pitch_index"])
        return set(zip(df["play_index"], df["pitch_index"]))
    return set()


def write_parquet(game_id: int, new_rows: list[dict]):
    if not new_rows:
        return
    path = parquet_path(game_id)
    new_df = pd.DataFrame(new_rows)
    if path.exists():
        existing_df = pd.read_parquet(path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    tmp = path.with_suffix(".parquet.tmp")
    combined_df.to_parquet(tmp, index=False)
    tmp.rename(path)


# ---------------------------------------------------------------------------
# Game monitor
# ---------------------------------------------------------------------------

ACTIVE_STATES = frozenset({"In Progress", "Warmup", "Pre-Game", "Manager Challenge"})
FINAL_STATES = frozenset({"Final", "Game Over", "Completed Early"})


class GameMonitor:
    """Tracks state for a single game being polled."""

    def __init__(self, game_pk: int, away: str, home: str, status: str):
        self.game_pk = game_pk
        self.away = away
        self.home = home
        self.status = status
        self.known_keys = load_known_keys(game_pk)
        self.total_new = 0
        self.errors = 0

    @property
    def label(self) -> str:
        return f"[{self.game_pk}] {self.away} @ {self.home}"

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_STATES

    @property
    def is_final(self) -> bool:
        return self.status in FINAL_STATES

    def update_status(self, new_status: str):
        if new_status != self.status:
            print(f"  {self.label}: {self.status} -> {new_status}")
            self.status = new_status

    async def poll(self, client: AsyncMlbClient) -> int:
        """Fetch new pitches and write them to parquet. Returns count of new pitches."""
        try:
            feed = await client.game(game_pk=self.game_pk)
            new_rows = []
            if feed.live_data and feed.live_data.plays:
                for play in feed.live_data.plays.all_plays:
                    for pitch in play.pitches:
                        key = (play.at_bat_index, pitch.index)
                        if key not in self.known_keys:
                            new_rows.append(flatten_pitch(self.game_pk, play, pitch))

            if new_rows:
                write_parquet(self.game_pk, new_rows)
                for row in new_rows:
                    self.known_keys.add((row["play_index"], row["pitch_index"]))
                self.total_new += len(new_rows)
                print(f"  {self.label}: +{len(new_rows)} pitches ({len(self.known_keys)} total)")
            return len(new_rows)
        except Exception as e:
            self.errors += 1
            print(f"  {self.label}: ERROR polling: {e}")
            return 0

    @classmethod
    def from_live_feed(cls, feed: LiveFeedResponse) -> "GameMonitor":
        gd = feed.game_data
        return cls(
            game_pk=int(feed.game_pk),
            away=gd.teams.away.name if gd.teams and gd.teams.away else "?",
            home=gd.teams.home.name if gd.teams and gd.teams.home else "?",
            status=gd.status.detailed_state if gd.status else "Unknown",
        )


# ---------------------------------------------------------------------------
# Polling loop for a single game
# ---------------------------------------------------------------------------

async def poll_game(monitor: GameMonitor, client: AsyncMlbClient, shutdown: asyncio.Event):
    print(f"  {monitor.label}: Starting poll (status: {monitor.status})")

    while not monitor.is_final and not shutdown.is_set():
        await monitor.poll(client)
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=POLL_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

    if not shutdown.is_set():
        await monitor.poll(client)
    print(
        f"  {monitor.label}: FINAL - {monitor.total_new} new pitches, "
        f"{len(monitor.known_keys)} total, {monitor.errors} errors"
    )


# ---------------------------------------------------------------------------
# Schedule helpers
# ---------------------------------------------------------------------------

def _date_to_mlb_format(iso_date: str) -> str:
    """Convert YYYY-MM-DD to MM/DD/YYYY for the MLB Stats API."""
    y, m, d = iso_date.split("-")
    return f"{m}/{d}/{y}"


async def fetch_schedule(
    client: AsyncMlbClient,
    target_date: str,
    team_filter: str | None = None,
) -> list[GameMonitor]:
    """Fetch today's schedule and return GameMonitor instances."""
    mlb_date = _date_to_mlb_format(target_date)

    team_id = None
    if team_filter:
        team_info = TEAM_MAPPING.get(team_filter)
        if not team_info:
            print(f"Unknown team key: {team_filter}")
            sys.exit(1)
        team_id = team_info["statsapi_id"]

    schedule = await client.schedule(date=mlb_date, team_id=team_id)

    monitors = []
    for d in schedule.dates:
        for game in d.games:
            status = game.status.detailed_state or game.status.abstract_game_state or "Unknown"
            monitors.append(GameMonitor(
                game_pk=int(game.game_pk),
                away=game.teams.away.team.name or "?",
                home=game.teams.home.team.name or "?",
                status=status,
            ))
    return monitors


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

async def run_backfill(client: AsyncMlbClient):
    """Rewrite every existing parquet file using the current schema."""
    files = sorted(LIVE_DATA_DIR.glob("game_*.parquet"))
    if not files:
        print("No parquet files found to backfill.")
        return

    print(f"Backfilling {len(files)} game(s)")
    total_pitches = 0
    total_errors = 0
    for path in files:
        game_id = int(path.stem.replace("game_", ""))
        try:
            feed = await client.game(game_pk=game_id)
            monitor = GameMonitor.from_live_feed(feed)
            rows = extract_pitches(feed, game_id)
            if rows:
                tmp = path.with_suffix(".parquet.tmp")
                pd.DataFrame(rows).to_parquet(tmp, index=False)
                tmp.rename(path)
                total_pitches += len(rows)
                print(f"  {monitor.label}: rewrote {len(rows)} pitches")
            else:
                print(f"  {monitor.label}: no pitches returned, skipping")
        except Exception as e:
            total_errors += 1
            print(f"  [{game_id}]: ERROR - {e}")

    print_summary(total_pitches, len(files), total_errors)


async def run_single_game(client: AsyncMlbClient, game_pk: int, shutdown: asyncio.Event):
    """Monitor a single game by gamePk."""
    print(f"Monitoring game {game_pk}...")
    try:
        feed = await client.game(game_pk=game_pk)
        monitor = GameMonitor.from_live_feed(feed)
    except Exception as e:
        print(f"  Warning: could not fetch game info: {e}")
        monitor = GameMonitor(game_pk=game_pk, away="?", home="?", status="In Progress")

    task = asyncio.create_task(poll_game(monitor, client, shutdown))
    try:
        await task
    except asyncio.CancelledError:
        pass

    print_summary(monitor.total_new, 1, monitor.errors)


async def run_schedule(
    client: AsyncMlbClient,
    target_date: str,
    team_filter: str | None,
    shutdown: asyncio.Event,
):
    """Monitor all games on a given date."""
    print(f"Monitoring games for {target_date}")
    if team_filter:
        print(f"  Filtering to team: {team_filter}")

    tasks: dict[int, asyncio.Task] = {}
    monitors: dict[int, GameMonitor] = {}

    while not shutdown.is_set():
        new_monitors = await fetch_schedule(client, target_date, team_filter)

        for m in new_monitors:
            if m.game_pk in monitors:
                monitors[m.game_pk].update_status(m.status)
                continue

            monitors[m.game_pk] = m

            if m.is_active or m.is_final:
                print(f"  Found game: {m.label} ({m.status})")
                tasks[m.game_pk] = asyncio.create_task(poll_game(m, client, shutdown))
            else:
                print(f"  Found game: {m.label} ({m.status}) - waiting")

        for gid, m in monitors.items():
            if m.is_active and gid not in tasks:
                print(f"  {m.label}: Game is now active, starting poll")
                tasks[gid] = asyncio.create_task(poll_game(m, client, shutdown))

        if monitors and all(m.is_final for m in monitors.values()):
            print("\nAll games are final.")
            break

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=SCHEDULE_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

    pending = [t for t in tasks.values() if not t.done()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    total_pitches = sum(m.total_new for m in monitors.values())
    total_errors = sum(m.errors for m in monitors.values())
    print_summary(total_pitches, len(monitors), total_errors)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(total_pitches: int, game_count: int, total_errors: int):
    print(f"\n=== Summary ===")
    print(f"\nTotal: {total_pitches} pitches across {game_count} games, {total_errors} errors")
    print(f"Parquet files: {LIVE_DATA_DIR}/")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Live MLB pitch-by-pitch listener")
    parser.add_argument("--team", type=str, help="Team key to filter (e.g. SFG)")
    parser.add_argument("--date", type=str, help="Date to monitor (YYYY-MM-DD, default: today)")
    parser.add_argument("--game", type=int, help="Specific gamePk to monitor")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Rewrite all existing parquet files using the current schema",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    LIVE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    shutdown = asyncio.Event()

    def handle_signal():
        print("\nShutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    async with AsyncMlbClient() as client:
        if args.backfill:
            await run_backfill(client)
        elif args.game:
            await run_single_game(client, args.game, shutdown)
        else:
            target_date = args.date or date.today().isoformat()
            await run_schedule(client, target_date, args.team, shutdown)


if __name__ == "__main__":
    asyncio.run(main())
