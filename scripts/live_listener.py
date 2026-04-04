"""
Live pitch-by-pitch listener using mlb-statsapi-pydantic async client.
Polls active games and writes new pitch events to Parquet files.

Currently uses HTTP polling. The async architecture is designed so that
switching to a websocket transport in the future requires minimal changes.
"""

import argparse
import asyncio
import hashlib
import signal
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from mlb_statsapi import AsyncMlbClient
from mlb_statsapi.models.game import Boxscore, Linescore
from mlb_statsapi.models.livefeed import LiveFeedResponse, Play, PlayEvent

from mlb_config import (
    LIVE_BOXSCORE_DIR,
    LIVE_DATA_DIR,
    LIVE_GAMES_DIR,
    LIVE_LINESCORE_DIR,
    TEAM_MAPPING,
)

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


def extract_pitches(
    feed: LiveFeedResponse,
    game_id: int,
    home_team_id: int | None = None,
    away_team_id: int | None = None,
) -> list[dict]:
    """Extract all pitch events from a live feed response."""
    rows = []
    if feed.live_data and feed.live_data.plays:
        for play in feed.live_data.plays.all_plays:
            for pitch in play.pitches:
                row = flatten_pitch(game_id, play, pitch)
                row["home_team_id"] = home_team_id
                row["away_team_id"] = away_team_id
                rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Game info flattening
# ---------------------------------------------------------------------------

def flatten_game_info(feed: LiveFeedResponse) -> dict:
    """Extract a single dim_game row from the live feed."""
    gd = feed.game_data
    teams = gd.teams if gd else None
    venue = gd.venue if gd else None
    weather = gd.weather if gd else None
    dt = gd.datetime if gd else None
    game = gd.game if gd else None
    info = gd.game_info if gd else None
    linescore = feed.live_data.linescore if feed.live_data else None

    return {
        "game_pk": int(feed.game_pk),
        "home_team_id": teams.home.id if teams and teams.home else None,
        "away_team_id": teams.away.id if teams and teams.away else None,
        "home_team_name": teams.home.name if teams and teams.home else None,
        "away_team_name": teams.away.name if teams and teams.away else None,
        "venue_id": venue.id if venue else None,
        "venue_name": venue.name if venue else None,
        "game_date": dt.official_date if dt else None,
        "game_datetime": dt.date_time.isoformat() if dt and dt.date_time else None,
        "day_night": str(dt.day_night) if dt and dt.day_night else None,
        "game_type": str(game.type) if game and game.type else None,
        "season": game.season if game else None,
        "scheduled_innings": linescore.scheduled_innings if linescore else None,
        "weather_condition": str(weather.condition) if weather and weather.condition else None,
        "weather_temp": weather.temp if weather else None,
        "weather_wind": weather.wind if weather else None,
        "attendance": info.attendance if info else None,
        "game_duration_minutes": info.game_duration_minutes if info else None,
        "detailed_state": gd.status.detailed_state if gd and gd.status else None,
    }


# ---------------------------------------------------------------------------
# Linescore flattening
# ---------------------------------------------------------------------------

def flatten_linescore_innings(feed: LiveFeedResponse, game_pk: int) -> list[dict]:
    """Extract fact_linescore_innings rows — two rows per completed inning."""
    linescore = feed.live_data.linescore if feed.live_data else None
    if not linescore or not linescore.innings:
        return []

    gd = feed.game_data
    home_team_id = gd.teams.home.id if gd and gd.teams and gd.teams.home else None
    away_team_id = gd.teams.away.id if gd and gd.teams and gd.teams.away else None

    rows = []
    for inning in linescore.innings:
        for side, team_line, team_id in [
            ("away", inning.away, away_team_id),
            ("home", inning.home, home_team_id),
        ]:
            extra = team_line.model_extra or {} if team_line else {}
            rows.append({
                "game_pk": game_pk,
                "inning_num": inning.num,
                "team_side": side,
                "team_id": team_id,
                "runs": team_line.runs if team_line else None,
                "hits": team_line.hits if team_line else None,
                "errors": team_line.errors if team_line else None,
                "left_on_base": team_line.left_on_base if team_line else None,
                "pitches": extra.get("pitches"),
                "balls": extra.get("balls"),
                "strikes": extra.get("strikes"),
                "at_bats": extra.get("atBats"),
                "walks": extra.get("walks"),
                "hit_by_pitch": extra.get("hitByPitch"),
            })
    return rows


# ---------------------------------------------------------------------------
# Boxscore flattening
# ---------------------------------------------------------------------------

def _player_role(person_id: int, team: "BoxscoreTeam") -> str:
    """Determine a player's role from the boxscore roster lists."""
    if person_id in team.batters:
        return "batter"
    if person_id in team.pitchers:
        return "pitcher"
    if person_id in team.bench:
        return "bench"
    if person_id in team.bullpen:
        return "bullpen"
    return "unknown"


def flatten_boxscore_players(feed: LiveFeedResponse, game_pk: int) -> list[dict]:
    """Extract dim_game_players rows — one per player per team."""
    boxscore = feed.live_data.boxscore if feed.live_data else None
    if not boxscore:
        return []

    gd = feed.game_data
    home_team_id = gd.teams.home.id if gd and gd.teams and gd.teams.home else None
    away_team_id = gd.teams.away.id if gd and gd.teams and gd.teams.away else None

    rows = []
    for side, team_box, team_id in [
        ("away", boxscore.teams.away, away_team_id),
        ("home", boxscore.teams.home, home_team_id),
    ]:
        for _key, player in team_box.players.items():
            pid = player.person.id
            pos = player.position
            gs = player.game_status
            rows.append({
                "game_pk": game_pk,
                "team_id": team_id,
                "team_side": side,
                "person_id": pid,
                "person_name": player.person.full_name,
                "jersey_number": player.jersey_number,
                "position_code": pos.code if pos else None,
                "position_name": pos.name if pos else None,
                "position_type": pos.type if pos else None,
                "batting_order": player.batting_order,
                "parent_team_id": player.parent_team_id,
                "role": _player_role(pid, team_box),
                "is_substitute": gs.is_substitute if gs else None,
            })
    return rows


def flatten_player_game_stats(feed: LiveFeedResponse, game_pk: int) -> list[dict]:
    """Extract fact_player_game_stats rows — one per player with stat lines."""
    boxscore = feed.live_data.boxscore if feed.live_data else None
    if not boxscore:
        return []

    gd = feed.game_data
    home_team_id = gd.teams.home.id if gd and gd.teams and gd.teams.home else None
    away_team_id = gd.teams.away.id if gd and gd.teams and gd.teams.away else None

    rows = []
    for side, team_box, team_id in [
        ("away", boxscore.teams.away, away_team_id),
        ("home", boxscore.teams.home, home_team_id),
    ]:
        for _key, player in team_box.players.items():
            stats = player.stats
            if not stats:
                continue
            row: dict = {
                "game_pk": game_pk,
                "team_id": team_id,
                "person_id": player.person.id,
                "person_name": player.person.full_name,
            }
            # Flatten each stat group with a prefix
            for prefix, group in [
                ("bat", stats.batting),
                ("pitch", stats.pitching),
                ("field", stats.fielding),
            ]:
                for k, v in group.items():
                    row[f"{prefix}_{k}"] = v
            rows.append(row)
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


def _write_snapshot(path: Path, rows: list[dict] | dict):
    """Atomically write a full snapshot parquet (not append)."""
    if isinstance(rows, dict):
        rows = [rows]
    if not rows:
        return
    df = pd.DataFrame(rows)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    tmp.rename(path)


def write_game_info(game_pk: int, row: dict):
    _write_snapshot(LIVE_GAMES_DIR / f"game_{game_pk}.parquet", row)


def write_linescore(game_pk: int, rows: list[dict]):
    _write_snapshot(LIVE_LINESCORE_DIR / f"game_{game_pk}.parquet", rows)


def write_boxscore_players(game_pk: int, rows: list[dict]):
    _write_snapshot(LIVE_BOXSCORE_DIR / f"game_{game_pk}_players.parquet", rows)


def write_player_game_stats(game_pk: int, rows: list[dict]):
    _write_snapshot(LIVE_BOXSCORE_DIR / f"game_{game_pk}_stats.parquet", rows)


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
        # Metadata tracking
        self.home_team_id: int | None = None
        self.away_team_id: int | None = None
        self.game_info_written: bool = False
        self.last_inning_state: tuple[int | None, str | None] | None = None
        self.last_roster_fingerprint: str | None = None

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

    # -- Change detection ---------------------------------------------------

    def _cache_team_ids(self, feed: LiveFeedResponse):
        """Cache team IDs from the first poll."""
        if self.home_team_id is not None:
            return
        gd = feed.game_data
        if gd and gd.teams:
            self.home_team_id = gd.teams.home.id if gd.teams.home else None
            self.away_team_id = gd.teams.away.id if gd.teams.away else None

    def _detect_inning_change(self, linescore: Linescore) -> bool:
        """Return True when inning state changes (end of half-inning)."""
        current = (linescore.current_inning, linescore.inning_state)
        if current != self.last_inning_state:
            self.last_inning_state = current
            return True
        return False

    @staticmethod
    def _roster_fingerprint(boxscore: Boxscore) -> str:
        """Hash batting orders + pitcher lists to detect lineup changes."""
        parts = []
        for side in (boxscore.teams.away, boxscore.teams.home):
            parts.append(str(side.batting_order))
            parts.append(str(side.pitchers))
        return hashlib.md5("|".join(parts).encode()).hexdigest()

    def _detect_roster_change(self, boxscore: Boxscore) -> bool:
        """Return True when batting order or pitchers change."""
        fp = self._roster_fingerprint(boxscore)
        if fp != self.last_roster_fingerprint:
            self.last_roster_fingerprint = fp
            return True
        return False

    # -- Polling ------------------------------------------------------------

    async def poll(self, client: AsyncMlbClient) -> int:
        """Fetch game data, write pitches + metadata. Returns count of new pitches."""
        try:
            feed = await client.game(game_pk=self.game_pk)
            self._cache_team_ids(feed)

            # --- Pitch events ---
            new_rows = []
            if feed.live_data and feed.live_data.plays:
                for play in feed.live_data.plays.all_plays:
                    for pitch in play.pitches:
                        key = (play.at_bat_index, pitch.index)
                        if key not in self.known_keys:
                            row = flatten_pitch(self.game_pk, play, pitch)
                            row["home_team_id"] = self.home_team_id
                            row["away_team_id"] = self.away_team_id
                            new_rows.append(row)

            if new_rows:
                write_parquet(self.game_pk, new_rows)
                for row in new_rows:
                    self.known_keys.add((row["play_index"], row["pitch_index"]))
                self.total_new += len(new_rows)
                print(f"  {self.label}: +{len(new_rows)} pitches ({len(self.known_keys)} total)")

            # --- Game info (write once, rewrite at game end) ---
            if not self.game_info_written:
                write_game_info(self.game_pk, flatten_game_info(feed))
                self.game_info_written = True

            # --- Linescore (on inning state change) ---
            if feed.live_data and feed.live_data.linescore:
                if self._detect_inning_change(feed.live_data.linescore):
                    ls_rows = flatten_linescore_innings(feed, self.game_pk)
                    if ls_rows:
                        write_linescore(self.game_pk, ls_rows)
                        print(f"  {self.label}: linescore updated (inning {feed.live_data.linescore.current_inning})")

            # --- Boxscore (on lineup/pitching change) ---
            if feed.live_data and feed.live_data.boxscore:
                if self._detect_roster_change(feed.live_data.boxscore):
                    bp_rows = flatten_boxscore_players(feed, self.game_pk)
                    if bp_rows:
                        write_boxscore_players(self.game_pk, bp_rows)
                    st_rows = flatten_player_game_stats(feed, self.game_pk)
                    if st_rows:
                        write_player_game_stats(self.game_pk, st_rows)
                    print(f"  {self.label}: boxscore updated")

            return len(new_rows)
        except Exception as e:
            self.errors += 1
            print(f"  {self.label}: ERROR polling: {e}")
            return 0

    def write_final(self, feed: LiveFeedResponse):
        """Final flush of all metadata at game end."""
        write_game_info(self.game_pk, flatten_game_info(feed))
        ls_rows = flatten_linescore_innings(feed, self.game_pk)
        if ls_rows:
            write_linescore(self.game_pk, ls_rows)
        bp_rows = flatten_boxscore_players(feed, self.game_pk)
        if bp_rows:
            write_boxscore_players(self.game_pk, bp_rows)
        st_rows = flatten_player_game_stats(feed, self.game_pk)
        if st_rows:
            write_player_game_stats(self.game_pk, st_rows)

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

    # Final poll + flush all metadata with complete game data
    if not shutdown.is_set():
        try:
            feed = await client.game(game_pk=monitor.game_pk)
            monitor.write_final(feed)
        except Exception as e:
            print(f"  {monitor.label}: ERROR on final flush: {e}")
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

def _backfill_game(feed: LiveFeedResponse, game_id: int) -> int:
    """Rewrite all parquet files for a single game. Returns pitch count."""
    gd = feed.game_data
    home_team_id = gd.teams.home.id if gd and gd.teams and gd.teams.home else None
    away_team_id = gd.teams.away.id if gd and gd.teams and gd.teams.away else None

    # Pitches
    rows = extract_pitches(feed, game_id, home_team_id, away_team_id)
    if rows:
        path = parquet_path(game_id)
        tmp = path.with_suffix(".parquet.tmp")
        pd.DataFrame(rows).to_parquet(tmp, index=False)
        tmp.rename(path)

    # Game info
    write_game_info(game_id, flatten_game_info(feed))

    # Linescore
    ls_rows = flatten_linescore_innings(feed, game_id)
    if ls_rows:
        write_linescore(game_id, ls_rows)

    # Boxscore players + stats
    bp_rows = flatten_boxscore_players(feed, game_id)
    if bp_rows:
        write_boxscore_players(game_id, bp_rows)
    st_rows = flatten_player_game_stats(feed, game_id)
    if st_rows:
        write_player_game_stats(game_id, st_rows)

    return len(rows)


async def run_backfill(client: AsyncMlbClient, start_date: str | None = None, end_date: str | None = None):
    """Rewrite parquet files using the current schema.

    If start_date/end_date are provided, fetch games from the schedule for that
    date range. Otherwise, rewrite all existing parquet files.
    """
    if start_date:
        # Date-range backfill via schedule
        from datetime import timedelta
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else current
        game_ids: list[int] = []

        print(f"Fetching schedule from {start_date} to {end.isoformat()}...")
        while current <= end:
            mlb_date = _date_to_mlb_format(current.isoformat())
            schedule = await client.schedule(date=mlb_date)
            for d in schedule.dates:
                for game in d.games:
                    game_ids.append(int(game.game_pk))
            current += timedelta(days=1)

        if not game_ids:
            print("No games found in date range.")
            return

        print(f"Backfilling {len(game_ids)} game(s) from schedule")
        total_pitches = 0
        total_errors = 0
        for game_id in game_ids:
            try:
                feed = await client.game(game_pk=game_id)
                monitor = GameMonitor.from_live_feed(feed)
                pitch_count = _backfill_game(feed, game_id)
                total_pitches += pitch_count
                print(f"  {monitor.label}: rewrote {pitch_count} pitches + metadata")
            except Exception as e:
                total_errors += 1
                print(f"  [{game_id}]: ERROR - {e}")

        print_summary(total_pitches, len(game_ids), total_errors)
    else:
        # Existing-file backfill
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
                pitch_count = _backfill_game(feed, game_id)
                total_pitches += pitch_count
                print(f"  {monitor.label}: rewrote {pitch_count} pitches + metadata")
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
    parser.add_argument("--start-date", "--start_date", type=str, dest="start_date", help="Start date for date-range backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", "--end_date", type=str, dest="end_date", help="End date for date-range backfill (YYYY-MM-DD)")
    return parser.parse_args()


async def main():
    args = parse_args()
    for d in (LIVE_DATA_DIR, LIVE_GAMES_DIR, LIVE_LINESCORE_DIR, LIVE_BOXSCORE_DIR):
        d.mkdir(parents=True, exist_ok=True)

    shutdown = asyncio.Event()

    def handle_signal():
        print("\nShutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    async with AsyncMlbClient() as client:
        if args.backfill or args.start_date:
            await run_backfill(client, start_date=args.start_date, end_date=args.end_date)
        elif args.game:
            await run_single_game(client, args.game, shutdown)
        else:
            target_date = args.date or date.today().isoformat()
            await run_schedule(client, target_date, args.team, shutdown)


if __name__ == "__main__":
    asyncio.run(main())
