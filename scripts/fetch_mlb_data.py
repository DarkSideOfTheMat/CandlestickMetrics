"""
Fetch MLB season data from Baseball Savant and Fangraphs
and load it into DuckDB for dbt processing.
"""

import asyncio
import duckdb
import pybaseball
import pybaseball.cache
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from dataclasses import dataclass

DB_PATH = Path(__file__).parent.parent / "data" / "candlestick_metrics.duckdb"

SEASON = 2025
MAX_CONCURRENT = 5  # limit concurrent API requests to be polite

# Mapping between different abbreviation systems used by pybaseball sources.
# baseball-reference (schedule_and_record) and fangraphs use 3-letter codes,
# while statcast (Baseball Savant) uses 2-3 letter codes.
TEAM_MAPPING = {
    "ARI": {
        "fangraphs": "ARI",
        "statcast": "ARI",
        "bbref": "ARI",
        "name": "Diamondbacks",
        "fullname": "Arizona Diamondbacks",
    },
    "ATL": {
        "fangraphs": "ATL",
        "statcast": "ATL",
        "bbref": "ATL",
        "name": "Braves",
        "fullname": "Atlanta Braves",
    },
    "BAL": {
        "fangraphs": "BAL",
        "statcast": "BAL",
        "bbref": "BAL",
        "name": "Orioles",
        "fullname": "Baltimore Orioles",
    },
    "BOS": {
        "fangraphs": "BOS",
        "statcast": "BOS",
        "bbref": "BOS",
        "name": "Red Sox",
        "fullname": "Boston Red Sox",
    },
    "CHC": {
        "fangraphs": "CHC",
        "statcast": "CHC",
        "bbref": "CHC",
        "name": "Cubs",
        "fullname": "Chicago Cubs",
    },
    "CHW": {
        "fangraphs": "CHW",
        "statcast": "CWS",
        "bbref": "CHW",
        "name": "White Sox",
        "fullname": "Chicago White Sox",
    },
    "CIN": {
        "fangraphs": "CIN",
        "statcast": "CIN",
        "bbref": "CIN",
        "name": "Reds",
        "fullname": "Cincinnati Reds",
    },
    "CLE": {
        "fangraphs": "CLE",
        "statcast": "CLE",
        "bbref": "CLE",
        "name": "Guardians",
        "fullname": "Cleveland Guardians",
    },
    "COL": {
        "fangraphs": "COL",
        "statcast": "COL",
        "bbref": "COL",
        "name": "Rockies",
        "fullname": "Colorado Rockies",
    },
    "DET": {
        "fangraphs": "DET",
        "statcast": "DET",
        "bbref": "DET",
        "name": "Tigers",
        "fullname": "Detroit Tigers",
    },
    "HOU": {
        "fangraphs": "HOU",
        "statcast": "HOU",
        "bbref": "HOU",
        "name": "Astros",
        "fullname": "Houston Astros",
    },
    "KCR": {
        "fangraphs": "KCR",
        "statcast": "KC",
        "bbref": "KCR",
        "name": "Royals",
        "fullname": "Kansas City Royals",
    },
    "LAA": {
        "fangraphs": "LAA",
        "statcast": "LAA",
        "bbref": "LAA",
        "name": "Angels",
        "fullname": "Los Angeles Angels",
    },
    "LAD": {
        "fangraphs": "LAD",
        "statcast": "LAD",
        "bbref": "LAD",
        "name": "Dodgers",
        "fullname": "Los Angeles Dodgers",
    },
    "MIA": {
        "fangraphs": "MIA",
        "statcast": "MIA",
        "bbref": "MIA",
        "name": "Marlins",
        "fullname": "Miami Marlins",
    },
    "MIL": {
        "fangraphs": "MIL",
        "statcast": "MIL",
        "bbref": "MIL",
        "name": "Brewers",
        "fullname": "Milwaukee Brewers",
    },
    "MIN": {
        "fangraphs": "MIN",
        "statcast": "MIN",
        "bbref": "MIN",
        "name": "Twins",
        "fullname": "Minnesota Twins",
    },
    "NYM": {
        "fangraphs": "NYM",
        "statcast": "NYM",
        "bbref": "NYM",
        "name": "Mets",
        "fullname": "New York Mets",
    },
    "NYY": {
        "fangraphs": "NYY",
        "statcast": "NYY",
        "bbref": "NYY",
        "name": "Yankees",
        "fullname": "New York Yankees",
    },
    "ATH": {
        "fangraphs": "ATH",
        "statcast": "OAK",
        "bbref": "OAK",
        "name": "Athletics",
        "fullname": "Oakland Athletics",
    },
    "PHI": {
        "fangraphs": "PHI",
        "statcast": "PHI",
        "bbref": "PHI",
        "name": "Phillies",
        "fullname": "Philadelphia Phillies",
    },
    "PIT": {
        "fangraphs": "PIT",
        "statcast": "PIT",
        "bbref": "PIT",
        "name": "Pirates",
        "fullname": "Pittsburgh Pirates",
    },
    "SDP": {
        "fangraphs": "SDP",
        "statcast": "SD",
        "bbref": "SDP",
        "name": "Padres",
        "fullname": "San Diego Padres",
    },
    "SEA": {
        "fangraphs": "SEA",
        "statcast": "SEA",
        "bbref": "SEA",
        "name": "Mariners",
        "fullname": "Seattle Mariners",
    },
    "SFG": {
        "fangraphs": "SFG",
        "statcast": "SF",
        "bbref": "SFG",
        "name": "Giants",
        "fullname": "San Francisco Giants",
    },
    "STL": {
        "fangraphs": "STL",
        "statcast": "STL",
        "bbref": "STL",
        "name": "Cardinals",
        "fullname": "St. Louis Cardinals",
    },
    "TBR": {
        "fangraphs": "TBR",
        "statcast": "TB",
        "bbref": "TBR",
        "name": "Rays",
        "fullname": "Tampa Bay Rays",
    },
    "TEX": {
        "fangraphs": "TEX",
        "statcast": "TEX",
        "bbref": "TEX",
        "name": "Rangers",
        "fullname": "Texas Rangers",
    },
    "TOR": {
        "fangraphs": "TOR",
        "statcast": "TOR",
        "bbref": "TOR",
        "name": "Blue Jays",
        "fullname": "Toronto Blue Jays",
    },
    "WSN": {
        "fangraphs": "WSN",
        "statcast": "WSH",
        "bbref": "WSN",
        "name": "Nationals",
        "fullname": "Washington Nationals",
    },
}


@dataclass
class TeamConfig:
    key: str  # canonical key into TEAM_MAPPING
    season: int

    @property
    def fangraphs(self) -> str:
        return TEAM_MAPPING[self.key]["fangraphs"]

    @property
    def statcast(self) -> str:
        return TEAM_MAPPING[self.key]["statcast"]

    @property
    def bbref(self) -> str:
        return TEAM_MAPPING[self.key]["bbref"]

    @property
    def name(self) -> str:
        return TEAM_MAPPING[self.key]["name"]

    @property
    def fullname(self) -> str:
        return TEAM_MAPPING[self.key]["fullname"]


def fetch_schedule_and_results(team: TeamConfig):
    print(f"  [{team.key}] Fetching schedule and results...")
    df = pybaseball.schedule_and_record(season=team.season, team=team.bbref)
    # pybaseball has a bug where Attendance='Unknown' fails numeric conversion
    if "Attendance" in df.columns:
        df["Attendance"] = df["Attendance"].replace("Unknown", None)
    df["season"] = team.season
    df["team"] = team.key
    print(f"  [{team.key}] -> {len(df)} games")
    return df


def fetch_all_batting_stats(season: int):
    print(f"Fetching {season} league-wide batting stats...")
    df = pybaseball.batting_stats(season, qual=0)
    print(f"  -> {len(df)} total batters")
    return df


def fetch_all_pitching_stats(season: int):
    print(f"Fetching {season} league-wide pitching stats...")
    df = pybaseball.pitching_stats(season, qual=0)
    print(f"  -> {len(df)} total pitchers")
    return df


def fetch_statcast_data(team: TeamConfig):
    print(f"  [{team.key}] Fetching statcast data...")
    df = pybaseball.statcast(
        start_dt=f"{team.season}-03-27",
        end_dt=f"{team.season}-09-28",
        team=team.statcast,
        verbose=False,
    )
    print(f"  [{team.key}] -> {len(df)} pitches")
    return df


def load_to_duckdb(con, df, table_name, append=False):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.register("df_temp", df)
    if append:
        con.execute(f"INSERT INTO raw.{table_name} SELECT * FROM df_temp")
    else:
        con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
        con.execute(f"CREATE TABLE raw.{table_name} AS SELECT * FROM df_temp")
    con.unregister("df_temp")
    count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
    print(f"  Loaded {len(df)} rows into raw.{table_name} ({count:,} total)")


async def _safe_run(executor, fn, *args, label=""):
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(executor, fn, *args)
    except Exception as e:
        print(f"  {label} ERROR: {e}")
        return None


async def fetch_team_data(team: TeamConfig, executor, semaphore):
    async with semaphore:
        print(f"[{team.key}] Starting {team.fullname}...")
        schedule_df, statcast_df = await asyncio.gather(
            _safe_run(
                executor,
                fetch_schedule_and_results,
                team,
                label=f"[{team.key}] schedule",
            ),
            _safe_run(
                executor, fetch_statcast_data, team, label=f"[{team.key}] statcast"
            ),
        )
        print(f"[{team.key}] Done.")
        return team, schedule_df, statcast_df


async def main():
    teams = [TeamConfig(key=key, season=SEASON) for key in TEAM_MAPPING]

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    loop = asyncio.get_event_loop()

    try:
        # Batting and pitching stats come league-wide from Fangraphs,
        # so we fetch once and load the full dataset.
        batting_df, pitching_df = await asyncio.gather(
            loop.run_in_executor(executor, fetch_all_batting_stats, SEASON),
            loop.run_in_executor(executor, fetch_all_pitching_stats, SEASON),
        )
        load_to_duckdb(con, batting_df, "batting_stats")
        load_to_duckdb(con, pitching_df, "pitching_stats")

        # Schedule and statcast are fetched per-team concurrently.
        print(
            f"\nFetching schedule + statcast for {len(teams)} teams "
            f"({MAX_CONCURRENT} concurrent)...\n"
        )

        tasks = [fetch_team_data(team, executor, semaphore) for team in teams]
        results = await asyncio.gather(*tasks)

        # Load results into DuckDB sequentially (DuckDB is single-writer).
        failed_teams = []
        schedule_loaded = False
        statcast_loaded = False
        for team, schedule_df, statcast_df in results:
            if schedule_df is None and statcast_df is None:
                failed_teams.append(team.key)
                continue
            if schedule_df is not None:
                load_to_duckdb(
                    con, schedule_df, "schedule_and_results", append=schedule_loaded
                )
                schedule_loaded = True
            if statcast_df is not None:
                load_to_duckdb(
                    con, statcast_df, "statcast_pitches", append=statcast_loaded
                )
                statcast_loaded = True

        if failed_teams:
            print(f"\nWARNING: Failed to fetch data for: {', '.join(failed_teams)}")
        print("\nAll data loaded successfully!")
        print("\nTables in raw schema:")
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
        for t in tables:
            count = con.execute(f"SELECT COUNT(*) FROM raw.{t[0]}").fetchone()[0]
            print(f"  - raw.{t[0]}: {count:,} rows")
    finally:
        executor.shutdown(wait=False)
        con.close()


if __name__ == "__main__":
    pybaseball.cache.enable()
    asyncio.run(main())
