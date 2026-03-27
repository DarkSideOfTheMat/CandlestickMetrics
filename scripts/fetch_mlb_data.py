"""
Fetch MLB season data from Baseball Savant and Fangraphs
and load it into DuckDB for dbt processing.
"""

import asyncio
import duckdb
import pybaseball
import pybaseball.cache
from concurrent.futures import ThreadPoolExecutor

from mlb_config import (
    TEAM_MAPPING,
    TeamConfig,
    DB_PATH,
    SEASON,
    MAX_CONCURRENT,
    load_to_duckdb,
    safe_run,
)


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


async def fetch_team_data(team: TeamConfig, executor, semaphore):
    async with semaphore:
        print(f"[{team.key}] Starting {team.fullname}...")
        schedule_df, statcast_df = await asyncio.gather(
            safe_run(
                executor,
                fetch_schedule_and_results,
                team,
                label=f"[{team.key}] schedule",
            ),
            safe_run(
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
