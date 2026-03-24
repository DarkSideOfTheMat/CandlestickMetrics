"""
Fetch SF Giants 2025 season data from Baseball Savant and Fangraphs
and load it into DuckDB for dbt processing.
"""

import duckdb
import pybaseball
from pathlib import Path
from dataclasses import dataclass

DB_PATH = Path(__file__).parent.parent / "data" / "candlestick_metrics.duckdb"

SEASON = 2025

# Mapping between different abbreviation systems used by pybaseball sources.
# baseball-reference (schedule_and_record) and fangraphs use 3-letter codes,
# while statcast (Baseball Savant) uses 2-3 letter codes.
TEAM_MAPPING = {
    "ARI": {"fangraphs": "ARI", "statcast": "ARI", "bbref": "ARI", "name": "Diamondbacks", "fullname": "Arizona Diamondbacks"},
    "ATL": {"fangraphs": "ATL", "statcast": "ATL", "bbref": "ATL", "name": "Braves",       "fullname": "Atlanta Braves"},
    "BAL": {"fangraphs": "BAL", "statcast": "BAL", "bbref": "BAL", "name": "Orioles",      "fullname": "Baltimore Orioles"},
    "BOS": {"fangraphs": "BOS", "statcast": "BOS", "bbref": "BOS", "name": "Red Sox",      "fullname": "Boston Red Sox"},
    "CHC": {"fangraphs": "CHC", "statcast": "CHC", "bbref": "CHC", "name": "Cubs",         "fullname": "Chicago Cubs"},
    "CHW": {"fangraphs": "CHW", "statcast": "CWS", "bbref": "CHW", "name": "White Sox",    "fullname": "Chicago White Sox"},
    "CIN": {"fangraphs": "CIN", "statcast": "CIN", "bbref": "CIN", "name": "Reds",         "fullname": "Cincinnati Reds"},
    "CLE": {"fangraphs": "CLE", "statcast": "CLE", "bbref": "CLE", "name": "Guardians",    "fullname": "Cleveland Guardians"},
    "COL": {"fangraphs": "COL", "statcast": "COL", "bbref": "COL", "name": "Rockies",      "fullname": "Colorado Rockies"},
    "DET": {"fangraphs": "DET", "statcast": "DET", "bbref": "DET", "name": "Tigers",       "fullname": "Detroit Tigers"},
    "HOU": {"fangraphs": "HOU", "statcast": "HOU", "bbref": "HOU", "name": "Astros",       "fullname": "Houston Astros"},
    "KCR": {"fangraphs": "KCR", "statcast": "KC",  "bbref": "KCR", "name": "Royals",       "fullname": "Kansas City Royals"},
    "LAA": {"fangraphs": "LAA", "statcast": "LAA", "bbref": "LAA", "name": "Angels",       "fullname": "Los Angeles Angels"},
    "LAD": {"fangraphs": "LAD", "statcast": "LAD", "bbref": "LAD", "name": "Dodgers",      "fullname": "Los Angeles Dodgers"},
    "MIA": {"fangraphs": "MIA", "statcast": "MIA", "bbref": "MIA", "name": "Marlins",      "fullname": "Miami Marlins"},
    "MIL": {"fangraphs": "MIL", "statcast": "MIL", "bbref": "MIL", "name": "Brewers",      "fullname": "Milwaukee Brewers"},
    "MIN": {"fangraphs": "MIN", "statcast": "MIN", "bbref": "MIN", "name": "Twins",        "fullname": "Minnesota Twins"},
    "NYM": {"fangraphs": "NYM", "statcast": "NYM", "bbref": "NYM", "name": "Mets",         "fullname": "New York Mets"},
    "NYY": {"fangraphs": "NYY", "statcast": "NYY", "bbref": "NYY", "name": "Yankees",      "fullname": "New York Yankees"},
    "ATH": {"fangraphs": "ATH", "statcast": "OAK", "bbref": "OAK", "name": "Athletics",    "fullname": "Oakland Athletics"},
    "PHI": {"fangraphs": "PHI", "statcast": "PHI", "bbref": "PHI", "name": "Phillies",     "fullname": "Philadelphia Phillies"},
    "PIT": {"fangraphs": "PIT", "statcast": "PIT", "bbref": "PIT", "name": "Pirates",      "fullname": "Pittsburgh Pirates"},
    "SDP": {"fangraphs": "SDP", "statcast": "SD",  "bbref": "SDP", "name": "Padres",       "fullname": "San Diego Padres"},
    "SEA": {"fangraphs": "SEA", "statcast": "SEA", "bbref": "SEA", "name": "Mariners",     "fullname": "Seattle Mariners"},
    "SFG": {"fangraphs": "SFG", "statcast": "SF",  "bbref": "SFG", "name": "Giants",       "fullname": "San Francisco Giants"},
    "STL": {"fangraphs": "STL", "statcast": "STL", "bbref": "STL", "name": "Cardinals",    "fullname": "St. Louis Cardinals"},
    "TBR": {"fangraphs": "TBR", "statcast": "TB",  "bbref": "TBR", "name": "Rays",         "fullname": "Tampa Bay Rays"},
    "TEX": {"fangraphs": "TEX", "statcast": "TEX", "bbref": "TEX", "name": "Rangers",      "fullname": "Texas Rangers"},
    "TOR": {"fangraphs": "TOR", "statcast": "TOR", "bbref": "TOR", "name": "Blue Jays",    "fullname": "Toronto Blue Jays"},
    "WSN": {"fangraphs": "WSN", "statcast": "WSH", "bbref": "WSN", "name": "Nationals",    "fullname": "Washington Nationals"},
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
    print(f"Fetching {team.season} {team.fullname} schedule and results...")
    df = pybaseball.schedule_and_record(season=team.season, team=team.bbref)
    df["season"] = team.season
    df["team"] = team.key
    print(f"  -> {len(df)} games")
    return df


def fetch_batting_stats(team: TeamConfig):
    print(f"Fetching {team.season} {team.name} batting stats...")
    df = pybaseball.batting_stats(team.season, qual=0)
    df = df[df["Team"] == team.fangraphs]
    print(f"  -> {len(df)} {team.name} batters")
    return df


def fetch_pitching_stats(team: TeamConfig):
    print(f"Fetching {team.season} {team.name} pitching stats...")
    df = pybaseball.pitching_stats(team.season, qual=0)
    df = df[df["Team"] == team.fangraphs]
    print(f"  -> {len(df)} {team.name} pitchers")
    return df


def fetch_statcast_data(team: TeamConfig):
    print(f"Fetching {team.season} {team.name} statcast data...")
    print("  (this may take several minutes)")
    df = pybaseball.statcast(
        start_dt=f"{team.season}-03-27",
        end_dt=f"{team.season}-09-28",
        team=team.statcast,
    )
    print(f"  -> {len(df)} pitches")
    return df


def load_to_duckdb(con, df, table_name):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
    con.register("df_temp", df)
    con.execute(f"CREATE TABLE raw.{table_name} AS SELECT * FROM df_temp")
    con.unregister("df_temp")
    count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
    print(f"  Loaded {count} rows into raw.{table_name}")


def main():
    team = TeamConfig(key="SFG", season=SEASON)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    try:
        schedule_df = fetch_schedule_and_results(team)
        load_to_duckdb(con, schedule_df, "schedule_and_results")

        batting_df = fetch_batting_stats(team)
        load_to_duckdb(con, batting_df, "batting_stats")

        pitching_df = fetch_pitching_stats(team)
        load_to_duckdb(con, pitching_df, "pitching_stats")

        statcast_df = fetch_statcast_data(team)
        load_to_duckdb(con, statcast_df, "statcast_pitches")

        print("\nAll data loaded successfully!")
        print("\nTables in raw schema:")
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
        for t in tables:
            print(f"  - raw.{t[0]}")
    finally:
        con.close()


if __name__ == "__main__":
    pybaseball.cache.enable()
    main()
