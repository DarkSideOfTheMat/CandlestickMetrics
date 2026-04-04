"""
Shared configuration for MLB data scripts.
Team mappings, constants, and common utilities.
"""

import asyncio
import duckdb
from pathlib import Path
from dataclasses import dataclass

DB_PATH = Path(__file__).parent.parent / "data" / "candlestick_metrics.duckdb"
LIVE_DATA_DIR = Path(__file__).parent.parent / "data" / "live" / "pitches"
LIVE_GAMES_DIR = Path(__file__).parent.parent / "data" / "live" / "games"
LIVE_LINESCORE_DIR = Path(__file__).parent.parent / "data" / "live" / "linescores"
LIVE_BOXSCORE_DIR = Path(__file__).parent.parent / "data" / "live" / "boxscores"

SEASON = 2025
MAX_CONCURRENT = 5

# Mapping between different abbreviation systems used by data sources.
# baseball-reference and fangraphs use 3-letter codes,
# statcast (Baseball Savant) uses 2-3 letter codes,
# MLB StatsAPI uses numeric team IDs.
TEAM_MAPPING = {
    "ARI": {
        "fangraphs": "ARI",
        "statcast": "ARI",
        "bbref": "ARI",
        "statsapi_id": 109,
        "name": "Diamondbacks",
        "fullname": "Arizona Diamondbacks",
    },
    "ATL": {
        "fangraphs": "ATL",
        "statcast": "ATL",
        "bbref": "ATL",
        "statsapi_id": 144,
        "name": "Braves",
        "fullname": "Atlanta Braves",
    },
    "BAL": {
        "fangraphs": "BAL",
        "statcast": "BAL",
        "bbref": "BAL",
        "statsapi_id": 110,
        "name": "Orioles",
        "fullname": "Baltimore Orioles",
    },
    "BOS": {
        "fangraphs": "BOS",
        "statcast": "BOS",
        "bbref": "BOS",
        "statsapi_id": 111,
        "name": "Red Sox",
        "fullname": "Boston Red Sox",
    },
    "CHC": {
        "fangraphs": "CHC",
        "statcast": "CHC",
        "bbref": "CHC",
        "statsapi_id": 112,
        "name": "Cubs",
        "fullname": "Chicago Cubs",
    },
    "CHW": {
        "fangraphs": "CHW",
        "statcast": "CWS",
        "bbref": "CHW",
        "statsapi_id": 145,
        "name": "White Sox",
        "fullname": "Chicago White Sox",
    },
    "CIN": {
        "fangraphs": "CIN",
        "statcast": "CIN",
        "bbref": "CIN",
        "statsapi_id": 113,
        "name": "Reds",
        "fullname": "Cincinnati Reds",
    },
    "CLE": {
        "fangraphs": "CLE",
        "statcast": "CLE",
        "bbref": "CLE",
        "statsapi_id": 114,
        "name": "Guardians",
        "fullname": "Cleveland Guardians",
    },
    "COL": {
        "fangraphs": "COL",
        "statcast": "COL",
        "bbref": "COL",
        "statsapi_id": 115,
        "name": "Rockies",
        "fullname": "Colorado Rockies",
    },
    "DET": {
        "fangraphs": "DET",
        "statcast": "DET",
        "bbref": "DET",
        "statsapi_id": 116,
        "name": "Tigers",
        "fullname": "Detroit Tigers",
    },
    "HOU": {
        "fangraphs": "HOU",
        "statcast": "HOU",
        "bbref": "HOU",
        "statsapi_id": 117,
        "name": "Astros",
        "fullname": "Houston Astros",
    },
    "KCR": {
        "fangraphs": "KCR",
        "statcast": "KC",
        "bbref": "KCR",
        "statsapi_id": 118,
        "name": "Royals",
        "fullname": "Kansas City Royals",
    },
    "LAA": {
        "fangraphs": "LAA",
        "statcast": "LAA",
        "bbref": "LAA",
        "statsapi_id": 108,
        "name": "Angels",
        "fullname": "Los Angeles Angels",
    },
    "LAD": {
        "fangraphs": "LAD",
        "statcast": "LAD",
        "bbref": "LAD",
        "statsapi_id": 119,
        "name": "Dodgers",
        "fullname": "Los Angeles Dodgers",
    },
    "MIA": {
        "fangraphs": "MIA",
        "statcast": "MIA",
        "bbref": "MIA",
        "statsapi_id": 146,
        "name": "Marlins",
        "fullname": "Miami Marlins",
    },
    "MIL": {
        "fangraphs": "MIL",
        "statcast": "MIL",
        "bbref": "MIL",
        "statsapi_id": 158,
        "name": "Brewers",
        "fullname": "Milwaukee Brewers",
    },
    "MIN": {
        "fangraphs": "MIN",
        "statcast": "MIN",
        "bbref": "MIN",
        "statsapi_id": 142,
        "name": "Twins",
        "fullname": "Minnesota Twins",
    },
    "NYM": {
        "fangraphs": "NYM",
        "statcast": "NYM",
        "bbref": "NYM",
        "statsapi_id": 121,
        "name": "Mets",
        "fullname": "New York Mets",
    },
    "NYY": {
        "fangraphs": "NYY",
        "statcast": "NYY",
        "bbref": "NYY",
        "statsapi_id": 147,
        "name": "Yankees",
        "fullname": "New York Yankees",
    },
    "ATH": {
        "fangraphs": "ATH",
        "statcast": "OAK",
        "bbref": "OAK",
        "statsapi_id": 133,
        "name": "Athletics",
        "fullname": "Oakland Athletics",
    },
    "PHI": {
        "fangraphs": "PHI",
        "statcast": "PHI",
        "bbref": "PHI",
        "statsapi_id": 143,
        "name": "Phillies",
        "fullname": "Philadelphia Phillies",
    },
    "PIT": {
        "fangraphs": "PIT",
        "statcast": "PIT",
        "bbref": "PIT",
        "statsapi_id": 134,
        "name": "Pirates",
        "fullname": "Pittsburgh Pirates",
    },
    "SDP": {
        "fangraphs": "SDP",
        "statcast": "SD",
        "bbref": "SDP",
        "statsapi_id": 135,
        "name": "Padres",
        "fullname": "San Diego Padres",
    },
    "SEA": {
        "fangraphs": "SEA",
        "statcast": "SEA",
        "bbref": "SEA",
        "statsapi_id": 136,
        "name": "Mariners",
        "fullname": "Seattle Mariners",
    },
    "SFG": {
        "fangraphs": "SFG",
        "statcast": "SF",
        "bbref": "SFG",
        "statsapi_id": 137,
        "name": "Giants",
        "fullname": "San Francisco Giants",
    },
    "STL": {
        "fangraphs": "STL",
        "statcast": "STL",
        "bbref": "STL",
        "statsapi_id": 138,
        "name": "Cardinals",
        "fullname": "St. Louis Cardinals",
    },
    "TBR": {
        "fangraphs": "TBR",
        "statcast": "TB",
        "bbref": "TBR",
        "statsapi_id": 139,
        "name": "Rays",
        "fullname": "Tampa Bay Rays",
    },
    "TEX": {
        "fangraphs": "TEX",
        "statcast": "TEX",
        "bbref": "TEX",
        "statsapi_id": 140,
        "name": "Rangers",
        "fullname": "Texas Rangers",
    },
    "TOR": {
        "fangraphs": "TOR",
        "statcast": "TOR",
        "bbref": "TOR",
        "statsapi_id": 141,
        "name": "Blue Jays",
        "fullname": "Toronto Blue Jays",
    },
    "WSN": {
        "fangraphs": "WSN",
        "statcast": "WSH",
        "bbref": "WSN",
        "statsapi_id": 120,
        "name": "Nationals",
        "fullname": "Washington Nationals",
    },
}

# Reverse lookup: statsapi numeric ID -> canonical team key
STATSAPI_ID_TO_KEY = {
    info["statsapi_id"]: key for key, info in TEAM_MAPPING.items()
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
    def statsapi_id(self) -> int:
        return TEAM_MAPPING[self.key]["statsapi_id"]

    @property
    def name(self) -> str:
        return TEAM_MAPPING[self.key]["name"]

    @property
    def fullname(self) -> str:
        return TEAM_MAPPING[self.key]["fullname"]


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


async def safe_run(executor, fn, *args, label=""):
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(executor, fn, *args)
    except Exception as e:
        print(f"  {label} ERROR: {e}")
        return None
