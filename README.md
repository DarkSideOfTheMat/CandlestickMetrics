# CandlestickMetrics

A python package to setup and run baseball analysis.

## Backfill Historical Data

Backfill to local DuckDB (more database setups TBD)

TODO: create script to backfill historical database

## Stream Live Games

Run `source venv/bin/activatte && python3 scripts/live_listener.py`

Live listener has option flags
`--date` date to monitor
`--game` game to monitor using MLB Stats-API gameid
`--backfill` force rewrite parquet files, used when changing the schema
