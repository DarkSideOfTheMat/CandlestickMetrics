import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "data/candlestick_metrics.duckdb"
    candlestick_metrics = duckdb.connect(DATABASE_URL, read_only=True)
    return (candlestick_metrics,)


@app.cell
def _(candlestick_metrics, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM READ_PARQUET('data/live/pitches/*.parquet', union_by_name=True)
        """,
        engine=candlestick_metrics
    )
    return


@app.cell(hide_code=True)
def _(candlestick_metrics, mo):
    _df = mo.sql(
        f"""
        SELECT distinct call_code, call_description FROM main.stg_live_pitch_events
        """,
        engine=candlestick_metrics
    )
    return


@app.cell(hide_code=True)
def _(candlestick_metrics, mo):
    _df = mo.sql(
        f"""
        -- At Bats
        SELECT
        	game_id,
        	inning,
        	half_inning,
        	play_index,
            ARBITRARY(batter_id) AS batter_id,
        	ARBITRARY(batter_name) AS batter_name,
            ARBITRARY(bat_side) AS bat_side,
            ARBITRARY(pitcher_id) AS pitcher_id,
            ARBITRARY(pitcher_name) AS pitcher_name,
            ARBITRARY(pitch_hand) AS pitch_hand,
            COUNT(1) AS pitches,
            COUNT_IF(is_ball)AS balls,
            COUNT_IF(is_strike) AS strikes,
            COUNT_IF(call_code = 'F') AS fouls,
            ARBITRARY(at_bat_event) AS at_bat_event
        FROM main.stg_live_pitch_events
        GROUP BY 1,2,3,4
        """,
        engine=candlestick_metrics
    )
    return


@app.cell(hide_code=True)
def _(candlestick_metrics, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM raw.statcast_pitches
        """,
        engine=candlestick_metrics
    )
    return


@app.cell
def _(candlestick_metrics, mo):
    _df = mo.sql(
        f"""

        """,
        engine=candlestick_metrics
    )
    return


@app.cell(hide_code=True)
def _(candlestick_metrics, mo):
    spray = mo.sql(
        f"""
        -- Simple pitcher spray chart
        SELECT
            pitch_type,
            plate_x,
            plate_z
        FROM main.stg_live_pitch_events
        """,
        engine=candlestick_metrics
    )
    return (spray,)


@app.cell
def _(spray):
    spray.plot.scatter(x="plate_x", y="plate_z")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
