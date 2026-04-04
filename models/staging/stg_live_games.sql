SELECT
  game_pk,
  game_date,
  game_datetime,
  home_teamid,
  home_team_name,
  away_teamid,
  away_team_name,
  venue_id,
  venue_name,
  day_night,
  game_type,
  season,
  scheduled_innings,
  weather_condition,
  weather_temp,
  weather_wind,
  attendance,
  game_duration_minutes,
  detailed_state
FROM READ_PARQUET('data/live/games/game_*.parquet')

-- TODO consume historical data from DuckDB
