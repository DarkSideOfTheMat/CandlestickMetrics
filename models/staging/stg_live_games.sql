SELECT 
  game_id,
  ARBITRARY()
  MAX_BY()
  MIN(timestamp) AS first_pitch_time,
  MAX(timestamp) AS last_pitch_time,

FROM read_parquet('data/live/pitches/*.parquet')
GROUP BY 1
