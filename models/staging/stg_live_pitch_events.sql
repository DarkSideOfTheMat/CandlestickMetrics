SELECT 
  game_id AS game_pk,
  -- event
  play_index,
  pitch_index,
  timezone('America/New_York',timestamp::TIMESTAMP) AS event_time,
  pitch_number,
  inning,
  half_inning,
  -- game
  away_team_id,
  home_team_id,
  if(half_inning='top', away_team_id, home_team_id) AS batter_team_id,
  if(half_inning='top', home_team_id, away_team_id) AS pitcher_team_id,
  -- batter
  batterid,
  batter_name,
  -- pitcher
  pitcher_id,
  pitcher_name,
  bat_side,
  pitch_hand,
  -- pitch
  pitch_type,
  pitch_description,
  call_code,
  call_description,
  start_speed,
  end_speed,
  extension,
  spin_rate,
  spin_direction,
  -- Pitch Location
  zone,
  plate_x,
  plate_z,
  pfx_x,
  pfx_z,
  -- Break
  break_angle,
  break_length,
  break_vertical,
  break_veritcal_induced,
  break_horizontal,
  -- Situational Context
  balls,
  strikes,
  outs,
  -- Hit Context
  is_in_play,
  is_strike,
  is_ball,
  launch_speed,
  launch_angle,
  hit_distance,
  hit_trajectory,
  at_bat_event,
  at_bat_event_type,
  at_bat_description,
  -- Play Review (+ ABS)
  has_review,
  review_overturned,
  review_in_progress,
  challenge_team_id,
  challenger_id,
  challenger_name
FROM read_parquet('data/live/pitches/*.parquet', union_by_name=True)
