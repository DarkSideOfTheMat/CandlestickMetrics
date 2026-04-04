
/*
SELECT 
  game_date AS ds,
  
  -- game info
  game_pk AS game_statcast_id,
  home_team AS home_team_short,
  away_team AS away_team_short,

  -- at bat info
  at_bat_number,
  pitch_number,
  balls,
  strikes,
  events,
  

  -- PLAYER INFO
  -- pitcher info
  pitcher AS pitcher_statcast_id,
  p_throws AS pitcher_handedness,

  -- batter info
  batter AS batter_statcast_id,
  stand, -- left or right, which way the batter is standing

  -- ball-in-play info


  -- fielders
  fielder_2 AS catcher_statcast_id,
  fielder_3 AS first_base_statcast_id,
  fielder_4 AS second_base_statcast_id,
  fielder_5 AS third_base_statcast_id,
  fielder_6 AS shortstop_statcast_id,
  fielder_7 AS left_field_statcast_id,
  fielder_8 AS center_field_statcast_id,
  fielder_9 AS right_field_statcast_id,
  -- pitch info
  type AS outcome, -- "S" Strike, "B" Ball, "X" Contact (not sure if hit or out)
  balls,
  strikes,
  pfx_x,
  pfx_z,
  plate_x,
  plate_z,
  zone, -- zone crossing plate from catchers perspective

  -- game current info
  inning,
  inning_top_bot,
  outs_when_up, -- outs at the beginning of at bat, doesn't update for pickoffs,
  on_1b,
  on_2b,
  on_3b
FROM raw.statcast_pitches
*/
