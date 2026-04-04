SELECT
    game_pk,
    inning_num AS inning,
    IF(team_side='away', 'top', 'bottom') AS half_inning,
    team_side,
    team_id,
    runs,
    hits,
    errors,
    left_on_base
FROM READ_PARQUET('data/live/linescores/game_*.parquet', union_by_name=True)
