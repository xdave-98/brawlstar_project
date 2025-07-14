import duckdb


def get_most_popular_map(path):
    query = f"""
        SELECT map_name, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        GROUP BY map_name
        ORDER BY games_played DESC
        LIMIT 1
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df


def get_game_mode_distribution(path):
    query = f"""
        SELECT battle_mode, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        GROUP BY battle_mode
        ORDER BY games_played DESC
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df


def get_winrate_by_game_mode(path):
    query = f"""
        SELECT battle_mode,
               COUNT(*) AS games_played,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM read_parquet('{path}')
        GROUP BY battle_mode
        ORDER BY games_played DESC
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df
