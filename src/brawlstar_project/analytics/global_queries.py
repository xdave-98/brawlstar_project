from brawlstar_project.analytics.duckdb_utils import duckdb_simple_query


@duckdb_simple_query()
def get_most_popular_map(path):
    return f"""
        SELECT map_name, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        GROUP BY map_name
        ORDER BY games_played DESC
        LIMIT 1
    """


@duckdb_simple_query()
def get_game_mode_distribution(path):
    return f"""
        SELECT battle_mode, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        GROUP BY battle_mode
        ORDER BY games_played DESC
    """


@duckdb_simple_query()
def get_winrate_by_game_mode(path):
    return f"""
        SELECT battle_mode,
               COUNT(*) AS games_played,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM read_parquet('{path}')
        GROUP BY battle_mode
        ORDER BY games_played DESC
    """
