from brawlstar_project.analytics.duckdb_utils import duckdb_simple_query


@duckdb_simple_query()
def get_club_winrate(path, club_tag: str):
    return f"""
        SELECT club_tag, COUNT(*) AS total_games,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) AS wins
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}'
        GROUP BY club_tag
    """


@duckdb_simple_query()
def get_club_winrate_last_day(path, club_tag: str):
    return f"""
        SELECT
            SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate,
            COUNT(*) AS games_played
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}' AND battle_time::DATE = CURRENT_DATE
    """


@duckdb_simple_query()
def get_club_winloss_by_day(path, club_tag: str):
    return f"""
        SELECT
            battle_time::DATE AS day,
            SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN battle_result = 'defeat' THEN 1 ELSE 0 END) AS losses
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}'
        GROUP BY day
        ORDER BY day
    """


@duckdb_simple_query()
def get_club_comparison_by_winrate(path):
    return f"""
        SELECT club_tag,
               COUNT(*) AS games_played,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM read_parquet('{path}')
        WHERE club_tag IS NOT NULL
        GROUP BY club_tag
        ORDER BY winrate DESC
    """


@duckdb_simple_query()
def get_club_member_participation(path, club_tag: str):
    return f"""
        SELECT player_tag, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}'
        GROUP BY player_tag
        ORDER BY games_played DESC
        LIMIT 10
    """


@duckdb_simple_query()
def get_club_activity_over_time(path, club_tag: str):
    return f"""
        SELECT battle_time::DATE AS day, COUNT(*) AS games_played
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}'
        GROUP BY day
        ORDER BY day
    """


@duckdb_simple_query()
def get_club_winrate_last_n(path, club_tag: str, n: int = 100):
    return f"""
        SELECT
            SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate,
            COUNT(*) AS games_played
        FROM (
            SELECT battle_result
            FROM read_parquet('{path}')
            WHERE club_tag = '{club_tag}'
            ORDER BY battle_time DESC
            LIMIT {n}
        )
    """
