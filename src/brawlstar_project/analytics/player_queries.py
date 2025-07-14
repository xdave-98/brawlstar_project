import duckdb

from brawlstar_project.constants.paths import get_data_root


def get_player_matches(path, player_tag: str, n_matches: int = 25):
    query = f"""
        SELECT *
        FROM read_parquet('{path}')
        WHERE player_tag = '{player_tag}'
        ORDER BY battle_time DESC
        LIMIT {n_matches}
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df


def get_player_winrate_last_n(path, player_tag: str, n: int = 25):
    query = f"""
        SELECT
            SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate,
            COUNT(*) AS games_played
        FROM (
            SELECT battle_result
            FROM read_parquet('{path}')
            WHERE player_tag = '{player_tag}'
            ORDER BY battle_time DESC
            LIMIT {n}
        )
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df


def get_player_vs_club_winrate(path, player_tag: str, n: int = 25):
    data_root = get_data_root()
    path = data_root / "fact_matches.parquet"
    # Get player's club
    club_query = f"""
        SELECT club_tag FROM read_parquet('{path}') WHERE player_tag = '{player_tag}' AND club_tag IS NOT NULL LIMIT 1
    """
    con = duckdb.connect()
    try:
        club_tag_result = con.execute(club_query).fetchone()
    finally:
        con.close()
    if not club_tag_result or not club_tag_result[0]:
        return None  # No club
    club_tag = club_tag_result[0]
    # Player winrate
    player_query = f"""
        SELECT SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM (
            SELECT battle_result
            FROM read_parquet('{path}')
            WHERE player_tag = '{player_tag}'
            ORDER BY battle_time DESC
            LIMIT {n}
        )
    """
    con = duckdb.connect()
    try:
        player_winrate_result = con.execute(player_query).fetchone()
    finally:
        con.close()
    if not player_winrate_result or player_winrate_result[0] is None:
        return None
    player_winrate = player_winrate_result[0]
    # Club winrate (all games for this club)
    club_query = f"""
        SELECT SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM read_parquet('{path}')
        WHERE club_tag = '{club_tag}'
    """
    con = duckdb.connect()
    try:
        club_winrate_result = con.execute(club_query).fetchone()
    finally:
        con.close()
    if not club_winrate_result or club_winrate_result[0] is None:
        return None
    club_winrate = club_winrate_result[0]
    return {
        "player_winrate": player_winrate,
        "club_winrate": club_winrate,
        "club_tag": club_tag,
    }


def get_player_winrate_by_map(path, player_tag: str):
    query = f"""
        SELECT map_name,
               COUNT(*) AS games_played,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM read_parquet('{path}')
        WHERE player_tag = '{player_tag}'
        GROUP BY map_name
        ORDER BY games_played DESC
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df


def get_player_winrate_by_mode(path, player_tag: str, n: int = 25):
    query = f"""
        SELECT battle_mode,
               COUNT(*) AS games_played,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS winrate
        FROM (
            SELECT battle_mode, battle_result
            FROM read_parquet('{path}')
            WHERE player_tag = '{player_tag}'
            ORDER BY battle_time DESC
            LIMIT {n}
        )
        GROUP BY battle_mode
        ORDER BY games_played DESC
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()
    return df
