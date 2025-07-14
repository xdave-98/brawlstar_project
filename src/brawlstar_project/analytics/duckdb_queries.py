
import duckdb
import pandas as pd

from brawlstar_project.constants.paths import DATA_CLEANED_DIR


def get_player_matches(player_tag: str, n_matches: int = 25) -> pd.DataFrame:
    con = duckdb.connect()
    query = f"""
        SELECT *
        FROM read_parquet('{DATA_CLEANED_DIR}/fact_matches/*/*.parquet')
        WHERE player_tag = '{player_tag}'
        ORDER BY battle_time DESC
        LIMIT {n_matches}
    """
    df = con.execute(query).df()
    con.close()
    return df


def get_club_winrate(club_tag: str) -> pd.DataFrame:
    con = duckdb.connect()
    query = f"""
        SELECT club_tag, COUNT(*) AS total_games,
               SUM(CASE WHEN battle_result = 'victory' THEN 1 ELSE 0 END) AS wins
        FROM read_parquet('{DATA_CLEANED_DIR}/fact_matches/*/*.parquet')
        WHERE club_tag = '{club_tag}'
        GROUP BY club_tag
    """
    df = con.execute(query).df()
    con.close()
    return df
