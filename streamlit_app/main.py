import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

os.environ["STREAMLIT_CLOUD"] = "1"

# 🧹 Robustly add src/ to PYTHONPATH to import the module
project_root = Path(__file__).resolve().parent.parent
src_path = project_root / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))
else:
    # Try fallback: look for src/ in CWD (for Streamlit Cloud)
    fallback_src = Path(os.getcwd()) / "src"
    if fallback_src.exists():
        sys.path.insert(0, str(fallback_src))
    else:
        st.error(f"❌ Could not find src/ directory at {src_path} or {fallback_src}")
        st.stop()

from brawlstar_project.analytics import duckdb_queries as dq  # noqa: E402
from brawlstar_project.constants.paths import get_data_root  # noqa: E402

st.title("BrawlStars Dashboard")


# Find the latest date partition in cleaned data
def get_latest_partition(base_dir, dim_folder):
    dim_path = base_dir / dim_folder
    if not dim_path.exists():
        return None
    dates = [d.name for d in dim_path.iterdir() if d.is_dir()]
    if not dates:
        return None
    return max(dates)


# Load dimension tables
def load_dim_players():
    data_root = get_data_root()
    path = data_root / "dim_players.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_dim_clubs():
    data_root = get_data_root()
    path = data_root / "dim_clubs.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


# User chooses between Player or Club
mode = st.radio("Select analysis type:", ["Player", "Club"], index=0)

if mode == "Player":
    dim_players = load_dim_players()
    if dim_players.empty:
        st.warning("No players available in the dimension table.")
        st.stop()
    player_options = [
        f"{row['name']} ({row['tag']})" for _, row in dim_players.iterrows()
    ]
    player_selection = st.selectbox("Select a player", player_options)
    # Extract tag from selection
    player_tag = player_selection.split("(")[-1].replace(")", "").strip()
    n_matches = st.slider("Number of matches", 5, 50, 25)
    df = dq.get_player_matches(player_tag, n_matches)
    if df.empty:
        st.warning("No data found.")
    else:
        st.write(df)

elif mode == "Club":
    dim_clubs = load_dim_clubs()
    if dim_clubs.empty:
        st.warning("No clubs available in the dimension table.")
        st.stop()
    club_options = [f"{row['name']} ({row['tag']})" for _, row in dim_clubs.iterrows()]
    club_selection = st.selectbox("Select a club", club_options)
    club_tag = club_selection.split("(")[-1].replace(")", "").strip()
    # Example: show winrate (you can expand this)
    df = dq.get_club_winrate(club_tag)
    if df.empty:
        st.warning("No data found for this club.")
    else:
        st.write(df)
