import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
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

from brawlstar_project.analytics import club_queries as cq  # noqa: E402
from brawlstar_project.analytics import global_queries as gq  # noqa: E402
from brawlstar_project.analytics import player_queries as pq  # noqa: E402
from brawlstar_project.constants.paths import get_data_root  # noqa: E402

st.title("BrawlStars Dashboard")


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


# User chooses between Player, Club, or Global analysis
mode = st.radio("Select analysis type:", ["Player", "Club", "Global"], index=0)

if mode == "Player":
    dim_players = load_dim_players()
    if dim_players.empty:
        st.warning("No players available in the dimension table.")
        st.stop()
    player_options = [
        f"{row['name']} ({row['tag']})" for _, row in dim_players.iterrows()
    ]
    player_selection = st.selectbox("Select a player", player_options)
    player_tag = player_selection.split("(")[-1].replace(")", "").strip()
    n_matches = st.slider("Number of matches", 5, 25, 10)

    data_root = get_data_root()
    fact_matches_path = data_root / "fact_matches.parquet"

    st.header("Player Match History")
    df = pq.get_player_matches(fact_matches_path, player_tag, n_matches)
    if df.empty:
        st.warning("No data found.")
    else:
        st.dataframe(df)

    st.header("Player Winrate")
    winrate_df = pq.get_player_winrate_last_n(fact_matches_path, player_tag, n_matches)
    if not winrate_df.empty:
        games_used = int(winrate_df["games_played"].iloc[0])
        st.metric(
            f"Winrate (Last {games_used} Games)",
            f"{winrate_df['winrate'].iloc[0] * 100:.1f}%",
        )
        st.metric("Games Played", games_used)

    st.header("Player vs Club Winrate")
    # Player winrate: last n_matches games; Club winrate: last 100 games (or less)
    winrate_comp = pq.get_player_vs_club_winrate(player_tag, n_matches)
    club_winrate_100 = None
    club_tag = winrate_comp["club_tag"] if winrate_comp else None
    if club_tag:
        club_winrate_100_df = cq.get_club_winrate_last_n(fact_matches_path, club_tag, 100)
        if not club_winrate_100_df.empty:
            club_winrate_100 = club_winrate_100_df["winrate"].iloc[0]
            club_games_used = int(club_winrate_100_df["games_played"].iloc[0])
    if winrate_comp and club_winrate_100 is not None:
        import pandas as pd

        winrate_df = pd.DataFrame(
            {
                "Entity": [
                    f"Player (Last {n_matches})",
                    f"Club (Last {club_games_used})",
                ],
                "Winrate": [winrate_comp["player_winrate"], club_winrate_100],
            }
        )
        winrate_df = winrate_df.set_index("Entity")
        st.bar_chart(winrate_df)
        st.caption(
            f"Comparison of the player's winrate over the last {n_matches} selected games with the club's winrate over the last {club_games_used} games (or fewer if not available)."
        )
        st.write(f"Club Tag: {club_tag}")
    elif winrate_comp:
        st.info(
            "Not enough data to calculate the club's winrate over the last 100 games."
        )
        st.write(f"Club Tag: {club_tag}")

    st.header("Winrate by Mode")
    winrate_mode_df = pq.get_player_winrate_by_mode(fact_matches_path, player_tag, n_matches)
    if not winrate_mode_df.empty:
        # Prepare data: scale winrate to percentage for better visualization
        plot_df = winrate_mode_df.copy()
        plot_df["winrate_percent"] = plot_df["winrate"] * 100
        # Melt the dataframe to long format for grouped bar chart
        plot_df = plot_df.melt(
            id_vars=["battle_mode"],
            value_vars=["games_played", "winrate_percent"],
            var_name="Metric",
            value_name="Value",
        )
        # Rename metrics for legend clarity
        metric_labels = {
            "games_played": "Games Played",
            "winrate_percent": "Winrate (%)",
        }
        plot_df["Metric"] = plot_df["Metric"].replace(metric_labels)
        fig = px.bar(
            plot_df,
            x="battle_mode",
            y="Value",
            color="Metric",
            barmode="group",
            labels={"battle_mode": "Game Mode", "Value": "Value"},
            title="Winrate and Number of Games Played by Mode",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "Each mode shows two bars: one for the number of games played, one for the winrate (in %)"
        )

elif mode == "Club":
    dim_clubs = load_dim_clubs()
    if dim_clubs.empty:
        st.warning("No clubs available in the dimension table.")
        st.stop()
    club_options = [f"{row['name']} ({row['tag']})" for _, row in dim_clubs.iterrows()]
    club_selection = st.selectbox("Select a club", club_options)
    club_tag = club_selection.split("(")[-1].replace(")", "").strip()

    data_root = get_data_root()
    fact_matches_path = data_root / "fact_matches.parquet"

    st.header("Club Winrate (All Time)")
    df = cq.get_club_winrate(fact_matches_path, club_tag)
    if df.empty:
        st.warning("No data found for this club.")
    else:
        st.dataframe(df)

    st.header("Club Winrate (Last Day)")
    winrate_day_df = cq.get_club_winrate_last_day(fact_matches_path, club_tag)
    if not winrate_day_df.empty:
        st.metric(
            "Winrate (Last Day)", f"{winrate_day_df['winrate'].iloc[0] * 100:.1f}%"
        )
        st.metric(
            "Games Played (Last Day)", int(winrate_day_df["games_played"].iloc[0])
        )

    st.header("Club Win/Loss by Day")
    winloss_day_df = cq.get_club_winloss_by_day(fact_matches_path, club_tag)
    if not winloss_day_df.empty:
        st.line_chart(winloss_day_df.set_index("day")[["wins", "losses"]])

    st.header("Top 10 Club Member Participation")
    member_part_df = cq.get_club_member_participation(fact_matches_path, club_tag)
    if not member_part_df.empty:
        st.bar_chart(member_part_df.set_index("player_tag")["games_played"])

    st.header("Club Activity Over Time")
    activity_df = cq.get_club_activity_over_time(fact_matches_path, club_tag)
    if not activity_df.empty:
        st.line_chart(activity_df.set_index("day")["games_played"])

elif mode == "Global":
    data_root = get_data_root()
    fact_matches_path = data_root / "fact_matches.parquet"

    st.header("Club Comparison by Winrate")
    club_comp_df = cq.get_club_comparison_by_winrate(fact_matches_path)
    if not club_comp_df.empty:
        st.bar_chart(club_comp_df.set_index("club_tag")["winrate"])

    st.header("Most Popular Map")
    map_df = gq.get_most_popular_map(fact_matches_path)
    if not map_df.empty:
        st.metric("Most Popular Map", map_df["map_name"].iloc[0])

    st.header("Game Mode Distribution")
    mode_dist_df = gq.get_game_mode_distribution(fact_matches_path)
    if not mode_dist_df.empty:
        st.bar_chart(mode_dist_df.set_index("battle_mode")["games_played"])

    st.header("Winrate by Game Mode")
    winrate_mode_df = gq.get_winrate_by_game_mode(fact_matches_path)
    if not winrate_mode_df.empty:
        st.bar_chart(winrate_mode_df.set_index("battle_mode")["winrate"])
