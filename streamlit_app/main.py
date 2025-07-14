import sys
from pathlib import Path

import streamlit as st

# 🧹 Ajouter src/ au PYTHONPATH pour importer le module
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from brawlstar_project.analytics import duckdb_queries as dq

st.title("BrawlStars Dashboard")

player_tag = st.text_input("Entrez le tag du joueur")
n_matches = st.slider("Nombre de matchs", 5, 50, 25)

if player_tag:
    df = dq.get_player_matches(player_tag, n_matches)

    if df.empty:
        st.warning("Aucune donnée trouvée.")
    else:
        st.write(df)
