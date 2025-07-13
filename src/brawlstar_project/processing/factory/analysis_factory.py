"""
Analysis factory for different analysis modes.

This module provides analysis runners for different types of analysis:
- single player analysis
- all players analysis
- club analysis
"""

from pathlib import Path
from typing import Any, Dict

import polars as pl

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.factory.base_factory import BaseFactory, BaseRunner
from brawlstar_project.processing.processed import BattlelogAnalysis, PlayerAnalysis


class AnalysisRunner(BaseRunner):
    """Base class for analysis runners."""

    def __init__(self):
        self.results: Dict[str, Any] = {}


class SinglePlayerAnalysisRunner(AnalysisRunner):
    """Runner for analyzing a single player."""

    def run(self, **kwargs) -> dict:
        player_tag = kwargs.get("player_tag")
        if not player_tag:
            return {"status": "error", "message": "player_tag is required"}
        
        data_dir = kwargs.get("data_dir", "data")
        days = kwargs.get("days", 1)

        print(f"\n🚀 Loading player data for {player_tag}...")
        player = Player(player_tag)
        player_df = player.load_player_data(Path(data_dir) / "raw", days)

        # Analyze player data
        if not player_df.is_empty():
            print("\n" + "="*60)
            print("📊 PLAYER ANALYSIS")
            print("="*60)
            player_analysis = PlayerAnalysis(player_df)
            player_analysis.display_basic_stats()
            self.results["player_analysis"] = True

        # Load and analyze battlelog data
        print(f"\n⚔️ Loading battlelog data for {player_tag}...")
        battlelog_df = player.load_battlelog_data(Path(data_dir) / "raw", days)

        if not battlelog_df.is_empty():
            print("\n" + "="*60)
            print("⚔️ BATTLELOG ANALYSIS")
            print("="*60)
            battlelog_analysis = BattlelogAnalysis(battlelog_df)
            battlelog_analysis.display_battlelog_count_per_player()
            battlelog_analysis.display_battle_stats()
            self.results["battlelog_analysis"] = True

        return {
            "status": "success",
            "player_tag": player_tag,
            "has_player_data": not player_df.is_empty(),
            "has_battlelog_data": not battlelog_df.is_empty(),
            "results": self.results
        }


class AllPlayersAnalysisRunner(AnalysisRunner):
    """Runner for analyzing all available players."""

    def run(self, **kwargs) -> dict:
        data_dir = kwargs.get("data_dir", "data")
        
        print("\n🚀 Loading all player data...")
        
        # Load all player data
        raw_dir = Path(data_dir) / "raw"
        player_files = list(raw_dir.glob("player/*/player.parquet"))
        
        if not player_files:
            print("❌ No player data found!")
            return {"status": "error", "message": "No player data found"}

        # Load all player data
        player_dfs = []
        for player_file in player_files:
            try:
                df = pl.read_parquet(player_file)
                player_dfs.append(df)
            except Exception as e:
                print(f"⚠️ Error loading {player_file}: {e}")

        if not player_dfs:
            print("❌ No valid player data found!")
            return {"status": "error", "message": "No valid player data found"}

        # Combine all player data
        all_players_df = pl.concat(player_dfs)
        print(f"📊 Loaded data for {all_players_df['tag'].n_unique()} unique players")

        # Analyze all players
        print("\n" + "="*60)
        print("📊 ALL PLAYERS ANALYSIS")
        print("="*60)
        player_analysis = PlayerAnalysis(all_players_df)
        player_analysis.display_basic_stats()
        self.results["player_analysis"] = True

        # Load all battlelog data
        print("\n⚔️ Loading all battlelog data...")
        battlelog_files = list(raw_dir.glob("player/*/battlelog.parquet"))

        if battlelog_files:
            battlelog_dfs = []
            for battlelog_file in battlelog_files:
                try:
                    df = pl.read_parquet(battlelog_file)
                    battlelog_dfs.append(df)
                except Exception as e:
                    print(f"⚠️ Error loading {battlelog_file}: {e}")

            if battlelog_dfs:
                all_battlelog_df = pl.concat(battlelog_dfs)
                print(f"📊 Loaded battlelog data for {all_battlelog_df['player_tag'].n_unique()} unique players")

                print("\n" + "="*60)
                print("⚔️ ALL PLAYERS BATTLELOG ANALYSIS")
                print("="*60)
                battlelog_analysis = BattlelogAnalysis(all_battlelog_df)
                battlelog_analysis.display_battlelog_count_per_player()
                battlelog_analysis.display_battle_stats()
                self.results["battlelog_analysis"] = True
            else:
                print("❌ No valid battlelog data found!")
        else:
            print("❌ No battlelog data found!")

        return {
            "status": "success",
            "total_players": all_players_df['tag'].n_unique(),
            "results": self.results
        }


class ClubAnalysisRunner(AnalysisRunner):
    """Runner for analyzing club data."""

    def run(self, **kwargs) -> dict:
        club_tag = kwargs.get("club_tag")
        if not club_tag:
            return {"status": "error", "message": "club_tag is required"}
        
        data_dir = kwargs.get("data_dir", "data")
        days = kwargs.get("days", 1)

        print(f"\n🏛️ Loading club data for {club_tag}...")
        club = Club(club_tag)
        club_df = club.load_club_data(Path(data_dir) / "raw", days)
        print(club_df)

        print(f"\n👥 Loading club members data for {club_tag}...")
        club_members_df = club.load_club_members_data(Path(data_dir) / "raw", days)
        print(club_members_df)

        return {
            "status": "success",
            "club_tag": club_tag,
            "has_club_data": not club_df.is_empty(),
            "has_members_data": not club_members_df.is_empty()
        }


class AnalysisFactory(BaseFactory):
    """Factory for analysis runners."""

    def __init__(self):
        super().__init__()
        # Register default analysis modes
        self.register("single-player", SinglePlayerAnalysisRunner)
        self.register("all-players", AllPlayersAnalysisRunner)
        self.register("club", ClubAnalysisRunner) 