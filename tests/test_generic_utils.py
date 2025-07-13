"""
Tests for generic utility functions in Brawl Stars data processing.
"""

import polars as pl

from brawlstar_project.processing.utils import (
    flatten_battlelog_data,
    flatten_player_data,
)


class TestFlattenFunctions:
    """Test flatten functions."""

    def test_flatten_player_data(self):
        """Test flatten_player_data function."""
        player_data = {
            "tag": "#PC0PPLRU",
            "name": "TestPlayer",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "brawlers": [],
        }

        df = flatten_player_data(player_data)

        assert isinstance(df, pl.DataFrame)
        assert not df.is_empty()
        assert "tag" in df.columns
        assert "name" in df.columns
        assert "trophies" in df.columns

    def test_flatten_battlelog_data(self):
        """Test flatten_battlelog_data function."""
        battlelog_data = {
            "items": [
                {
                    "battleTime": "20250711T162154.000Z",
                    "event": {
                        "id": 15000132,
                        "mode": "brawlBall",
                        "map": "Center Stage",
                    },
                    "battle": {
                        "mode": "brawlBall",
                        "type": "soloRanked",
                        "result": "defeat",
                        "duration": 115,
                        "teams": [],
                    },
                }
            ]
        }

        df = flatten_battlelog_data(battlelog_data)

        assert isinstance(df, pl.DataFrame)
        assert not df.is_empty()
        # The function returns snake_case column names
        assert "battle_time" in df.columns
        assert "event_mode" in df.columns
        assert "battle_mode" in df.columns

    def test_flatten_battlelog_data_empty(self):
        """Test flatten_battlelog_data with empty data."""
        battlelog_data = {"items": []}

        df = flatten_battlelog_data(battlelog_data)

        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()


class TestIntegration:
    """Test integration of all components."""

    def test_full_pipeline_mock(self):
        """Test the full pipeline with mock data."""
        # Create test player data
        player_data = {
            "tag": "#PC0PPLRU",
            "name": "TestPlayer",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "brawlers": [],
        }

        # Test flattening functionality
        df = flatten_player_data(player_data)

        # Verify results
        assert isinstance(df, pl.DataFrame)
        assert not df.is_empty()
        assert "tag" in df.columns
        assert "name" in df.columns
        assert "trophies" in df.columns

        # Test battlelog flattening
        battlelog_data = {
            "items": [
                {
                    "battleTime": "20250711T162154.000Z",
                    "event": {
                        "id": 15000132,
                        "mode": "brawlBall",
                        "map": "Center Stage",
                    },
                    "battle": {
                        "mode": "brawlBall",
                        "type": "soloRanked",
                        "result": "defeat",
                        "duration": 115,
                        "teams": [],
                    },
                }
            ]
        }

        battlelog_df = flatten_battlelog_data(battlelog_data)
        assert isinstance(battlelog_df, pl.DataFrame)
        assert not battlelog_df.is_empty()
