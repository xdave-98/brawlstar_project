"""
Tests for generic utility functions in Brawl Stars data processing.
"""

import os
import tempfile
from pathlib import Path

import polars as pl

from brawlstar_project.processing.utils import (
    convert_json_to_parquet_generic,
    flatten_battlelog_data,
    flatten_player_data,
    save_json_data,
    save_json_decorator,
)


class TestSaveJsonData:
    """Test save_json_data function."""

    def test_save_json_data_basic(self):
        """Test basic JSON saving functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data = {"test": "data", "number": 42}
            player_tag = "PC0PPLRU"
            filename = "test.json"

            result_path = save_json_data(
                data=data, player_tag=player_tag, base_dir=temp_dir, filename=filename
            )

            # Check file was created
            assert os.path.exists(result_path)

            # Check content
            with open(result_path, "r") as f:
                import json

                saved_data = json.load(f)

            assert saved_data == data

    def test_save_json_data_with_validation(self):
        """Test JSON saving with validation function."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data = {"test": "data", "number": 42}
            player_tag = "PC0PPLRU"

            def validate_func(data_dict):
                data_dict["validated"] = True
                return data_dict

            result_path = save_json_data(
                data=data,
                player_tag=player_tag,
                base_dir=temp_dir,
                filename="test.json",
                validate_func=validate_func,
            )

            # Check content was validated
            with open(result_path, "r") as f:
                import json

                saved_data = json.load(f)

            assert saved_data["validated"]


class TestSaveJsonDecorator:
    """Test save_json_decorator function."""

    def test_save_json_decorator(self):
        """Test the decorator functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:

            @save_json_decorator(base_dir=temp_dir, filename="decorated.json")
            def test_validator(data, player_tag):
                data["decorated"] = True
                return data

            data = {"test": "data"}
            player_tag = "PC0PPLRU"

            result_path = test_validator(data, player_tag)

            # Check file was created
            assert os.path.exists(result_path)

            # Check content was processed by decorator
            with open(result_path, "r") as f:
                import json

                saved_data = json.load(f)

            assert saved_data["decorated"]


class TestSavePlayerData:
    """Test save_player_data decorated function."""

    def test_save_player_data_valid(self):
        """Test saving valid player data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock player data
            player_data = {
                "tag": "#PC0PPLRU",
                "name": "TestPlayer",
                "trophies": 1000,
                "highestTrophies": 1200,
                "expLevel": 50,
                "expPoints": 50000,
                "brawlers": [],
            }

            @save_json_decorator(base_dir=temp_dir, filename="player.json")
            def temp_save_player_data(data, player_tag):
                from brawlstar_project.player.player_models import PlayerData

                player_data = PlayerData.model_validate(data)
                return player_data.model_dump()

            result_path = temp_save_player_data(player_data, "PC0PPLRU")

            # Check file was created
            assert os.path.exists(result_path)
            assert result_path.endswith("player.json")


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
        assert "battleTime" in df.columns
        assert "event" in df.columns
        assert "battle" in df.columns

    def test_flatten_battlelog_data_empty(self):
        """Test flatten_battlelog_data with empty data."""
        battlelog_data = {"items": []}

        df = flatten_battlelog_data(battlelog_data)

        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()


class TestConvertJsonToParquetGeneric:
    """Test convert_json_to_parquet_generic function."""

    def test_convert_json_to_parquet_generic(self):
        """Test generic JSON to Parquet conversion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test directory structure
            ingested_dir = Path(temp_dir) / "ingested"
            raw_dir = Path(temp_dir) / "raw"

            # Create test data
            player_dir = ingested_dir / "PC0PPLRU" / "2025-01-01"
            player_dir.mkdir(parents=True, exist_ok=True)

            test_data = {"test": "data", "number": 42}
            json_file = player_dir / "test.json"

            with open(json_file, "w") as f:
                import json

                json.dump(test_data, f)

            def test_flatten_func(data):
                return pl.DataFrame([data])

            # Test conversion
            convert_json_to_parquet_generic(
                ingested_base_dir=str(ingested_dir),
                raw_base_dir=str(raw_dir),
                json_filename="test.json",
                parquet_filename="test.parquet",
                flatten_func=test_flatten_func,
            )

            # Check parquet file was created
            parquet_file = raw_dir / "PC0PPLRU" / "2025-01-01" / "test.parquet"
            assert parquet_file.exists()

            # Check data was converted correctly
            df = pl.read_parquet(str(parquet_file))
            assert not df.is_empty()
            assert "test" in df.columns
            assert "number" in df.columns


class TestIntegration:
    """Test integration of all components."""

    def test_full_pipeline_mock(self):
        """Test the full pipeline with mock data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the entire pipeline
            ingested_dir = Path(temp_dir) / "ingested"
            raw_dir = Path(temp_dir) / "raw"

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

            # Save player data
            @save_json_decorator(base_dir=str(ingested_dir), filename="player.json")
            def save_test_player_data(data, player_tag):
                return data

            save_test_player_data(player_data, "PC0PPLRU")

            # Convert to parquet
            convert_json_to_parquet_generic(
                ingested_base_dir=str(ingested_dir),
                raw_base_dir=str(raw_dir),
                json_filename="player.json",
                parquet_filename="player.parquet",
                flatten_func=flatten_player_data,
            )

            # Verify results - use current date format
            from datetime import datetime

            current_date = datetime.now().strftime("%Y-%m-%d")
            parquet_file = raw_dir / "PC0PPLRU" / current_date / "player.parquet"
            assert parquet_file.exists()

            df = pl.read_parquet(str(parquet_file))
            assert not df.is_empty()
            assert "tag" in df.columns
