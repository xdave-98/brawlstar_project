"""
Tests for Pydantic models in Brawl Stars data processing.
"""

from datetime import datetime

import pytest

from brawlstar_project.entities.player.models import (
    Brawler,
    Club,
    FlattenedPlayerData,
    PlayerData,
    create_flattened_player_data,
)


class TestClub:
    """Test Club model."""

    def test_valid_club(self):
        """Test valid club data."""
        club_data = {"name": "Test Club", "tag": "#CLUB123"}
        club = Club.model_validate(club_data)

        assert club.name == "Test Club"
        assert club.tag == "#CLUB123"

    def test_invalid_club_missing_fields(self):
        """Test invalid club data with missing fields."""
        club_data = {"name": "Test Club"}  # Missing tag

        with pytest.raises(ValueError):
            Club.model_validate(club_data)


class TestBrawler:
    """Test Brawler model."""

    def test_valid_brawler(self):
        """Test valid brawler data."""
        brawler_data = {
            "id": 1,
            "name": "Shelly",
            "power": 11,
            "rank": 25,
            "trophies": 500,
            "highestTrophies": 600,
        }
        brawler = Brawler.model_validate(brawler_data)

        assert brawler.id == 1
        assert brawler.name == "Shelly"
        assert brawler.power == 11
        assert brawler.rank == 25
        assert brawler.trophies == 500
        assert brawler.highestTrophies == 600

    def test_invalid_brawler_power(self):
        """Test invalid brawler power level."""
        brawler_data = {
            "id": 1,
            "name": "Shelly",
            "power": 12,  # Invalid power level
            "rank": 25,
            "trophies": 500,
            "highestTrophies": 600,
        }

        with pytest.raises(ValueError):
            Brawler.model_validate(brawler_data)


class TestPlayerData:
    """Test PlayerData model."""

    def test_valid_player_data(self):
        """Test valid player data."""
        player_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "nameColor": "#FF0000",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "3vs3Victories": 100,
            "soloVictories": 50,
            "duoVictories": 25,
            "bestRoboRumbleTime": 300,
            "bestTimeAsBigBrawler": 600,
            "club": {"name": "Test Club", "tag": "#CLUB123"},
            "brawlers": [
                {
                    "id": 1,
                    "name": "Shelly",
                    "power": 11,
                    "rank": 25,
                    "trophies": 500,
                    "highestTrophies": 600,
                }
            ],
        }

        player = PlayerData.model_validate(player_data)

        assert player.tag == "#PLAYER123"
        assert player.name == "TestPlayer"
        assert player.trophies == 1000
        assert player.club is not None
        assert getattr(player.club, "name", None) == "Test Club"
        assert len(player.brawlers) == 1
        assert player.brawlers[0].name == "Shelly"

    def test_player_data_without_club(self):
        """Test player data without club information."""
        player_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "brawlers": [],
        }

        player = PlayerData.model_validate(player_data)

        assert player.tag == "#PLAYER123"
        assert player.club is None
        assert len(player.brawlers) == 0


class TestFlattenedPlayerData:
    """Test FlattenedPlayerData model."""

    def test_valid_flattened_data(self):
        """Test valid flattened player data."""
        flattened_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "nameColor": "#FF0000",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "3vs3Victories": 100,
            "soloVictories": 50,
            "duoVictories": 25,
            "bestRoboRumbleTime": 300,
            "bestTimeAsBigBrawler": 600,
            "club_name": "Test Club",
            "club_tag": "#CLUB123",
            "total_brawlers": 5,
            "maxed_brawlers": 2,
            "total_brawler_trophies": 2500,
            "extracted_at": datetime.now(),
        }

        flattened = FlattenedPlayerData.model_validate(flattened_data)

        assert flattened.tag == "#PLAYER123"
        assert flattened.name == "TestPlayer"
        assert flattened.total_brawlers == 5
        assert flattened.maxed_brawlers == 2
        assert flattened.total_brawler_trophies == 2500


class TestCreateFlattenedPlayerData:
    """Test create_flattened_player_data function."""

    def test_create_flattened_data(self):
        """Test creating flattened data from raw player data."""
        raw_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "nameColor": "#FF0000",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "3vs3Victories": 100,
            "soloVictories": 50,
            "duoVictories": 25,
            "bestRoboRumbleTime": 300,
            "bestTimeAsBigBrawler": 600,
            "club": {"name": "Test Club", "tag": "#CLUB123"},
            "brawlers": [
                {
                    "id": 1,
                    "name": "Shelly",
                    "power": 11,
                    "rank": 25,
                    "trophies": 500,
                    "highestTrophies": 600,
                },
                {
                    "id": 2,
                    "name": "Colt",
                    "power": 9,
                    "rank": 20,
                    "trophies": 400,
                    "highestTrophies": 450,
                },
            ],
        }

        flattened = create_flattened_player_data(raw_data)

        assert flattened.tag == "#PLAYER123"
        assert flattened.name == "TestPlayer"
        assert flattened.total_brawlers == 2
        assert flattened.maxed_brawlers == 1  # Only Shelly is maxed (power 11)
        assert flattened.total_brawler_trophies == 900  # 500 + 400
        assert flattened.club_name == "Test Club"
        assert flattened.club_tag == "#CLUB123"

    def test_create_flattened_data_no_club(self):
        """Test creating flattened data for player without club."""
        raw_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "brawlers": [],
        }

        flattened = create_flattened_player_data(raw_data)

        assert flattened.tag == "#PLAYER123"
        assert flattened.club_name == ""
        assert flattened.club_tag == ""
        assert flattened.total_brawlers == 0
        assert flattened.maxed_brawlers == 0
        assert flattened.total_brawler_trophies == 0


class TestModelSerialization:
    """Test model serialization and deserialization."""

    def test_player_data_serialization(self):
        """Test PlayerData model serialization."""
        player_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "brawlers": [],
        }

        player = PlayerData.model_validate(player_data)
        serialized = player.model_dump()

        assert serialized["tag"] == "#PLAYER123"
        assert serialized["name"] == "TestPlayer"
        assert serialized["trophies"] == 1000

    def test_flattened_data_json_serialization(self):
        """Test FlattenedPlayerData JSON serialization."""
        flattened_data = {
            "tag": "#PLAYER123",
            "name": "TestPlayer",
            "nameColor": "#FF0000",
            "trophies": 1000,
            "highestTrophies": 1200,
            "expLevel": 50,
            "expPoints": 50000,
            "3vs3Victories": 100,
            "soloVictories": 50,
            "duoVictories": 25,
            "bestRoboRumbleTime": 300,
            "bestTimeAsBigBrawler": 600,
            "club_name": "Test Club",
            "club_tag": "#CLUB123",
            "total_brawlers": 5,
            "maxed_brawlers": 2,
            "total_brawler_trophies": 2500,
            "extracted_at": datetime.now(),
        }

        flattened = FlattenedPlayerData.model_validate(flattened_data)
        json_data = flattened.model_dump_json()

        # Verify JSON can be parsed back
        parsed = FlattenedPlayerData.model_validate_json(json_data)
        assert parsed.tag == "#PLAYER123"
        assert parsed.name == "TestPlayer"
