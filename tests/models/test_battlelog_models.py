"""
Tests for battlelog Pydantic models in Brawl Stars data processing.
"""

from datetime import datetime

import pytest

from brawlstar_project.player.models.battlelog import (
    Battle,
    BattleDetails,
    BattleEvent,
    BattlelogData,
    BattlePlayer,
    BrawlerInBattle,
    FlattenedBattleData,
    create_flattened_battle_data,
)


class TestBrawlerInBattle:
    """Test BrawlerInBattle model."""

    def test_valid_brawler(self):
        """Test valid brawler data."""
        brawler_data = {"id": 16000047, "name": "SQUEAK", "power": 11, "trophies": 14}
        brawler = BrawlerInBattle.model_validate(brawler_data)

        assert brawler.id == 16000047
        assert brawler.name == "SQUEAK"
        assert brawler.power == 11
        assert brawler.trophies == 14

    def test_invalid_brawler_power(self):
        """Test invalid brawler power level."""
        brawler_data = {
            "id": 16000047,
            "name": "SQUEAK",
            "power": 12,  # Invalid power level
            "trophies": 14,
        }

        with pytest.raises(ValueError):
            BrawlerInBattle.model_validate(brawler_data)


class TestBattlePlayer:
    """Test BattlePlayer model."""

    def test_valid_battle_player(self):
        """Test valid battle player data."""
        player_data = {
            "tag": "#PC0PPLRU",
            "name": "Gobelin-Poilu",
            "brawler": {"id": 16000047, "name": "SQUEAK", "power": 11, "trophies": 14},
        }
        player = BattlePlayer.model_validate(player_data)

        assert player.tag == "#PC0PPLRU"
        assert player.name == "Gobelin-Poilu"
        assert player.brawler.name == "SQUEAK"
        assert player.brawler.power == 11


class TestBattleEvent:
    """Test BattleEvent model."""

    def test_valid_battle_event(self):
        """Test valid battle event data."""
        event_data = {"id": 15000132, "mode": "brawlBall", "map": "Center Stage"}
        event = BattleEvent.model_validate(event_data)

        assert event.id == 15000132
        assert event.mode == "brawlBall"
        assert event.map == "Center Stage"


class TestBattleDetails:
    """Test BattleDetails model."""

    def test_valid_battle_details(self):
        """Test valid battle details data."""
        battle_data = {
            "mode": "brawlBall",
            "type": "soloRanked",
            "result": "defeat",
            "duration": 115,
            "starPlayer": {
                "tag": "#2PJRY0U8V",
                "name": "Assassins.bug",
                "brawler": {
                    "id": 16000004,
                    "name": "RICO",
                    "power": 11,
                    "trophies": 13,
                },
            },
            "teams": [
                [
                    {
                        "tag": "#PC0PPLRU",
                        "name": "Gobelin-Poilu",
                        "brawler": {
                            "id": 16000047,
                            "name": "SQUEAK",
                            "power": 11,
                            "trophies": 14,
                        },
                    }
                ]
            ],
        }
        battle = BattleDetails.model_validate(battle_data)

        assert battle.mode == "brawlBall"
        assert battle.type == "soloRanked"
        assert battle.result == "defeat"
        assert battle.duration == 115
        assert battle.starPlayer is not None
        assert getattr(battle.starPlayer, "tag", None) == "#2PJRY0U8V"
        assert len(battle.teams) == 1
        assert len(battle.teams[0]) == 1


class TestBattle:
    """Test Battle model."""

    def test_valid_battle(self):
        """Test valid battle data."""
        battle_data = {
            "battleTime": "20250711T162154.000Z",
            "event": {"id": 15000132, "mode": "brawlBall", "map": "Center Stage"},
            "battle": {
                "mode": "brawlBall",
                "type": "soloRanked",
                "result": "defeat",
                "duration": 115,
                "starPlayer": {
                    "tag": "#2PJRY0U8V",
                    "name": "Assassins.bug",
                    "brawler": {
                        "id": 16000004,
                        "name": "RICO",
                        "power": 11,
                        "trophies": 13,
                    },
                },
                "teams": [
                    [
                        {
                            "tag": "#PC0PPLRU",
                            "name": "Gobelin-Poilu",
                            "brawler": {
                                "id": 16000047,
                                "name": "SQUEAK",
                                "power": 11,
                                "trophies": 14,
                            },
                        }
                    ]
                ],
            },
        }
        battle = Battle.model_validate(battle_data)

        assert battle.battleTime == "20250711T162154.000Z"
        assert battle.event.mode == "brawlBall"
        assert battle.event.map == "Center Stage"
        assert battle.battle.mode == "brawlBall"
        assert battle.battle.result == "defeat"


class TestBattlelogData:
    """Test BattlelogData model."""

    def test_valid_battlelog_data(self):
        """Test valid battlelog data."""
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
                        "teams": [
                            [
                                {
                                    "tag": "#PC0PPLRU",
                                    "name": "Gobelin-Poilu",
                                    "brawler": {
                                        "id": 16000047,
                                        "name": "SQUEAK",
                                        "power": 11,
                                        "trophies": 14,
                                    },
                                }
                            ]
                        ],
                    },
                }
            ]
        }
        battlelog = BattlelogData.model_validate(battlelog_data)

        assert len(battlelog.items) == 1
        assert battlelog.items[0].battleTime == "20250711T162154.000Z"


class TestFlattenedBattleData:
    """Test FlattenedBattleData model."""

    def test_valid_flattened_battle_data(self):
        """Test valid flattened battle data."""
        flattened_data = {
            "battleTime": "20250711T162154.000Z",
            "eventMode": "brawlBall",
            "eventMap": "Center Stage",
            "battleMode": "brawlBall",
            "battleType": "soloRanked",
            "battleResult": "defeat",
            "battleDuration": 115,
            "playerTag": "#PC0PPLRU",
            "playerName": "Gobelin-Poilu",
            "brawlerName": "SQUEAK",
            "brawlerPower": 11,
            "brawlerTrophies": 14,
            "teamSize": 1,
            "opponentCount": 0,
            "isStarPlayer": False,
            "extracted_at": datetime.now(),
        }

        flattened = FlattenedBattleData.model_validate(flattened_data)

        assert flattened.battle_time == "20250711T162154.000Z"
        assert flattened.event_mode == "brawlBall"
        assert flattened.battle_result == "defeat"
        assert flattened.player_tag == "#PC0PPLRU"
        assert flattened.brawler_name == "SQUEAK"


class TestCreateFlattenedBattleData:
    """Test create_flattened_battle_data function."""

    def test_create_flattened_battle_data(self):
        """Test creating flattened battle data from raw API response."""
        raw_data = {
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
                        "starPlayer": {
                            "tag": "#2PJRY0U8V",
                            "name": "Assassins.bug",
                            "brawler": {
                                "id": 16000004,
                                "name": "RICO",
                                "power": 11,
                                "trophies": 13,
                            },
                        },
                        "teams": [
                            [
                                {
                                    "tag": "#PC0PPLRU",
                                    "name": "Gobelin-Poilu",
                                    "brawler": {
                                        "id": 16000047,
                                        "name": "SQUEAK",
                                        "power": 11,
                                        "trophies": 14,
                                    },
                                }
                            ],
                            [
                                {
                                    "tag": "#YG0CPCU0G",
                                    "name": "⚠️| GGecto 🗿",
                                    "brawler": {
                                        "id": 16000009,
                                        "name": "DYNAMIKE",
                                        "power": 11,
                                        "trophies": 11,
                                    },
                                }
                            ],
                        ],
                    },
                }
            ]
        }

        flattened_battles = create_flattened_battle_data(raw_data, "#PC0PPLRU")

        assert len(flattened_battles) == 1
        battle = flattened_battles[0]

        assert battle.battle_time == "20250711T162154.000Z"
        assert battle.event_mode == "brawlBall"
        assert battle.event_map == "Center Stage"
        assert battle.battle_mode == "brawlBall"
        assert battle.battle_type == "soloRanked"
        assert battle.battle_result == "defeat"
        assert battle.battle_duration == 115
        assert battle.player_tag == "#PC0PPLRU"
        assert battle.player_name == "Gobelin-Poilu"
        assert battle.brawler_name == "SQUEAK"
        assert battle.brawler_power == 11
        assert battle.brawler_trophies == 14
        assert battle.team_size == 1
        assert battle.opponent_count == 1
        assert not battle.is_star_player

    def test_create_flattened_battle_data_star_player(self):
        """Test creating flattened battle data for star player."""
        raw_data = {
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
                        "result": "victory",
                        "duration": 115,
                        "starPlayer": {
                            "tag": "#PC0PPLRU",
                            "name": "Gobelin-Poilu",
                            "brawler": {
                                "id": 16000047,
                                "name": "SQUEAK",
                                "power": 11,
                                "trophies": 14,
                            },
                        },
                        "teams": [
                            [
                                {
                                    "tag": "#PC0PPLRU",
                                    "name": "Gobelin-Poilu",
                                    "brawler": {
                                        "id": 16000047,
                                        "name": "SQUEAK",
                                        "power": 11,
                                        "trophies": 14,
                                    },
                                }
                            ]
                        ],
                    },
                }
            ]
        }

        flattened_battles = create_flattened_battle_data(raw_data, "#PC0PPLRU")

        assert len(flattened_battles) == 1
        battle = flattened_battles[0]

        assert battle.is_star_player
        assert battle.battle_result == "victory"


class TestModelSerialization:
    """Test model serialization and deserialization."""

    def test_battle_serialization(self):
        """Test Battle model serialization."""
        battle_data = {
            "battleTime": "20250711T162154.000Z",
            "event": {"id": 15000132, "mode": "brawlBall", "map": "Center Stage"},
            "battle": {
                "mode": "brawlBall",
                "type": "soloRanked",
                "result": "defeat",
                "duration": 115,
                "teams": [],
            },
        }

        battle = Battle.model_validate(battle_data)
        serialized = battle.model_dump()

        assert serialized["battleTime"] == "20250711T162154.000Z"
        assert serialized["event"]["mode"] == "brawlBall"
        assert serialized["battle"]["result"] == "defeat"

    def test_flattened_battle_data_json_serialization(self):
        """Test FlattenedBattleData JSON serialization."""
        flattened_data = {
            "battleTime": "20250711T162154.000Z",
            "eventMode": "brawlBall",
            "eventMap": "Center Stage",
            "battleMode": "brawlBall",
            "battleType": "soloRanked",
            "battleResult": "defeat",
            "battleDuration": 115,
            "playerTag": "#PC0PPLRU",
            "playerName": "Gobelin-Poilu",
            "brawlerName": "SQUEAK",
            "brawlerPower": 11,
            "brawlerTrophies": 14,
            "teamSize": 1,
            "opponentCount": 0,
            "isStarPlayer": False,
            "extracted_at": datetime.now(),
        }

        flattened = FlattenedBattleData.model_validate(flattened_data)
        json_data = flattened.model_dump_json()

        # Verify JSON can be parsed back
        parsed = FlattenedBattleData.model_validate_json(json_data)
        assert parsed.player_tag == "#PC0PPLRU"
        assert parsed.brawler_name == "SQUEAK"
