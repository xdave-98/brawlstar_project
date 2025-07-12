"""
Pydantic models for Brawl Stars battlelog data structures.

This module defines Pydantic models for type-safe handling of Brawl Stars battlelog API data,
providing validation, serialization, and deserialization capabilities.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class BrawlerInBattle(BaseModel):
    """Model for brawler information in battle."""

    id: int = Field(description="Brawler ID")
    name: str = Field(description="Brawler name")
    power: int = Field(description="Brawler power level", ge=1, le=11)
    trophies: int = Field(description="Brawler trophies", ge=0)


class BattlePlayer(BaseModel):
    """Model for player in battle."""

    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    brawler: BrawlerInBattle = Field(description="Brawler used in battle")


class StarPlayer(BaseModel):
    """Model for star player in battle."""

    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    brawler: BrawlerInBattle = Field(description="Brawler used by star player")


class BattleEvent(BaseModel):
    """Model for battle event information."""

    id: int = Field(description="Event ID")
    mode: str = Field(description="Event mode")
    map: str = Field(description="Event map")


class BattleDetails(BaseModel):
    """Model for battle details."""

    mode: str = Field(description="Battle mode")
    type: str = Field(description="Battle type")
    result: str = Field(description="Battle result")
    duration: int = Field(description="Battle duration in seconds")
    starPlayer: Optional[StarPlayer] = Field(default=None, description="Star player")
    teams: List[List[BattlePlayer]] = Field(description="Battle teams")

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API


class Battle(BaseModel):
    """Model for individual battle data."""

    battleTime: str = Field(description="Battle timestamp")
    event: BattleEvent = Field(description="Event information")
    battle: BattleDetails = Field(description="Battle details")

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API


class BattlelogData(BaseModel):
    """Model for complete battlelog data from Brawl Stars API."""

    items: List[Battle] = Field(description="List of battles")

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API


class FlattenedBattleData(BaseModel):
    """Flattened battle data model for analysis."""

    # Battle info
    battle_time: str = Field(alias="battleTime", description="Battle timestamp")
    event_mode: str = Field(alias="eventMode", description="Event mode")
    event_map: str = Field(alias="eventMap", description="Event map")
    battle_mode: str = Field(alias="battleMode", description="Battle mode")
    battle_type: str = Field(alias="battleType", description="Battle type")
    battle_result: str = Field(alias="battleResult", description="Battle result")
    battle_duration: int = Field(
        alias="battleDuration", description="Battle duration in seconds"
    )

    # Player info
    player_tag: str = Field(alias="playerTag", description="Player tag")
    player_name: str = Field(alias="playerName", description="Player name")
    brawler_name: str = Field(alias="brawlerName", description="Brawler used")
    brawler_power: int = Field(alias="brawlerPower", description="Brawler power level")
    brawler_trophies: int = Field(
        alias="brawlerTrophies", description="Brawler trophies"
    )

    # Team info
    team_size: int = Field(alias="teamSize", description="Team size")
    opponent_count: int = Field(
        alias="opponentCount", description="Number of opponents"
    )
    is_star_player: bool = Field(
        alias="isStarPlayer", description="Whether player was star player"
    )

    # Timestamp
    extracted_at: datetime = Field(description="Data extraction timestamp")

    model_config = ConfigDict(
        populate_by_name=True, json_encoders={datetime: lambda v: v.isoformat()}
    )


def create_flattened_battle_data(
    raw_data: dict, player_tag: str
) -> List[FlattenedBattleData]:
    """
    Create flattened battle data from raw API response.

    Args:
        raw_data: Raw battlelog data from Brawl Stars API
        player_tag: Player tag for reference

    Returns:
        List of FlattenedBattleData instances
    """
    # Parse raw data with BattlelogData model for validation
    battlelog_data = BattlelogData.model_validate(raw_data)

    flattened_battles = []

    for battle in battlelog_data.items:
        # Find the player in the battle
        player_battle_info = None
        team_size = 0
        opponent_count = 0
        is_star_player = False

        # Extract team information
        if battle.battle.teams:
            for team in battle.battle.teams:
                team_size = len(team)
                for player in team:
                    if player.tag == player_tag:
                        player_battle_info = player
                        break
                if player_battle_info:
                    break
            opponent_count = sum(len(team) for team in battle.battle.teams) - team_size

        # Check if player was star player
        if battle.battle.starPlayer and battle.battle.starPlayer.tag == player_tag:
            is_star_player = True

        # Extract brawler information
        brawler_name = ""
        brawler_power = 0
        brawler_trophies = 0

        if player_battle_info:
            brawler_name = player_battle_info.brawler.name
            brawler_power = player_battle_info.brawler.power
            brawler_trophies = player_battle_info.brawler.trophies

        # Create flattened battle data
        flattened_dict = {
            "battleTime": battle.battleTime,
            "eventMode": battle.event.mode,
            "eventMap": battle.event.map,
            "battleMode": battle.battle.mode,
            "battleType": battle.battle.type,
            "battleResult": battle.battle.result,
            "battleDuration": battle.battle.duration,
            "playerTag": player_tag,
            "playerName": player_battle_info.name if player_battle_info else "",
            "brawlerName": brawler_name,
            "brawlerPower": brawler_power,
            "brawlerTrophies": brawler_trophies,
            "teamSize": team_size,
            "opponentCount": opponent_count,
            "isStarPlayer": is_star_player,
            "extracted_at": datetime.now(),
        }

        flattened_battles.append(FlattenedBattleData.model_validate(flattened_dict))

    return flattened_battles
