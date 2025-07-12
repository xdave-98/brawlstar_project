"""
Pydantic models for Brawl Stars data structures.

This module defines Pydantic models for type-safe handling of Brawl Stars API data,
providing validation, serialization, and deserialization capabilities.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


class Club(BaseModel):
    """Model for club information."""
    
    name: str = Field(description="Club name")
    tag: str = Field(description="Club tag")


class Brawler(BaseModel):
    """Model for individual brawler data."""
    
    id: int = Field(description="Brawler ID")
    name: str = Field(description="Brawler name")
    power: int = Field(description="Brawler power level", ge=1, le=11)
    rank: int = Field(description="Brawler rank", ge=0)
    trophies: int = Field(description="Brawler trophies", ge=0)
    highestTrophies: int = Field(description="Highest trophies achieved", ge=0)
    gears: Optional[List[dict]] = Field(default=None, description="Gears")
    starPowers: Optional[List[dict]] = Field(default=None, description="Star powers")
    gadgets: Optional[List[dict]] = Field(default=None, description="Gadgets")


class PlayerData(BaseModel):
    """Model for complete player data from Brawl Stars API."""
    
    model_config = ConfigDict(extra="allow")  # Allow extra fields from API
    
    # Basic player info
    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    nameColor: Optional[str] = Field(default=None, description="Player name color")
    trophies: int = Field(description="Current trophies", ge=0)
    highestTrophies: int = Field(description="Highest trophies achieved", ge=0)
    expLevel: int = Field(description="Experience level", ge=1)
    expPoints: int = Field(description="Experience points", ge=0)
    
    # Battle statistics
    three_vs_three_victories: Optional[int] = Field(
        default=None, alias="3vs3Victories", description="3v3 victories"
    )
    solo_victories: Optional[int] = Field(
        default=None, alias="soloVictories", description="Solo victories"
    )
    duo_victories: Optional[int] = Field(
        default=None, alias="duoVictories", description="Duo victories"
    )
    best_robo_rumble_time: Optional[int] = Field(
        default=None, alias="bestRoboRumbleTime", description="Best Robo Rumble time"
    )
    best_time_as_big_brawler: Optional[int] = Field(
        default=None, alias="bestTimeAsBigBrawler", description="Best time as Big Brawler"
    )
    
    # Club info
    club: Optional[Club] = Field(default=None, description="Club information")
    
    # Brawlers
    brawlers: List[Brawler] = Field(default_factory=list, description="List of brawlers")


class FlattenedPlayerData(BaseModel):
    """Flattened player data model for analysis."""
    
    # Basic player info
    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    name_color: str = Field(alias="nameColor", description="Player name color")
    trophies: int = Field(description="Current trophies", ge=0)
    highest_trophies: int = Field(alias="highestTrophies", description="Highest trophies achieved", ge=0)
    exp_level: int = Field(alias="expLevel", description="Experience level", ge=1)
    exp_points: int = Field(alias="expPoints", description="Experience points", ge=0)
    
    # Battle statistics
    three_vs_three_victories: int = Field(
        alias="3vs3Victories", description="3v3 victories", ge=0
    )
    solo_victories: int = Field(alias="soloVictories", description="Solo victories", ge=0)
    duo_victories: int = Field(alias="duoVictories", description="Duo victories", ge=0)
    best_robo_rumble_time: int = Field(
        alias="bestRoboRumbleTime", description="Best Robo Rumble time", ge=0
    )
    best_time_as_big_brawler: int = Field(
        alias="bestTimeAsBigBrawler", description="Best time as Big Brawler", ge=0
    )
    
    # Club info
    club_name: str = Field(description="Club name")
    club_tag: str = Field(description="Club tag")
    
    # Brawler summary
    total_brawlers: int = Field(description="Total number of brawlers", ge=0)
    maxed_brawlers: int = Field(description="Number of maxed brawlers", ge=0)
    total_brawler_trophies: int = Field(description="Total brawler trophies", ge=0)
    
    # Timestamp
    extracted_at: datetime = Field(description="Data extraction timestamp")
    
    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


def create_flattened_player_data(raw_data: dict) -> FlattenedPlayerData:
    """
    Create flattened player data from raw API response.
    
    Args:
        raw_data: Raw player data from Brawl Stars API
        
    Returns:
        FlattenedPlayerData instance with validated data
    """
    # Parse raw data with PlayerData model for validation
    player_data = PlayerData.model_validate(raw_data)
    
    # Calculate derived fields
    total_brawlers = len(player_data.brawlers)
    maxed_brawlers = sum(1 for b in player_data.brawlers if b.power == 11)
    total_brawler_trophies = sum(b.trophies for b in player_data.brawlers)
    
    # Create flattened data
    flattened_dict = {
        "tag": player_data.tag,
        "name": player_data.name,
        "nameColor": player_data.nameColor or "",
        "trophies": player_data.trophies,
        "highestTrophies": player_data.highestTrophies,
        "expLevel": player_data.expLevel,
        "expPoints": player_data.expPoints,
        "3vs3Victories": player_data.three_vs_three_victories or 0,
        "soloVictories": player_data.solo_victories or 0,
        "duoVictories": player_data.duo_victories or 0,
        "bestRoboRumbleTime": player_data.best_robo_rumble_time or 0,
        "bestTimeAsBigBrawler": player_data.best_time_as_big_brawler or 0,
        "club_name": player_data.club.name if player_data.club else "",
        "club_tag": player_data.club.tag if player_data.club else "",
        "total_brawlers": total_brawlers,
        "maxed_brawlers": maxed_brawlers,
        "total_brawler_trophies": total_brawler_trophies,
        "extracted_at": datetime.now()
    }
    
    return FlattenedPlayerData.model_validate(flattened_dict) 