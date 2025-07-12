"""
Pydantic models for Brawl Stars club data.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from brawlstar_project.entities.club.models.members import ClubMember


class ClubData(BaseModel):
    """Model for club information from Brawl Stars API."""

    tag: str = Field(description="Club tag")
    name: str = Field(description="Club name")
    description: Optional[str] = Field(default=None, description="Club description")
    type: str = Field(description="Club type")
    badge_id: int = Field(alias="badgeId", description="Club badge ID")
    required_trophies: int = Field(
        alias="requiredTrophies", description="Required trophies to join"
    )
    trophies: int = Field(description="Club total trophies")
    members: list[ClubMember] = Field(description="List of club members")


def create_flattened_club_data(club_data: dict) -> "FlattenedClubData":
    """
    Create flattened club data from raw API response.

    Args:
        club_data: Raw club data from Brawl Stars API

    Returns:
        Flattened club data model
    """
    # Compute missing fields
    member_count = len(club_data.get("members", []))
    extracted_at = datetime.now().isoformat()

    # Create data with computed fields
    flattened_data = {
        **club_data,
        "member_count": member_count,
        "extracted_at": extracted_at,
    }

    return FlattenedClubData.model_validate(flattened_data)


class FlattenedClubData(BaseModel):
    """Flattened club data for easier analysis."""

    # Club basic info
    tag: str = Field(description="Club tag")
    name: str = Field(description="Club name")
    description: Optional[str] = Field(default=None, description="Club description")
    type: str = Field(description="Club type")
    badge_id: int = Field(description="Club badge ID")
    required_trophies: int = Field(description="Required trophies to join")
    trophies: int = Field(description="Club total trophies")
    member_count: int = Field(description="Number of members")

    # Analysis fields
    extracted_at: str = Field(description="Data extraction timestamp")
