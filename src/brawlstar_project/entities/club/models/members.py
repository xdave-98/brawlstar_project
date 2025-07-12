"""
Pydantic models for Brawl Stars club members data.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PlayerIcon(BaseModel):
    """Model for player icon information."""

    id: int = Field(description="Player icon ID")


class ClubMember(BaseModel):
    """Model for club member information."""

    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    name_color: Optional[str] = Field(default=None, description="Player name color")
    role: str = Field(description="Player role")
    trophies: int = Field(description="Player trophies")
    icon: PlayerIcon = Field(description="Player icon information")


class ClubMembersData(BaseModel):
    """Model for club members list from Brawl Stars API."""

    items: list[ClubMember] = Field(description="List of club members")


def create_flattened_club_members_data(
    members_data: dict,
) -> "FlattenedClubMembersData":
    """
    Create flattened club members data from raw API response.

    Args:
        members_data: Raw club members data from Brawl Stars API

    Returns:
        Flattened club members data model
    """
    # Compute missing fields
    extracted_at = datetime.now().isoformat()

    # First validate the raw data to get proper ClubMember objects
    club_members_data = ClubMembersData.model_validate(members_data)

    # Flatten each member's icon data
    flattened_items = []
    for member in club_members_data.items:
        flattened_member = {
            "tag": member.tag,
            "name": member.name,
            "name_color": member.name_color,
            "role": member.role,
            "trophies": member.trophies,
            "icon_id": member.icon.id,
            "extracted_at": extracted_at,
        }
        flattened_items.append(flattened_member)

    # Create data with computed fields
    flattened_data = {
        "items": flattened_items,
        "extracted_at": extracted_at,
    }

    return FlattenedClubMembersData.model_validate(flattened_data)


class FlattenedClubMembersData(BaseModel):
    """Flattened club members data for easier analysis."""

    # Club members list
    items: list["FlattenedClubMember"] = Field(
        description="List of flattened club members"
    )

    # Analysis fields
    extracted_at: str = Field(description="Data extraction timestamp")


class FlattenedClubMember(BaseModel):
    """Flattened club member data for easier analysis."""

    # Member basic info
    tag: str = Field(description="Player tag")
    name: str = Field(description="Player name")
    name_color: Optional[str] = Field(default=None, description="Player name color")
    role: str = Field(description="Player role")
    trophies: int = Field(description="Player trophies")
    icon_id: int = Field(description="Player icon ID")

    # Analysis fields
    extracted_at: str = Field(description="Data extraction timestamp")
