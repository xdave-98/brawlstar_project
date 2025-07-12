"""
Pydantic models for Brawl Stars club members data.
"""

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
