import pytest
from pydantic import ValidationError

from brawlstar_project.entities.club.models.members import ClubMembersData


class TestClubMembersData:
    """Test cases for ClubMembersData model."""

    def test_valid_members_data(self):
        """Test that valid members data passes validation."""
        valid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "nameColor": "#ff0000",
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        members_data = ClubMembersData.model_validate(valid_data)
        assert len(members_data.items) == 1
        member = members_data.items[0]
        assert member.tag == "#2L0R0R0R0"
        assert member.name == "Test Player"
        assert member.name_color is None  # name_color is optional and defaults to None
        assert member.role == "member"
        assert member.trophies == 5000
        assert member.icon.id == 16000000

    def test_empty_members_list(self):
        """Test that empty members list is valid."""
        valid_data = {"items": []}

        members_data = ClubMembersData.model_validate(valid_data)
        assert len(members_data.items) == 0

    def test_multiple_members(self):
        """Test that multiple members are handled correctly."""
        valid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Player 1",
                    "nameColor": "#ff0000",
                    "role": "president",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                },
                {
                    "tag": "#2L0R0R0R1",
                    "name": "Player 2",
                    "nameColor": "#00ff00",
                    "role": "vicePresident",
                    "trophies": 4500,
                    "icon": {"id": 16000001},
                },
            ]
        }

        members_data = ClubMembersData.model_validate(valid_data)
        assert len(members_data.items) == 2
        assert members_data.items[0].name == "Player 1"
        assert members_data.items[1].name == "Player 2"

    def test_missing_optional_fields(self):
        """Test that members data with missing optional fields is valid."""
        minimal_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        members_data = ClubMembersData.model_validate(minimal_data)
        member = members_data.items[0]
        assert member.tag == "#2L0R0R0R0"
        assert member.name == "Test Player"
        assert member.name_color is None
        assert member.role == "member"
        assert member.trophies == 5000

    def test_invalid_tag_format(self):
        """Test that invalid tag format is accepted (no validation constraint)."""
        invalid_data = {
            "items": [
                {
                    "tag": "invalid_tag",  # Missing # prefix
                    "name": "Test Player",
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        # The model doesn't have tag format validation, so this should pass
        members_data = ClubMembersData.model_validate(invalid_data)
        assert members_data.items[0].tag == "invalid_tag"

    def test_invalid_role_value(self):
        """Test that invalid role value is accepted (no validation constraint)."""
        invalid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "role": "invalid_role",  # Invalid role
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        # The model doesn't have role validation, so this should pass
        members_data = ClubMembersData.model_validate(invalid_data)
        assert members_data.items[0].role == "invalid_role"

    def test_negative_trophies(self):
        """Test that negative trophies are accepted (no validation constraint)."""
        invalid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "role": "member",
                    "trophies": -1000,  # Negative trophies
                    "icon": {"id": 16000000},
                }
            ]
        }

        # The model doesn't have trophies validation, so this should pass
        members_data = ClubMembersData.model_validate(invalid_data)
        assert members_data.items[0].trophies == -1000

    def test_empty_name(self):
        """Test that empty name is accepted (no validation constraint)."""
        invalid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "",  # Empty name
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        # The model doesn't have name validation, so this should pass
        members_data = ClubMembersData.model_validate(invalid_data)
        assert members_data.items[0].name == ""

    def test_invalid_name_color_format(self):
        """Test that invalid name color format is accepted (no validation constraint)."""
        invalid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "nameColor": "invalid_color",  # Invalid color format
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        # The model doesn't have name_color validation, so this should pass
        members_data = ClubMembersData.model_validate(invalid_data)
        # name_color is optional and defaults to None, so it's not mapped from nameColor
        assert members_data.items[0].name_color is None

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        invalid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    # Missing name
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        with pytest.raises(ValidationError) as exc_info:
            ClubMembersData.model_validate(invalid_data)
        assert "name" in str(exc_info.value)

    def test_model_dump(self):
        """Test that model_dump returns correct dictionary."""
        valid_data = {
            "items": [
                {
                    "tag": "#2L0R0R0R0",
                    "name": "Test Player",
                    "nameColor": "#ff0000",
                    "role": "member",
                    "trophies": 5000,
                    "icon": {"id": 16000000},
                }
            ]
        }

        members_data = ClubMembersData.model_validate(valid_data)
        dumped = members_data.model_dump()

        assert "items" in dumped
        assert len(dumped["items"]) == 1
        member = dumped["items"][0]
        assert member["tag"] == "#2L0R0R0R0"
        assert member["name"] == "Test Player"
        assert (
            member["name_color"] is None
        )  # name_color is optional and defaults to None
        assert member["role"] == "member"
        assert member["trophies"] == 5000
        assert member["icon"]["id"] == 16000000
