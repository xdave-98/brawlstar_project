
from brawlstar_project.entities.club.models.club import ClubData


class TestClubData:
    """Test cases for ClubData model."""

    def test_valid_club_data(self):
        """Test that valid club data passes validation."""
        valid_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "description": "A test club",
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        club_data = ClubData.model_validate(valid_data)
        assert club_data.tag == "#2L0R0R0R0"
        assert club_data.name == "Test Club"
        assert club_data.description == "A test club"
        assert club_data.type == "open"
        assert club_data.badge_id == 16000000
        assert club_data.required_trophies == 1000
        assert club_data.trophies == 50000
        assert club_data.members == []

    def test_missing_optional_fields(self):
        """Test that club data with missing optional fields is valid."""
        minimal_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        club_data = ClubData.model_validate(minimal_data)
        assert club_data.tag == "#2L0R0R0R0"
        assert club_data.name == "Test Club"
        assert club_data.description is None

    def test_invalid_tag_format(self):
        """Test that invalid tag format is accepted (no validation constraint)."""
        invalid_data = {
            "tag": "invalid_tag",  # Missing # prefix
            "name": "Test Club",
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        # The model doesn't have tag format validation, so this should pass
        club_data = ClubData.model_validate(invalid_data)
        assert club_data.tag == "invalid_tag"

    def test_invalid_type_value(self):
        """Test that invalid type value is accepted (no validation constraint)."""
        invalid_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "type": "invalid_type",  # Invalid type
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        # The model doesn't have type validation, so this should pass
        club_data = ClubData.model_validate(invalid_data)
        assert club_data.type == "invalid_type"

    def test_negative_trophies(self):
        """Test that negative trophies are accepted (no validation constraint)."""
        invalid_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": -1000,  # Negative trophies
            "members": [],
        }

        # The model doesn't have trophies validation, so this should pass
        club_data = ClubData.model_validate(invalid_data)
        assert club_data.trophies == -1000

    def test_empty_name(self):
        """Test that empty name is accepted (no validation constraint)."""
        invalid_data = {
            "tag": "#2L0R0R0R0",
            "name": "",  # Empty name
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        # The model doesn't have name validation, so this should pass
        club_data = ClubData.model_validate(invalid_data)
        assert club_data.name == ""

    def test_long_description(self):
        """Test that very long description is handled correctly."""
        long_description = "A" * 1000  # Very long description
        valid_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "description": long_description,
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        club_data = ClubData.model_validate(valid_data)
        assert club_data.description == long_description

    def test_model_dump(self):
        """Test that model_dump returns correct dictionary."""
        valid_data = {
            "tag": "#2L0R0R0R0",
            "name": "Test Club",
            "description": "A test club",
            "type": "open",
            "badgeId": 16000000,
            "requiredTrophies": 1000,
            "trophies": 50000,
            "members": [],
        }

        club_data = ClubData.model_validate(valid_data)
        dumped = club_data.model_dump()

        assert dumped["tag"] == "#2L0R0R0R0"
        assert dumped["name"] == "Test Club"
        assert dumped["description"] == "A test club"
        assert dumped["type"] == "open"
        assert dumped["badge_id"] == 16000000
        assert dumped["required_trophies"] == 1000
        assert dumped["trophies"] == 50000
        assert dumped["members"] == []
