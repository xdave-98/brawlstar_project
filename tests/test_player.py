import pytest

from brawlstar_project.entities.player import Player


def test_tag_auto_hash():
    p = Player("ABCDEFGH")
    assert p.tag == "#ABCDEFGH"


def test_tag_already_has_hash():
    p = Player("#ABCDEFGH")
    assert p.tag == "#ABCDEFGH"


def test_tag_invalid_length_too_short():
    with pytest.raises(ValueError, match="between 7-9 characters"):
        Player("#A1")


def test_tag_invalid_length_too_long():
    with pytest.raises(ValueError, match="between 7-9 characters"):
        Player("#ABCDEFGHIJKL")


def test_tag_invalid_chars():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Player("#ABC12*GH")


def test_tag_9_characters():
    """Test that 9-character tags are now accepted."""
    p = Player("ABCDEFGHI")
    assert p.tag == "#ABCDEFGHI"


def test_tag_7_characters():
    """Test that 7-character tags are now accepted."""
    p = Player("ABCDEFG")
    assert p.tag == "#ABCDEFG"


def test_tag_8_characters():
    """Test that 8-character tags are still accepted."""
    p = Player("ABCDEFGH")
    assert p.tag == "#ABCDEFGH"


def test_formatted_tag():
    p = Player("ABCDEFGH")
    assert p.formatted_tag == "%23ABCDEFGH"
