import pytest
from brawlstar_project.player.player import Player


def test_tag_auto_hash():
    p = Player("ABCDEFGH")
    assert p.tag == "#ABCDEFGH"


def test_tag_already_has_hash():
    p = Player("#ABCDEFGH")
    assert p.tag == "#ABCDEFGH"


def test_tag_invalid_length_too_short():
    with pytest.raises(ValueError, match="exactly 8 characters"):
        Player("#A1")


def test_tag_invalid_length_too_long():
    with pytest.raises(ValueError, match="exactly 8 characters"):
        Player("#ABCDEFGHIJKL")


def test_tag_invalid_chars():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Player("#ABC12*GH")


def test_formatted_tag():
    p = Player("ABCDEFGH")
    assert p.formatted_tag == "%23ABCDEFGH"
