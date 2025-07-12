import pytest

from brawlstar_project.entities.club import Club


def test_tag_auto_hash():
    c = Club("ABCDEFGH")
    assert c.tag == "#ABCDEFGH"


def test_tag_already_has_hash():
    c = Club("#ABCDEFGH")
    assert c.tag == "#ABCDEFGH"


def test_tag_valid_chars():
    c = Club("ABC123")
    assert c.tag == "#ABC123"


def test_tag_invalid_chars():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Club("#ABC12*GH")


def test_tag_with_lowercase():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Club("#abc123")


def test_formatted_tag():
    c = Club("ABCDEFGH")
    assert c.formatted_tag == "%23ABCDEFGH"


def test_empty_tag():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Club("")


def test_tag_with_special_chars():
    with pytest.raises(ValueError, match="only uppercase letters A-Z and digits"):
        Club("#ABC@123")
