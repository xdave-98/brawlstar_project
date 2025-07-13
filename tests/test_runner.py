"""
Tests for the Runner DesignFactory pattern in Brawl Stars data processing.
"""

from unittest.mock import MagicMock, patch

import pytest

from brawlstar_project.processing.utils.runner import (
    ClubRunner,
    ClubWithMembersRunner,
    PlayerRunner,
    get_runner,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.get_player.return_value = {"mock": "player_data"}
    client.get_battlelog.return_value = {"mock": "battlelog_data"}
    return client


@patch("brawlstar_project.processing.utils.runner.save_player_data_partitioned")
@patch("brawlstar_project.processing.utils.runner.save_battlelog_data_partitioned")
def test_player_runner(mock_save_battlelog, mock_save_player, mock_client):
    runner = PlayerRunner()
    result = runner.run(mock_client, "#ABCDEFGH")

    assert result["status"] == "success"
    mock_client.get_player.assert_called_once()
    mock_client.get_battlelog.assert_called_once()
    mock_save_player.assert_called_once()
    mock_save_battlelog.assert_called_once()


@patch("brawlstar_project.processing.utils.runner.fetch_club_members_data")
@patch("brawlstar_project.processing.utils.runner.fetch_club_data")
def test_club_runner(mock_fetch_club, mock_fetch_members, mock_client):
    mock_fetch_members.return_value = {"items": [{}]}

    runner = ClubRunner()
    result = runner.run(mock_client, "#CLUBTAG")

    assert result["status"] == "success"
    mock_fetch_club.assert_called_once()
    mock_fetch_members.assert_called_once()


@patch("time.sleep", return_value=None)  # Pour accélérer les tests
@patch("brawlstar_project.processing.utils.runner.save_battlelog_data_partitioned")
@patch("brawlstar_project.processing.utils.runner.save_player_data_partitioned")
@patch("brawlstar_project.processing.utils.runner.fetch_club_members_data")
@patch("brawlstar_project.processing.utils.runner.fetch_club_data")
def test_club_with_members_runner(
    mock_fetch_club,    
    mock_fetch_members, 
    mock_save_player,   
    mock_save_battlelog,
    mock_sleep,         
    mock_client
):
    mock_fetch_members.return_value = {
        "items": [
            {"tag": "#ABCDEFGH"},
            {"tag": "#IJKLMNOP"}
        ]
    }

    runner = ClubWithMembersRunner()
    result = runner.run(mock_client, "#CLUBTAG", delay=0)

    assert result["status"] == "success"
    assert result["total"] == 2
    assert result["successful"] + result["failed"] == 2
    mock_fetch_club.assert_called_once()
    mock_fetch_members.assert_called_once()
    assert mock_save_player.call_count == 2
    assert mock_save_battlelog.call_count == 2


def test_factory_valid_modes():
    assert isinstance(get_runner("player"), PlayerRunner)
    assert isinstance(get_runner("club"), ClubRunner)
    assert isinstance(get_runner("club-players"), ClubWithMembersRunner)


def test_factory_invalid_mode():
    with pytest.raises(ValueError):
        get_runner("invalid-mode")
