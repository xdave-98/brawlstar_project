import time
from abc import ABC, abstractmethod

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.utils.json_utils import (
    fetch_club_data,
    fetch_club_members_data,
    save_battlelog_data_partitioned,
    save_player_data_partitioned,
)


class IngestionRunner(ABC):
    """
    Abstract base class for ingestion runners.
    """

    @abstractmethod
    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        """
        Run the ingestion process.

        Returns:
            dict: statistics or results
        """
        pass


class PlayerRunner(IngestionRunner):
    """
    Runner for a single player.
    """

    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        print(f"👤 Processing single player: {tag}")
        try:
            player = Player(tag)

            print("  📊 Fetching player data...")
            player_data = client.get_player(player.formatted_tag)
            save_player_data_partitioned(player_data, player.tag)

            print("  ⚔️ Fetching battlelog data...")
            battlelog_data = client.get_battlelog(player.formatted_tag)
            save_battlelog_data_partitioned(battlelog_data, player.tag)

            print(f"  ✅ Completed data fetch for {tag}")
            return {"status": "success"}

        except Exception as e:
            print(f"  ❌ Error processing {tag}: {e}")
            return {"status": "error", "error": str(e)}


class ClubRunner(IngestionRunner):
    """
    Runner for a single club (info + members).
    """

    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        print(f"🏛️ Processing single club: {tag}")
        try:
            club = Club(tag)

            print("  📋 Fetching club data...")
            fetch_club_data(client, club)

            print("  👥 Fetching club members data...")
            club_members_data = fetch_club_members_data(client, club)

            member_count = len(club_members_data.get("items", []))
            print(f"  ✅ Completed data fetch for {tag} ({member_count} members)")

            return {"status": "success", "members": member_count}

        except Exception as e:
            print(f"  ❌ Error processing {tag}: {e}")
            return {"status": "error", "error": str(e)}


class ClubWithMembersRunner(IngestionRunner):
    """
    Runner for club and all its members.
    """

    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        print(f"🏛️ Processing club with all members: {tag}")
        try:
            club = Club(tag)

            print("  📋 Fetching club data...")
            fetch_club_data(client, club)

            print("  👥 Fetching club members data...")
            club_members_data = fetch_club_members_data(client, club)

            member_tags = [
                member["tag"]
                for member in club_members_data.get("items", [])
                if "tag" in member
            ]
            print(f"  🎯 Found {len(member_tags)} club members to process")

            successful, failed = 0, 0

            for i, member_tag in enumerate(member_tags, 1):
                print(f"\n  👤 Processing member {i}/{len(member_tags)}: {member_tag}")
                try:
                    player = Player(member_tag)

                    player_data = client.get_player(player.formatted_tag)
                    save_player_data_partitioned(player_data, player.tag)

                    battlelog_data = client.get_battlelog(player.formatted_tag)
                    save_battlelog_data_partitioned(battlelog_data, player.tag)

                    print(f"    ✅ Completed data fetch for {member_tag}")
                    successful += 1

                    if i < len(member_tags):
                        print(f"    ⏳ Waiting {delay}s before next API call...")
                        time.sleep(delay)

                except Exception as e:
                    print(f"    ❌ Error processing {member_tag}: {e}")
                    failed += 1

            return {
                "status": "success",
                "total": len(member_tags),
                "successful": successful,
                "failed": failed,
            }

        except Exception as e:
            print(f"  ❌ Error processing club {tag}: {e}")
            return {"status": "error", "error": str(e)}


def get_runner(mode: str) -> IngestionRunner:
    """
    Factory to return the appropriate runner.

    Args:
        mode: Mode string ("player", "club", "club-players")

    Returns:
        IngestionRunner instance
    """
    runners = {
        "player": PlayerRunner,
        "club": ClubRunner,
        "club-players": ClubWithMembersRunner,
    }
    try:
        return runners[mode]()
    except KeyError:
        raise ValueError(f"Unknown mode: {mode}")
