import time
from abc import ABC, abstractmethod

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.utils import (
    fetch_club_members_data,
    save_battlelog_data_partitioned,
    save_player_data_partitioned,
)


class Processor(ABC):
    """Abstract base class for a processing mode."""

    @abstractmethod
    def process(self, client, tag: str, delay: float = 1.0) -> dict:
        """
        Run the processing logic.

        Args:
            client: BrawlStars API client
            tag: player or club tag
            delay: optional delay between calls (default: 1.0)

        Returns:
            dict: result statistics
        """
        ...


class PlayerProcessor(Processor):
    """Process a single player (player data + battlelog)."""

    def process(self, client, tag: str, delay: float = 1.0) -> dict:
        print(f"👤 Processing single player: {tag}")
        try:
            player = Player(tag)

            # Fetch player data
            print("  📊 Fetching player data…")
            player_data = client.get_player(player.formatted_tag)
            save_player_data_partitioned(player_data, player.tag)

            # Fetch battlelog data
            print("  ⚔️ Fetching battlelog data…")
            battlelog_data = client.get_battlelog(player.formatted_tag)
            save_battlelog_data_partitioned(battlelog_data, player.tag)

            print(f"  ✅ Completed data fetch for {tag}")
            return {"total": 1, "successful": 1, "failed": 0}

        except Exception as e:
            print(f"  ❌ Error processing player {tag}: {e}")
            return {"total": 1, "successful": 0, "failed": 1}


class ClubProcessor(Processor):
    """Process a single club (club members list)."""

    def process(self, client, tag: str, delay: float = 1.0) -> dict:
        print(f"🏛️ Processing single club: {tag}")
        try:
            club = Club(tag)

            # Fetch club members data
            print("  👥 Fetching club members data…")
            club_members_data = fetch_club_members_data(client, club)

            member_count = len(club_members_data.get("items", []))
            print(f"  ✅ Completed data fetch for {tag} ({member_count} members)")

            return {"total": 1, "successful": 1, "failed": 0}

        except Exception as e:
            print(f"  ❌ Error processing club {tag}: {e}")
            return {"total": 1, "successful": 0, "failed": 1}


class ClubWithMembersProcessor(Processor):
    """Process a club + all its members (club members, player & battlelog for each)."""

    def process(self, client, tag: str, delay: float = 1.0) -> dict:
        print(f"🏛️ Processing club with all members: {tag}")

        try:
            club = Club(tag)

            # Fetch club members
            print("  👥 Fetching club members data…")
            club_members_data = fetch_club_members_data(client, club)

            member_tags = [
                member.get("tag")
                for member in club_members_data.get("items", [])
                if member.get("tag")
            ]

            print(f"  🎯 Found {len(member_tags)} club members to process")

            successful = 0
            failed = 0

            for i, member_tag in enumerate(member_tags, 1):
                print(f"\n  👤 Processing member {i}/{len(member_tags)}: {member_tag}")
                try:
                    player = Player(member_tag)

                    # Player data
                    player_data = client.get_player(player.formatted_tag)
                    save_player_data_partitioned(player_data, player.tag)

                    # Battlelog
                    battlelog_data = client.get_battlelog(player.formatted_tag)
                    save_battlelog_data_partitioned(battlelog_data, player.tag)

                    print(f"    ✅ Completed data fetch for {member_tag}")
                    successful += 1

                    if i < len(member_tags):
                        print(f"    ⏳ Waiting {delay}s before next API call…")
                        time.sleep(delay)

                except Exception as e:
                    print(f"    ❌ Error processing {member_tag}: {e}")
                    failed += 1

            return {
                "total": len(member_tags),
                "successful": successful,
                "failed": failed,
            }

        except Exception as e:
            print(f"  ❌ Error processing club {tag}: {e}")
            return {"total": 0, "successful": 0, "failed": 0}


class ProcessorFactory:
    """Factory to obtain the appropriate Processor based on mode."""

    _registry = {
        "player": PlayerProcessor,
        "club": ClubProcessor,
        "club-players": ClubWithMembersProcessor,
    }

    @staticmethod
    def get_processor(mode: str) -> Processor:
        """
        Get a Processor instance for the given mode.

        Args:
            mode (str): The processing mode (player, club, club-players)

        Returns:
            Processor: An instance of a Processor subclass
        """
        cls = ProcessorFactory._registry.get(mode)
        if not cls:
            raise ValueError(f"Unsupported processing mode: {mode}")
        return cls()
