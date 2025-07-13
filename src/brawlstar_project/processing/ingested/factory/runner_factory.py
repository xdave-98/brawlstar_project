import logging
import time
from abc import abstractmethod

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.factory.base_factory import BaseFactory, BaseRunner
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.utils import (
    fetch_club_data,
    fetch_club_members_data,
    save_battlelog_data_partitioned,
    save_player_data_partitioned,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class IngestionRunner(BaseRunner):
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
        logger.info(f"👤 Processing single player: {tag}")
        try:
            player = Player(tag)

            logger.info("  📊 Fetching player data...")
            player_data = client.get_player(player.formatted_tag)
            save_player_data_partitioned(player_data, player.tag)

            logger.info("  ⚔️ Fetching battlelog data...")
            battlelog_data = client.get_battlelog(player.formatted_tag)
            save_battlelog_data_partitioned(battlelog_data, player.tag)

            logger.info(f"  ✅ Completed data fetch for {tag}")
            return {"status": "success"}

        except Exception as e:
            logger.error(f"  ❌ Error processing {tag}: {e}")
            return {"status": "error", "error": str(e)}


class ClubRunner(IngestionRunner):
    """
    Runner for a single club (info + members).
    """

    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        logger.info(f"🏛️ Processing single club: {tag}")
        try:
            club = Club(tag)

            logger.info("  📋 Fetching club data...")
            fetch_club_data(client, club)

            logger.info("  👥 Fetching club members data...")
            club_members_data = fetch_club_members_data(client, club)

            member_count = len(club_members_data.get("items", []))
            logger.info(f"  ✅ Completed data fetch for {tag} ({member_count} members)")

            return {"status": "success", "members": member_count}

        except Exception as e:
            logger.error(f"  ❌ Error processing {tag}: {e}")
            return {"status": "error", "error": str(e)}


class ClubWithMembersRunner(IngestionRunner):
    """
    Runner for club and all its members.
    """

    def run(self, client: BrawlStarsClient, tag: str, delay: float = 1.0) -> dict:
        logger.info(f"🏛️ Processing club with all members: {tag}")
        try:
            club = Club(tag)

            logger.info("  📋 Fetching club data...")
            fetch_club_data(client, club)

            logger.info("  👥 Fetching club members data...")
            club_members_data = fetch_club_members_data(client, club)

            member_tags = [
                member["tag"]
                for member in club_members_data.get("items", [])
                if "tag" in member
            ]
            logger.info(f"  🎯 Found {len(member_tags)} club members to process")

            successful, failed = 0, 0

            for i, member_tag in enumerate(member_tags, 1):
                logger.info(f"\n  👤 Processing member {i}/{len(member_tags)}: {member_tag}")
                try:
                    player = Player(member_tag)

                    player_data = client.get_player(player.formatted_tag)
                    save_player_data_partitioned(player_data, player.tag)

                    battlelog_data = client.get_battlelog(player.formatted_tag)
                    save_battlelog_data_partitioned(battlelog_data, player.tag)

                    logger.info(f"    ✅ Completed data fetch for {member_tag}")
                    successful += 1

                    if i < len(member_tags):
                        logger.info(f"    ⏳ Waiting {delay}s before next API call...")
                        time.sleep(delay)

                except Exception as e:
                    logger.error(f"    ❌ Error processing {member_tag}: {e}")
                    failed += 1

            return {
                "status": "success",
                "total": len(member_tags),
                "successful": successful,
                "failed": failed,
            }

        except Exception as e:
            logger.error(f"  ❌ Error processing club {tag}: {e}")
            return {"status": "error", "error": str(e)}


class RunnerFactory(BaseFactory):
    """
    Factory to obtain the appropriate IngestionRunner based on mode.
    """

    def __init__(self):
        super().__init__()
        # Register default ingestion modes
        self.register("player", PlayerRunner)
        self.register("club", ClubRunner)
        self.register("club-players", ClubWithMembersRunner)
