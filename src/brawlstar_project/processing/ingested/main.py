"""
This script runs the ingestion pipeline for all player and club tags found in config/config.yaml.
For batch ingestion, use this script. For full pipeline, use unified_main.py.
"""

import argparse
import logging

from brawlstar_project.processing.factory.runner_factory import RunnerFactory
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.utils.config_utils import (
    load_pipeline_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main function to orchestrate ingestion pipeline for all tags in config."""
    parser = argparse.ArgumentParser(
        description="Brawl Stars data ingestion pipeline for all tags in config."
    )
    parser.add_argument(
        "--mode",
        choices=["player", "club", "club-players"],
        required=True,
        help="Ingestion mode: player (all players), club (all clubs), club-players (all clubs + all members)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay (in seconds) between API calls for club-players mode",
    )

    args = parser.parse_args()

    try:
        config = load_pipeline_config()
    except FileNotFoundError as e:
        logger.error(str(e))
        return
    player_tags = config.get("default_player_tags", [])
    club_tags = config.get("default_club_tags", [])

    config_env = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config_env.api_key, base_url=config_env.base_url)

    factory: RunnerFactory = RunnerFactory()  # type: ignore
    runner = factory.get_runner(args.mode)

    if args.mode == "player":
        for tag in player_tags:
            logger.info(f"\n🚀 Starting player ingestion for {tag}")
            result = runner.run(client, tag, args.delay)
            logger.info("\n📊 Result:")
            logger.info(result)
    elif args.mode == "club":
        for tag in club_tags:
            logger.info(f"\n🚀 Starting club ingestion for {tag}")
            result = runner.run(client, tag, args.delay)
            logger.info("\n📊 Result:")
            logger.info(result)
    elif args.mode == "club-players":
        for tag in club_tags:
            logger.info(f"\n🚀 Starting club+members ingestion for {tag}")
            result = runner.run(client, tag, args.delay)
            logger.info("\n📊 Result:")
            logger.info(result)


if __name__ == "__main__":
    main()
