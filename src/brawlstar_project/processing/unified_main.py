"""
Unified main entry point for all pipeline stages.

This module provides a single entry point that can handle
ingestion, analysis, and other pipeline stages using the factory pattern.
"""

import argparse
import logging

from brawlstar_project.processing.factory.analysis_factory import AnalysisFactory
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.ingested.factory.runner_factory import RunnerFactory

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Unified main function for all pipeline stages."""
    parser = argparse.ArgumentParser(
        description="Brawl Stars data pipeline - unified entry point"
    )

    # Pipeline stage
    parser.add_argument(
        "--stage",
        choices=["ingestion", "analysis"],
        required=True,
        help="Pipeline stage to run",
    )

    # Mode (for both stages)
    parser.add_argument("--mode", required=True, help="Mode to run (depends on stage)")

    # Common arguments
    parser.add_argument("--tag", help="Player or Club tag")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to load")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between API calls"
    )

    args = parser.parse_args()

    if args.stage == "ingestion":
        if not args.tag:
            logger.error("❌ --tag is required for ingestion stage")
            return
        logger.info(f"🚀 Starting {args.mode} ingestion...")
        # Load config and create client
        config = ConfigLoader.from_env()
        client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)
        try:
            factory = RunnerFactory()
            runner = factory.get_runner(args.mode)
            result = runner.run(client=client, tag=args.tag, delay=args.delay)
            logger.info(f"\n📊 Result: {result}")
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            logger.info(f"Available modes: {factory.list_modes()}")
            return
    elif args.stage == "analysis":
        run_analysis(args)


def run_analysis(args):
    """Run analysis stage."""
    logger.info(f"📊 Starting {args.mode} analysis...")

    # Get runner from factory
    try:
        factory = AnalysisFactory()
        runner = factory.get_runner(args.mode)

        # Prepare kwargs based on mode
        kwargs = {"data_dir": args.data_dir, "days": args.days}

        if args.mode == "single-player":
            if not args.tag:
                logger.error("❌ --tag is required for single-player analysis")
                return
            kwargs["player_tag"] = args.tag
        elif args.mode == "club":
            if not args.tag:
                logger.error("❌ --tag is required for club analysis")
                return
            kwargs["club_tag"] = args.tag

        result = runner.run(**kwargs)
        logger.info(f"\n📊 Result: {result}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.info(f"Available modes: {factory.list_modes()}")
        return


if __name__ == "__main__":
    main()
