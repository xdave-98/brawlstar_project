import argparse
import logging

from brawlstar_project.processing.factory.processing_factory import ProcessingFactory

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Batch process and clean silver data for all entities."
    )
    parser.add_argument(
        "--mode",
        choices=["player", "battlelog", "club", "club_members", "all"],
        default="all",
        help="Which entity to process (default: all)",
    )
    parser.add_argument(
        "--date", help="Date partition to process (YYYY-MM-DD). Defaults to today."
    )
    args = parser.parse_args()

    logger.info(
        f"Starting processing for mode: {args.mode}, date: {args.date or 'today'}"
    )
    factory = ProcessingFactory()
    runner = factory.get_runner(args.mode)
    runner.run(date=args.date)
    logger.info("Processing complete.")


if __name__ == "__main__":
    main()
