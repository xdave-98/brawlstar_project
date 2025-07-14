"""
This script processes the gold layer (cleaned stage) for a given date.
- Use --date to select the date partition (default: today).
- Intended for batch, Airflow, or ad-hoc runs.
- For full pipeline, use unified_main.py.
"""

import argparse
import logging
from typing import Optional

from brawlstar_project.processing.cleaned import (
    DimClubsProcessor,
    DimGameModesProcessor,
    DimMapsProcessor,
    DimPlayersProcessor,
    FactMatchesProcessor,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def process_gold_layer(date: Optional[str] = None):
    """Process complete gold layer (fact + all dimensions)."""
    logger.info(f"Processing complete gold layer for date: {date or 'today'}")

    # Process fact table first
    logger.info("Processing fact_matches table...")
    fact_processor = FactMatchesProcessor(date)
    fact_processor.process()

    # Then process all dimensions
    logger.info("Processing dimension tables...")
    dimension_processors = [
        DimPlayersProcessor(date),
        DimClubsProcessor(date),
        DimGameModesProcessor(date),
        DimMapsProcessor(date),
    ]

    for processor in dimension_processors:
        try:
            logger.info(f"Processing {processor.get_dimension_name()}...")
            processor.process()
        except Exception as e:
            logger.error(f"Error processing {processor.get_dimension_name()}: {e}")

    logger.info("Gold layer processing complete!")


def main():
    parser = argparse.ArgumentParser(description="Gold layer processing pipeline")
    parser.add_argument(
        "--date",
        help="Date partition to process (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()

    process_gold_layer(args.date)


if __name__ == "__main__":
    main()
