import logging
from pathlib import Path

import polars as pl

from .base_dimension_processor import BaseDimensionProcessor

logger = logging.getLogger(__name__)


class DimClubsProcessor(BaseDimensionProcessor):
    """
    Processor for building dim_clubs dimension table from processed data.
    """

    def get_source_path(self) -> Path:
        """Get the path to processed club data."""
        processed_base = Path("data/processed")
        return processed_base / "club" / self.date / "club.parquet"

    def get_output_path(self) -> Path:
        """Get the output path for dim_clubs."""
        cleaned_base = Path("data/cleaned")
        return cleaned_base / "dim_clubs.parquet"

    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """Build dim_clubs table from club data."""
        self.logger.info("Building dim_clubs table...")

        OUTPUT_DIM_CLUB_COLS = [
            "tag",
            "name",
            "description",
            "trophies",
            "required_trophies",
            "member_count",
        ]

        dim_clubs_df = (
            source_df.select(OUTPUT_DIM_CLUB_COLS).unique(
                subset=["tag"]
            )  # Ensure one row per club
        )

        # Add _process_date column as a date type
        dim_clubs_df = dim_clubs_df.with_columns(
            pl.lit(self.date).str.strptime(pl.Date, "%Y-%m-%d").alias("_process_date")
        )

        self.logger.info(f"Built dim_clubs table with {len(dim_clubs_df)} rows")
        return dim_clubs_df

    def get_dimension_name(self) -> str:
        """Get the dimension name."""
        return "dim_clubs"


# Convenience function for backward compatibility
def process_dim_clubs(date=None):
    """
    Convenience function to process dim_clubs using the processor.

    Args:
        date: Date partition to process (YYYY-MM-DD). Defaults to today.
    """
    processor = DimClubsProcessor(date)
    processor.process()
