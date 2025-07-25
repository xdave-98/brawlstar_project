import logging
from pathlib import Path

import polars as pl

from brawlstar_project.constants.paths import DATA_CLEANED_DIR, DATA_PROCESSED_DIR

from .base_dimension_processor import BaseDimensionProcessor

logger = logging.getLogger(__name__)


class DimMapsProcessor(BaseDimensionProcessor):
    """
    Processor for building dim_maps dimension table from processed battlelog data.
    """

    def get_source_path(self) -> Path:
        """Get the path to processed battlelog data."""
        return DATA_PROCESSED_DIR / "player" / self.date / "battlelog.parquet"

    def get_output_path(self) -> Path:
        """Get the output path for dim_maps."""
        return DATA_CLEANED_DIR / "dim_maps.parquet"

    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """Build dim_maps table by extracting unique maps."""
        self.logger.info("Building dim_maps table...")
        dim_maps_df = source_df.select("map_name").unique(subset=["map_name"])

        # Add _process_date column as a date type
        dim_maps_df = dim_maps_df.with_columns(
            pl.lit(self.date).str.strptime(pl.Date, "%Y-%m-%d").alias("_process_date")
        )

        self.logger.info(f"Built dim_maps table with {len(dim_maps_df)} rows")
        return dim_maps_df

    def get_dimension_name(self) -> str:
        """Get the dimension name."""
        return "dim_maps"


# Convenience function for backward compatibility
def process_dim_maps(date=None):
    """
    Convenience function to process dim_maps using the processor.

    Args:
        date: Date partition to process (YYYY-MM-DD). Defaults to today.
    """
    processor = DimMapsProcessor(date)
    processor.process()
