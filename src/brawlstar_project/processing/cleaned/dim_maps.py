import logging
from pathlib import Path

import polars as pl

from .base_dimension_processor import BaseDimensionProcessor

logger = logging.getLogger(__name__)


class DimMapsProcessor(BaseDimensionProcessor):
    """
    Processor for building dim_maps dimension table from processed battlelog data.
    """

    def get_source_path(self) -> Path:
        """Get the path to processed battlelog data."""
        processed_base = Path("data/processed")
        return processed_base / "player" / self.date / "battlelog.parquet"

    def get_output_path(self) -> Path:
        """Get the output path for dim_maps."""
        cleaned_base = Path("data/cleaned")
        return cleaned_base / "dim_maps" / self.date / "dim_maps.parquet"

    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """Build dim_maps table by extracting unique maps."""
        self.logger.info("Building dim_maps table...")
        dim_maps_df = source_df.select("map_name").unique(subset=["map_name"])

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
