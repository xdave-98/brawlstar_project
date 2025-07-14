import logging
from pathlib import Path

import polars as pl

from brawlstar_project.constants.paths import DATA_CLEANED_DIR, DATA_PROCESSED_DIR

from .base_dimension_processor import BaseDimensionProcessor

logger = logging.getLogger(__name__)


class DimGameModesProcessor(BaseDimensionProcessor):
    """
    Processor for building dim_game_modes dimension table from processed battlelog data.
    """

    def get_source_path(self) -> Path:
        """Get the path to processed battlelog data."""
        return DATA_PROCESSED_DIR / "player" / self.date / "battlelog.parquet"

    def get_output_path(self) -> Path:
        """Get the output path for dim_game_modes."""
        return DATA_CLEANED_DIR / "dim_game_modes" / self.date / "dim_game_modes.parquet"

    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """Build dim_game_modes table by extracting unique game modes."""
        self.logger.info("Building dim_game_modes table...")
        dim_game_modes_df = source_df.select("battle_mode").unique(
            subset=["battle_mode"]
        )

        self.logger.info(
            f"Built dim_game_modes table with {len(dim_game_modes_df)} rows"
        )
        return dim_game_modes_df

    def get_dimension_name(self) -> str:
        """Get the dimension name."""
        return "dim_game_modes"


# Convenience function for backward compatibility
def process_dim_game_modes(date=None):
    """
    Convenience function to process dim_game_modes using the processor.

    Args:
        date: Date partition to process (YYYY-MM-DD). Defaults to today.
    """
    processor = DimGameModesProcessor(date)
    processor.process()
