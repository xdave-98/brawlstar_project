import logging
from pathlib import Path

import polars as pl

from .base_dimension_processor import BaseDimensionProcessor

logger = logging.getLogger(__name__)


class DimPlayersProcessor(BaseDimensionProcessor):
    """
    Processor for building dim_players dimension table from processed data.
    """

    def get_source_path(self) -> Path:
        """Get the path to processed player data."""
        processed_base = Path("data/processed")
        return processed_base / "player" / self.date / "player.parquet"

    def get_output_path(self) -> Path:
        """Get the output path for dim_players."""
        cleaned_base = Path("data/cleaned")
        return cleaned_base / "dim_players" / self.date / "dim_players.parquet"

    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """Build dim_players table from player data."""
        self.logger.info("Building dim_players table...")
        dim_players_df = (
            source_df.select(
                [
                    "tag",
                    "name",
                    "club_tag",
                    "trophies",
                    "highest_trophies",
                    "exp_level",
                    "exp_points",
                ]
            ).unique(subset=["tag"])  # Ensure one row per player
        )

        self.logger.info(f"Built dim_players table with {len(dim_players_df)} rows")
        return dim_players_df

    def get_dimension_name(self) -> str:
        """Get the dimension name."""
        return "dim_players"


# Convenience function for backward compatibility
def process_dim_players(date=None):
    """
    Convenience function to process dim_players using the processor.

    Args:
        date: Date partition to process (YYYY-MM-DD). Defaults to today.
    """
    processor = DimPlayersProcessor(date)
    processor.process()
