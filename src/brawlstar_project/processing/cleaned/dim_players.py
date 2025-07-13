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
        """Build dim_players table from player data with club role."""
        self.logger.info("Building dim_players table...")

        INPUT_SOURCE_COLS = [
            "tag",
            "name",
            "club_tag",
            "trophies",
            "highest_trophies",
            "exp_level",
            "exp_points",
        ]

        # Define column order for dim_players
        OUTPUT_DIM_PLAYERS_COLS = [
            "tag",
            "name",
            "club_tag",
            "club_role",
            "trophies",
            "highest_trophies",
            "exp_level",
            "exp_points",
        ]

        # Load club members data to get roles
        club_members_path = (
            Path("data/processed") / "club" / self.date / "club_members.parquet"
        )
        if club_members_path.exists():
            self.logger.info(f"Loading club members data from {club_members_path}")
            club_members_df = pl.read_parquet(club_members_path)

            # Join player data with club members data to get role
            dim_players_df = (
                source_df.select(INPUT_SOURCE_COLS)
                .unique(subset=["tag"])  # Ensure one row per player
                .join(
                    club_members_df.select(["tag", "role"]),
                    left_on="tag",
                    right_on="tag",
                    how="left",  # Keep all players even if not in club members
                )
                .with_columns(pl.col("role").alias("club_role"))
                .select(OUTPUT_DIM_PLAYERS_COLS)
            )
        else:
            self.logger.warning(f"Club members data not found at {club_members_path}")
            # Fallback to original structure without role
            dim_players_df = (
                source_df.select(INPUT_SOURCE_COLS).unique(
                    subset=["tag"]
                )  # Ensure one row per player
            )
            # Add empty club_role column and reorder
            dim_players_df = dim_players_df.with_columns(
                pl.lit(None).alias("club_role")
            ).select(OUTPUT_DIM_PLAYERS_COLS)

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
