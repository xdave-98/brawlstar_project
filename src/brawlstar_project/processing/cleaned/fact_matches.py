import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)


class FactMatchesProcessor:
    """
    Processor for building fact_matches table from processed data.
    """

    def __init__(self, date: Optional[str] = None):
        self.date = date or datetime.today().strftime("%Y-%m-%d")
        self.logger = logging.getLogger(__name__)

    def generate_match_id(
        self, player_tag: str, battle_time, map_name: str
    ) -> str:
        """
        Generate unique match ID with structure: <player_tag>-<yyyyMMdd>-<map_name>.
        
        Args:
            player_tag: Player tag (e.g., "#GQJRYV0JQ")
            battle_time: Battle timestamp (datetime or string)
            map_name: Name of the map
            
        Returns:
            Match ID in format: <player_tag>-<yyyyMMdd>-<map_name>
        """
        # Clean player tag (remove #)
        clean_player_tag = player_tag.replace("#", "")
        
        # Extract date in yyyyMMdd format
        if hasattr(battle_time, 'strftime'):
            # battle_time is a datetime object
            date_str = battle_time.strftime("%Y%m%d")
        else:
            # fallback for string - try to parse and extract date
            try:
                # Handle ISO format like "20250713T061819.000Z"
                if "T" in str(battle_time):
                    date_part = str(battle_time).split("T")[0]
                    # Convert YYYY-MM-DD to YYYYMMDD
                    date_str = date_part.replace("-", "")
                else:
                    # Try to extract date from other formats
                    date_str = str(battle_time)[:8]  # Take first 8 chars as date
            except (ValueError, IndexError, AttributeError):
                # Fallback to current date if parsing fails
                date_str = datetime.today().strftime("%Y%m%d")
        
        # Clean map name (replace spaces and hyphens with underscores)
        clean_map = map_name.replace(" ", "_").replace("-", "_")
        
        return f"{clean_player_tag}-{date_str}-{clean_map}"

    def build_fact_matches(self) -> pl.DataFrame:
        """
        Build fact_matches table from processed battlelog and player data.

        Returns:
            DataFrame with fact_matches data
        """
        # Load processed data
        processed_base = Path("data/processed")
        battlelog_path = processed_base / "player" / self.date / "battlelog.parquet"
        player_path = processed_base / "player" / self.date / "player.parquet"

        if not battlelog_path.exists():
            self.logger.warning(f"Battlelog data not found: {battlelog_path}")
            return pl.DataFrame()

        if not player_path.exists():
            self.logger.warning(f"Player data not found: {player_path}")
            return pl.DataFrame()

        self.logger.info(f"Loading battlelog data from {battlelog_path}")
        battlelog_df = pl.read_parquet(battlelog_path)

        self.logger.info(f"Loading player data from {player_path}")
        player_df = pl.read_parquet(player_path)

        # Join battlelog with player data to get club information
        self.logger.info("Joining battlelog with player data...")
        fact_df = battlelog_df.join(
            player_df.select(["tag", "club_tag"]),
            left_on="player_tag",
            right_on="tag",
            how="left",
        )

        # Generate match_id and battle_time_date, then select fact table columns
        self.logger.info("Generating match_id, battle_time_date and selecting fact table columns...")
        fact_matches_df = fact_df.with_columns(
            [
                pl.struct(["player_tag", "battle_time", "map_name"])
                .map_elements(
                    lambda x: self.generate_match_id(
                        x["player_tag"], x["battle_time"], x["map_name"]
                    )
                )
                .alias("match_id"),
                pl.col("battle_time").dt.date().alias("battle_time_date")
            ]
        ).select(
            [
                "match_id",
                "battle_time",
                "battle_time_date",
                "player_tag",
                "club_tag",
                "map_name",
                "battle_mode",
                "battle_result",
            ]
        )

        self.logger.info(f"Built fact_matches table with {len(fact_matches_df)} rows")
        return fact_matches_df

    def save_fact_matches(self, fact_df: pl.DataFrame):
        """
        Save fact_matches DataFrame to cleaned data directory.

        Args:
            fact_df: Fact matches DataFrame
        """
        if fact_df.is_empty():
            self.logger.warning("No fact_matches data to save")
            return

        cleaned_base = Path("data/cleaned")
        output_path = cleaned_base / "fact_matches" / self.date / "fact_matches.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Saving fact_matches to {output_path}")
        fact_df.write_parquet(str(output_path))
        self.logger.info("Fact matches saved successfully")

    def process(self):
        """
        Complete pipeline to build and save fact_matches table.
        """
        self.logger.info(f"Processing fact_matches for date: {self.date}")

        # Build the fact table
        fact_df = self.build_fact_matches()

        # Save to cleaned data
        self.save_fact_matches(fact_df)

        self.logger.info("Fact matches processing complete")


# Convenience function for backward compatibility
def process_fact_matches(date: Optional[str] = None):
    """
    Convenience function to process fact_matches using the processor.

    Args:
        date: Date partition to process (YYYY-MM-DD). Defaults to today.
    """
    processor = FactMatchesProcessor(date)
    processor.process()
