import re
from pathlib import Path

import polars as pl

from brawlstar_project.entities.tag_entity import TagEntity


class Player(TagEntity):
    def validate_tag(self, tag_body: str) -> None:
        if len(tag_body) < 7 or len(tag_body) > 9:
            raise ValueError("Player tag must be between 7-9 characters after '#'")
        if not re.fullmatch(r"[A-Z0-9]+", tag_body):
            raise ValueError(
                "Player tag must contain only uppercase letters A-Z and digits 0-9"
            )

    def load_player_data(self, base_dir: Path, days: int = 1) -> pl.DataFrame:
        return self._load_past_days_data(base_dir, "player.parquet", days)

    def load_battlelog_data(self, base_dir: Path, days: int = 1) -> pl.DataFrame:
        return self._load_past_days_data(base_dir, "battlelog.parquet", days)

    @staticmethod
    def process_player_df(df: pl.DataFrame) -> pl.DataFrame:
        """Clean and process player DataFrame for silver layer."""
        columns_to_drop = [
            "name_color",
            "best_robo_rumble_time",
            "best_time_as_big_brawler",
        ]
        df = df.drop(columns_to_drop)
        return df

    @staticmethod
    def process_battlelog_df(df: pl.DataFrame) -> pl.DataFrame:
        """Clean and process battlelog DataFrame for silver layer."""
        # Example: Remove friendlies, drop null events, etc.
        # df = df.filter(df["battle_type"] != "friendly")
        # Add more battlelog cleaning steps here
        return df
