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
        return df.drop(columns_to_drop)

    @staticmethod
    def process_battlelog_df(df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and process battlelog DataFrame for silver layer.
        """
        return (
            df.filter(
                ((df["battle_type"] != "friendly") & (df["battle_result"] != "unknown"))
            )
            .with_columns(
                pl.col("battle_time").str.strptime(
                    pl.Datetime, format="%Y%m%dT%H%M%S.%3fZ", strict=False
                ).alias("battle_time")
            )
            .rename({"event_map": "map_name"})
        )
