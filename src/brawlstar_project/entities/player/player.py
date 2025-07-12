import re
from pathlib import Path
from typing import Optional

import polars as pl

from brawlstar_project.entities.tag_entity import TagEntity


class Player(TagEntity):
    def validate_tag(self, tag_body: str) -> None:
        if len(tag_body) != 8:
            raise ValueError("Player tag must be exactly 8 characters after '#'")
        if not re.fullmatch(r"[A-Z0-9]{8}", tag_body):
            raise ValueError(
                "Player tag must contain only uppercase letters A-Z and digits 0-9"
            )

    def load_player_data(self, base_dir: Path, days: int = 1) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "player.parquet", days)

    def load_battlelog_data(
        self, base_dir: Path, days: int = 1
    ) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "battlelog.parquet", days)
