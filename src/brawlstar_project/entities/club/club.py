import re
from pathlib import Path
from typing import Optional

import polars as pl

from brawlstar_project.entities.tag_entity import TagEntity


class Club(TagEntity):
    def validate_tag(self, tag_body: str) -> None:
        if not re.fullmatch(r"[A-Z0-9]+", tag_body):
            raise ValueError(
                "Club tag must contain only uppercase letters A-Z and digits 0-9"
            )

    def load_club_data(self, base_dir: Path, days: int = 1) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "club.parquet", days)

    def load_club_members_data(
        self, base_dir: Path, days: int = 1
    ) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "club_members.parquet", days)
