from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Optional
import polars as pl


@dataclass
class Player:
    tag: str

    def __post_init__(self):
        if not self.tag.startswith("#"):
            self.tag = "#" + self.tag

        player_tag = self.tag[1:]

        # Check if player tag contains 8 caracters
        if len(player_tag) != 8:
            raise ValueError("Player tag must be exactly 8 characters after '#'")

        # Player tag should only contains letters and numbers
        if not re.fullmatch(r"[A-Z0-9]{8}", player_tag):
            raise ValueError(
                "Player tag must contain only uppercase letters A-Z and digits 0-9"
            )

    @property
    def formatted_tag(self) -> str:
        return self.tag.replace("#", "%23")

    def _load_data(self, base_dir: Path, filename: str, days: int) -> Optional[pl.DataFrame]:
        dfs = []
        today = datetime.today()
        for i in range(days):
            day = today - timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            parquet_path = base_dir / self.tag / date_str / filename
            if parquet_path.exists():
                df = pl.read_parquet(parquet_path)
                df = df.with_columns(pl.lit(date_str).alias("date"))
                dfs.append(df)
        return pl.concat(dfs) if dfs else None

    def load_player_data(self, base_dir: Path, days: int = 1) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "player.parquet", days)

    def load_battlelog_data(self, base_dir: Path, days: int = 1) -> Optional[pl.DataFrame]:
        return self._load_data(base_dir, "battlelog.parquet", days)
