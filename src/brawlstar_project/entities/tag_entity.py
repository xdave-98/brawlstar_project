import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)


@dataclass
class TagEntity(ABC):
    tag: str

    def __post_init__(self):
        if not self.tag.startswith("#"):
            self.tag = "#" + self.tag

        tag_body = self.tag[1:]
        self.validate_tag(tag_body)

    @abstractmethod
    def validate_tag(self, tag_body: str) -> None:
        """Validate the tag according to the entity's rules."""
        pass

    @property
    def formatted_tag(self) -> str:
        return self.tag.replace("#", "%23")

    @staticmethod
    def _date_range(days: int):
        today = datetime.today()
        for i in range(days):
            yield (today - timedelta(days=i)).strftime("%Y-%m-%d")

    def _load_single_day(
        self, base_dir: Path, filename: str, date_str: str
    ) -> pl.DataFrame | None:
        parquet_path = base_dir / date_str / filename
        if not parquet_path.exists():
            logging.warning(f"No file found for date {date_str}: {parquet_path}")
            return None

        df = pl.read_parquet(parquet_path)

        if "tag" in df.columns:
            # Player data: filter by tag
            df = df.filter(pl.col("tag") == self.tag)
        else:
            # Battlelog data: add player tag column
            df = df.with_columns(pl.lit(self.tag).alias("player_battlelog_tag"))

        df = df.with_columns(pl.lit(date_str).alias("date"))
        return df

    def _load_data(self, base_dir: Path, filename: str, days: int) -> pl.DataFrame:
        dfs = []
        for date_str in self._date_range(days):
            df = self._load_single_day(base_dir, filename, date_str)
            if df is not None and df.height > 0:
                dfs.append(df)

        if dfs:
            return pl.concat(dfs)
        else:
            logging.info("No data found for the given period.")
            return pl.DataFrame()
