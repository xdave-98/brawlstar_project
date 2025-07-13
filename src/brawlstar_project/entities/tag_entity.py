import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

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
    def _date_range(days: int) -> Iterator[str]:
        today = datetime.today()
        for i in range(days):
            yield (today - timedelta(days=i)).strftime("%Y-%m-%d")

    @staticmethod
    def _build_parquet_path(base_dir: Path, filename: str, date_str: str) -> Path:
        # Determine data type from filename
        if filename in ["player.parquet", "battlelog.parquet"]:
            data_type = "player"
        elif filename in ["club.parquet", "club_members.parquet"]:
            data_type = "club"
        else:
            raise ValueError(f"Unknown filename: {filename}")
        
        return base_dir / data_type / date_str / filename

    @staticmethod
    def _read_parquet(path: Path) -> pl.DataFrame:
        return pl.read_parquet(path)

    def _filter_or_annotate(self, df: pl.DataFrame) -> pl.DataFrame:
        if "tag" in df.columns:
            return df.filter(pl.col("tag") == self.tag)
        else:
            return df.with_columns(pl.lit(self.tag).alias("entity_tag"))

    @staticmethod
    def _add_date_column(df: pl.DataFrame, date_str: str) -> pl.DataFrame:
        return df.with_columns(pl.lit(date_str).alias("date"))

    def _load_single_day(
        self, base_dir: Path, filename: str, date_str: str, filter_by_tag: bool = True
    ) -> pl.DataFrame | None:
        parquet_path = self._build_parquet_path(base_dir, filename, date_str)

        if not parquet_path.exists():
            logging.warning(f"No file found for date {date_str}: {parquet_path}")
            return None

        df = self._read_parquet(parquet_path)

        if filter_by_tag:
            df = self._filter_or_annotate(df)

        df = self._add_date_column(df, date_str)
        return df

    def _load_past_days_data(
        self, base_dir: Path, filename: str, days: int, filter_by_tag: bool = True
    ) -> pl.DataFrame:
        dfs = []
        for date_str in self._date_range(days):
            df = self._load_single_day(base_dir, filename, date_str, filter_by_tag)
            if df is not None and df.height > 0:
                dfs.append(df)

        if dfs:
            return pl.concat(dfs)
        else:
            logging.info("No data found for the given period.")
            return pl.DataFrame()
