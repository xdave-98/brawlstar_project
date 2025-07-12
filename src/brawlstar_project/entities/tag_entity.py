from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl


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

    def _load_data(
        self, base_dir: Path, filename: str, days: int
    ) -> Optional[pl.DataFrame]:
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
