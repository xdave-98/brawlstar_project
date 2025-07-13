import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)


class BaseDimensionProcessor(ABC):
    """
    Abstract base class for dimension table processors.

    All dimension processors should inherit from this class and implement
    the abstract methods to ensure consistent behavior.
    """

    def __init__(self, date: Optional[str] = None):
        self.date = date or datetime.today().strftime("%Y-%m-%d")
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_source_path(self) -> Path:
        """
        Get the path to the source data file for this dimension.

        Returns:
            Path to the source data file
        """
        pass

    @abstractmethod
    def get_output_path(self) -> Path:
        """
        Get the output path for this dimension table.

        Returns:
            Path where the dimension table should be saved
        """
        pass

    @abstractmethod
    def build_dimension(self, source_df: pl.DataFrame) -> pl.DataFrame:
        """
        Build the dimension table from source data.

        Args:
            source_df: Source DataFrame loaded from processed data

        Returns:
            DataFrame with dimension table data
        """
        pass

    @abstractmethod
    def get_dimension_name(self) -> str:
        """
        Get the name of this dimension (for logging and file naming).

        Returns:
            Dimension name (e.g., "dim_players", "dim_clubs")
        """
        pass

    def load_source_data(self) -> pl.DataFrame:
        """
        Load source data from processed files.

        Returns:
            Source DataFrame
        """
        source_path = self.get_source_path()

        if not source_path.exists():
            self.logger.warning(f"Source data not found: {source_path}")
            return pl.DataFrame()

        self.logger.info(f"Loading source data from {source_path}")
        return pl.read_parquet(source_path)

    def save_dimension(self, dim_df: pl.DataFrame):
        """
        Save dimension DataFrame to cleaned data directory.

        Args:
            dim_df: Dimension DataFrame
        """
        if dim_df.is_empty():
            self.logger.warning(f"No {self.get_dimension_name()} data to save")
            return

        output_path = self.get_output_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Saving {self.get_dimension_name()} to {output_path}")
        dim_df.write_parquet(str(output_path))
        self.logger.info(f"{self.get_dimension_name()} saved successfully")

    def process(self):
        """
        Complete pipeline to build and save dimension table.
        """
        self.logger.info(
            f"Processing {self.get_dimension_name()} for date: {self.date}"
        )

        # Load source data
        source_df = self.load_source_data()

        if source_df.is_empty():
            self.logger.warning(
                f"No source data available for {self.get_dimension_name()}"
            )
            return

        # Build the dimension table
        dim_df = self.build_dimension(source_df)

        # Save to cleaned data
        self.save_dimension(dim_df)

        self.logger.info(f"{self.get_dimension_name()} processing complete")
