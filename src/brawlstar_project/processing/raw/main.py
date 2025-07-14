"""
This script converts all ingested JSON data to Parquet files (raw layer).
- Processes all available data in data/ingested/.
- Intended for batch or Airflow orchestration.
- For full pipeline, use unified_main.py.
"""

from brawlstar_project.processing.utils import (
    convert_all_json_to_parquet_partitioned,
)


def main():
    convert_all_json_to_parquet_partitioned()


if __name__ == "__main__":
    main()
