from brawlstar_project.processing.utils.json_utils import (
    convert_all_json_to_parquet_partitioned,
)


def main():
    convert_all_json_to_parquet_partitioned()


if __name__ == "__main__":
    main()
