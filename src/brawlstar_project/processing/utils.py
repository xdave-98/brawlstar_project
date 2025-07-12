import json
import os
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

import polars as pl


def save_json_data(
    data: dict,
    player_tag: str,
    base_dir: str,
    filename: str = "data.json",
    validate_func: Optional[Callable[[dict], dict]] = None,
) -> str:
    """
    Save any data dict to JSON file under player_tag/today/filename.

    Args:
        data: Raw data dict
        player_tag: Player tag identifier
        base_dir: Base directory for data storage
        filename: JSON filename (default: "data.json")
        validate_func: Optional callable to validate/transform data before saving

    Returns:
        Path to saved JSON file
    """
    if validate_func:
        data = validate_func(data)

    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = os.path.join(base_dir, player_tag, today)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, filename)

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Data saved in: {file_path}")
    return file_path


def save_json_decorator(base_dir: str = "data/ingested", filename: str = "data.json"):
    """
    Decorator to save JSON data with consistent parameters.

    Args:
        base_dir: Base directory for data storage
        filename: JSON filename

    Returns:
        Decorated function that saves data to JSON
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(data: dict, player_tag: str, **kwargs) -> str:
            # Call the validation function with both data and player_tag
            validated_data = func(data, player_tag)
            return save_json_data(
                data=validated_data,
                player_tag=player_tag,
                base_dir=base_dir,
                filename=filename,
                **kwargs,
            )

        return wrapper

    return decorator


@save_json_decorator(base_dir="data/ingested", filename="player.json")
def save_player_data(data: dict, player_tag: str) -> dict:
    """
    Validate and save player data.

    Args:
        data: Raw player data from Brawl Stars API
        player_tag: Player tag identifier

    Returns:
        Validated player data dict
    """
    from brawlstar_project.player.models.player import PlayerData

    # Validate data with Pydantic model
    player_data = PlayerData.model_validate(data)
    return player_data.model_dump()


@save_json_decorator(base_dir="data/ingested", filename="battlelog.json")
def save_battlelog_data(data: dict, player_tag: str) -> dict:
    """
    Validate and save battlelog data.

    Args:
        data: Raw battlelog data from Brawl Stars API
        player_tag: Player tag identifier

    Returns:
        Validated battlelog data dict
    """
    from brawlstar_project.player.models.battlelog import BattlelogData

    # Validate data with Pydantic model
    battlelog_data = BattlelogData.model_validate(data)
    return battlelog_data.model_dump()


def convert_json_to_parquet_generic(
    ingested_base_dir: str,
    raw_base_dir: str,
    json_filename: str,
    parquet_filename: str,
    flatten_func: Callable[[dict], pl.DataFrame],
):
    """
    Convert JSON files to Parquet files for all players and dates.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
        json_filename: Name of JSON files to read (e.g., "player.json", "battlelog.json")
        parquet_filename: Name of Parquet files to write (e.g., "player.parquet")
        flatten_func: Function to convert raw JSON dict to polars.DataFrame
    """
    for player_dir in Path(ingested_base_dir).iterdir():
        if not player_dir.is_dir():
            continue
        for date_dir in player_dir.iterdir():
            if not date_dir.is_dir():
                continue

            json_file = date_dir / json_filename
            if not json_file.exists():
                print(f"Missing {json_filename} in {date_dir}")
                continue

            with open(json_file, "r") as f:
                raw_data = json.load(f)

            df = flatten_func(raw_data)

            # Skip if DataFrame is empty
            if df.is_empty():
                print(f"No data to convert in {json_file}")
                continue

            out_dir = Path(raw_base_dir) / player_dir.name / date_dir.name
            out_dir.mkdir(parents=True, exist_ok=True)

            parquet_file = out_dir / parquet_filename

            df.write_parquet(str(parquet_file))
            print(f"Converted {json_file} to {parquet_file}")


def convert_all_json_to_parquet(
    ingested_base_dir: str = "data/ingested", raw_base_dir: str = "data/raw"
):
    """
    Convert all JSON files (player and battlelog) to Parquet files.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
    """
    # Convert player data
    convert_json_to_parquet_generic(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="player.json",
        parquet_filename="player.parquet",
        flatten_func=flatten_player_data,
    )

    # Convert battlelog data
    convert_json_to_parquet_generic(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="battlelog.json",
        parquet_filename="battlelog.parquet",
        flatten_func=flatten_battlelog_data,
    )


def flatten_player_data(data: dict) -> pl.DataFrame:
    """
    Flatten player data to a Polars DataFrame.

    Args:
        data: Raw player data dict

    Returns:
        Polars DataFrame with a single row (flattened player data)
    """
    from brawlstar_project.player.models.player import create_flattened_player_data

    flattened = create_flattened_player_data(data)
    return pl.DataFrame([flattened.model_dump()])


def flatten_battlelog_data(data: dict) -> pl.DataFrame:
    """
    Flatten battlelog raw data to a Polars DataFrame.

    Args:
        data: Raw battlelog data dict (with "items" key)

    Returns:
        Polars DataFrame with normalized battlelog info
    """
    battles = data.get("items", [])
    if not battles:
        return pl.DataFrame()

    df = pl.DataFrame(battles)

    if "brawler" in df.columns:
        # Flatten nested brawler dict column if exists
        brawler_df = pl.DataFrame(df["brawler"].to_list())
        df = df.drop("brawler").with_columns(brawler_df)

    if "battleTime" in df.columns:
        df = df.with_columns(
            pl.col("battleTime").str.strptime(
                pl.Datetime, format="%Y%m%dT%H%M%S.%.fZ", strict=False
            )
        )

    return df
