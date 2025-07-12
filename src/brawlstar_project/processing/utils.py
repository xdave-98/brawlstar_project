import json
import os
from collections import defaultdict
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

import polars as pl


def save_json_data(
    data: dict,
    tag: str,
    base_dir: str,
    filename: str = "data.json",
    validate_func: Optional[Callable[[dict], dict]] = None,
) -> str:
    """
    Save any data dict to JSON file under tag/today/filename.

    Args:
        data: Raw data dict
        tag: Tag identifier (player or club)
        base_dir: Base directory for data storage
        filename: JSON filename (default: "data.json")
        validate_func: Optional callable to validate/transform data before saving

    Returns:
        Path to saved JSON file
    """
    if validate_func:
        data = validate_func(data)

    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = os.path.join(base_dir, tag, today)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, filename)

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Data saved in: {file_path}")
    return file_path


def save_json_data_partitioned(
    data: dict,
    tag: str,
    data_type: str,  # "player" or "club"
    filename: str = "data.json",
    validate_func: Optional[Callable[[dict], dict]] = None,
) -> str:
    """
    Save any data dict to JSON file under partitioned structure: data_type/tag/today/filename.

    Args:
        data: Raw data dict
        tag: Tag identifier (player or club)
        data_type: Data type ("player" or "club")
        filename: JSON filename (default: "data.json")
        validate_func: Optional callable to validate/transform data before saving

    Returns:
        Path to saved JSON file
    """
    if validate_func:
        data = validate_func(data)

    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = os.path.join("data/ingested", data_type, tag, today)
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
        def wrapper(data: dict, tag: str, **kwargs) -> str:
            # Call the validation function with both data and tag
            validated_data = func(data, tag)
            return save_json_data(
                data=validated_data,
                tag=tag,
                base_dir=base_dir,
                filename=filename,
                **kwargs,
            )

        return wrapper

    return decorator


@save_json_decorator(base_dir="data/ingested", filename="player.json")
def save_player_data(data: dict, tag: str) -> dict:
    """
    Validate and save player data.

    Args:
        data: Raw player data from Brawl Stars API
        tag: Player tag identifier

    Returns:
        Validated player data dict
    """
    from brawlstar_project.entities.player.models.player import PlayerData

    # Validate data with Pydantic model
    player_data = PlayerData.model_validate(data)
    return player_data.model_dump()


@save_json_decorator(base_dir="data/ingested", filename="battlelog.json")
def save_battlelog_data(data: dict, tag: str) -> dict:
    """
    Validate and save battlelog data.

    Args:
        data: Raw battlelog data from Brawl Stars API
        tag: Player tag identifier

    Returns:
        Validated battlelog data dict
    """
    from brawlstar_project.entities.player.models.battlelog import BattlelogData

    # Validate data with Pydantic model
    battlelog_data = BattlelogData.model_validate(data)
    return battlelog_data.model_dump()


@save_json_decorator(base_dir="data/ingested", filename="club.json")
def save_club_data(data: dict, tag: str) -> dict:
    """
    Validate and save club data.

    Args:
        data: Raw club data from Brawl Stars API
        tag: Club tag identifier

    Returns:
        Validated club data dict
    """
    from brawlstar_project.entities.club.models.club import ClubData

    club_data = ClubData.model_validate(data)
    return club_data.model_dump()


@save_json_decorator(base_dir="data/ingested", filename="club_members.json")
def save_club_members_data(data: dict, tag: str) -> dict:
    """
    Validate and save club members data.

    Args:
        data: Raw club members data from Brawl Stars API
        tag: Club tag identifier

    Returns:
        Validated club members data dict
    """
    from brawlstar_project.entities.club.models.members import ClubMembersData

    club_members_data = ClubMembersData.model_validate(data)
    return club_members_data.model_dump()


# Partitioned save functions
def save_player_data_partitioned(data: dict, player_tag: str) -> dict:
    """
    Validate and save player data in partitioned structure.

    Args:
        data: Raw player data from Brawl Stars API
        player_tag: Player tag identifier

    Returns:
        Validated player data dict
    """
    from brawlstar_project.entities.player.models.player import PlayerData

    # Validate data with Pydantic model
    player_data = PlayerData.model_validate(data)
    validated_data = player_data.model_dump()

    # Save to partitioned structure
    save_json_data_partitioned(validated_data, player_tag, "player", "player.json")
    return validated_data


def save_battlelog_data_partitioned(data: dict, player_tag: str) -> dict:
    """
    Validate and save battlelog data in partitioned structure.

    Args:
        data: Raw battlelog data from Brawl Stars API
        player_tag: Player tag identifier

    Returns:
        Validated battlelog data dict
    """
    from brawlstar_project.entities.player.models.battlelog import BattlelogData

    # Validate data with Pydantic model
    battlelog_data = BattlelogData.model_validate(data)
    validated_data = battlelog_data.model_dump()

    # Save to partitioned structure
    save_json_data_partitioned(validated_data, player_tag, "player", "battlelog.json")
    return validated_data


def save_club_data_partitioned(data: dict, club_tag: str) -> dict:
    """
    Validate and save club data in partitioned structure.

    Args:
        data: Raw club data from Brawl Stars API
        club_tag: Club tag identifier

    Returns:
        Validated club data dict
    """
    from brawlstar_project.entities.club.models.club import ClubData

    # Validate data with Pydantic model
    club_data = ClubData.model_validate(data)
    validated_data = club_data.model_dump()

    # Save to partitioned structure
    save_json_data_partitioned(validated_data, club_tag, "club", "club.json")
    return validated_data


def save_club_members_data_partitioned(data: dict, club_tag: str) -> dict:
    """
    Validate and save club members data in partitioned structure.

    Args:
        data: Raw club members data from Brawl Stars API
        club_tag: Club tag identifier

    Returns:
        Validated club members data dict
    """
    from brawlstar_project.entities.club.models.members import ClubMembersData

    # Validate data with Pydantic model
    club_members_data = ClubMembersData.model_validate(data)
    validated_data = club_members_data.model_dump()

    # Save to partitioned structure
    save_json_data_partitioned(validated_data, club_tag, "club", "club_members.json")
    return validated_data


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


def convert_jsons_to_parquet_per_date(
    ingested_base_dir: str,
    raw_base_dir: str,
    json_filename: str,
    parquet_filename: str,
    flatten_func: Callable[[dict], pl.DataFrame],
):
    """
    Convert JSON files to one Parquet file per date, concatenating all players' data.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
        json_filename: Name of JSON files to read (e.g., "player.json", "battlelog.json")
        parquet_filename: Name of Parquet file to write (e.g., "player.parquet")
        flatten_func: Function to convert raw JSON dict to polars.DataFrame
    """

    # 1. Recenser tous les dossiers de joueurs et dates pour trouver toutes les dates existantes
    date_to_files = defaultdict(
        list
    )  # dict: date_str -> list of (player_tag, json_file_path)
    ingested_path = Path(ingested_base_dir)

    for player_dir in ingested_path.iterdir():
        if not player_dir.is_dir():
            continue
        player_tag = player_dir.name
        for date_dir in player_dir.iterdir():
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name
            json_file = date_dir / json_filename
            if json_file.exists():
                date_to_files[date_str].append((player_tag, json_file))

    # 2. Pour chaque date, concaténer les données de tous les joueurs et écrire un parquet
    for date_str, files in date_to_files.items():
        dfs: list[pl.DataFrame] = []
        for player_tag, json_file in files:
            with open(json_file, "r") as f:
                raw_data = json.load(f)

            df = flatten_func(raw_data)

            if df.is_empty():
                print(f"No data in {json_file}, skipping")
                continue

            dfs.append(df)

        if not dfs:
            print(f"No valid data for date {date_str}, skipping")
            continue

        all_data_df = pl.concat(dfs)

        out_dir = Path(raw_base_dir) / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        parquet_file = out_dir / parquet_filename
        all_data_df.write_parquet(parquet_file)

        print(f"Written consolidated parquet for date {date_str}: {parquet_file}")


def convert_all_json_to_parquet(
    ingested_base_dir: str = "data/ingested", raw_base_dir: str = "data/raw"
):
    convert_jsons_to_parquet_per_date(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="player.json",
        parquet_filename="player.parquet",
        flatten_func=flatten_player_data,
    )

    convert_jsons_to_parquet_per_date(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="battlelog.json",
        parquet_filename="battlelog.parquet",
        flatten_func=flatten_battlelog_data,
    )

    convert_jsons_to_parquet_per_date(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="club.json",
        parquet_filename="club.parquet",
        flatten_func=flatten_club_data,
    )

    convert_jsons_to_parquet_per_date(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        json_filename="club_members.json",
        parquet_filename="club_members.parquet",
        flatten_func=flatten_club_members_data,
    )


def convert_all_json_to_parquet_partitioned(
    ingested_base_dir: str = "data/ingested", raw_base_dir: str = "data/raw"
):
    """
    Convert JSON files to Parquet files for partitioned structure.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
    """
    # Convert player data
    convert_jsons_to_parquet_per_date_partitioned(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        data_type="player",
        json_filename="player.json",
        parquet_filename="player.parquet",
        flatten_func=flatten_player_data,
    )

    # Convert battlelog data
    convert_jsons_to_parquet_per_date_partitioned(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        data_type="player",
        json_filename="battlelog.json",
        parquet_filename="battlelog.parquet",
        flatten_func=flatten_battlelog_data,
    )

    # Convert club data
    convert_jsons_to_parquet_per_date_partitioned(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        data_type="club",
        json_filename="club.json",
        parquet_filename="club.parquet",
        flatten_func=flatten_club_data,
    )

    # Convert club members data
    convert_jsons_to_parquet_per_date_partitioned(
        ingested_base_dir=ingested_base_dir,
        raw_base_dir=raw_base_dir,
        data_type="club",
        json_filename="club_members.json",
        parquet_filename="club_members.parquet",
        flatten_func=flatten_club_members_data,
    )


def convert_jsons_to_parquet_per_date_partitioned(
    ingested_base_dir: str,
    raw_base_dir: str,
    data_type: str,  # "player" or "club"
    json_filename: str,
    parquet_filename: str,
    flatten_func: Callable[[dict], pl.DataFrame],
):
    """
    Convert JSON files to one Parquet file per date for partitioned structure.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
        data_type: Data type ("player" or "club")
        json_filename: Name of JSON files to read (e.g., "player.json", "battlelog.json")
        parquet_filename: Name of Parquet file to write (e.g., "player.parquet")
        flatten_func: Function to convert raw JSON dict to polars.DataFrame
    """

    # 1. Find all dates and files for the data type
    date_to_files = defaultdict(list)  # dict: date_str -> list of (tag, json_file_path)
    ingested_path = Path(ingested_base_dir) / data_type

    if not ingested_path.exists():
        print(f"No {data_type} data found in {ingested_path}")
        return

    for tag_dir in ingested_path.iterdir():
        if not tag_dir.is_dir():
            continue
        tag = tag_dir.name
        for date_dir in tag_dir.iterdir():
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name
            json_file = date_dir / json_filename
            if json_file.exists():
                date_to_files[date_str].append((tag, json_file))

    # 2. For each date, concatenate all data and write one parquet
    for date_str, files in date_to_files.items():
        dfs: list[pl.DataFrame] = []
        for tag, json_file in files:
            with open(json_file, "r") as f:
                raw_data = json.load(f)

            df = flatten_func(raw_data)

            if df.is_empty():
                print(f"No data in {json_file}, skipping")
                continue

            dfs.append(df)

        if not dfs:
            print(f"No valid data for date {date_str}, skipping")
            continue

        all_data_df = pl.concat(dfs)

        out_dir = Path(raw_base_dir) / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        parquet_file = out_dir / parquet_filename
        all_data_df.write_parquet(parquet_file)

        print(f"Written consolidated parquet for date {date_str}: {parquet_file}")


def flatten_player_data(data: dict) -> pl.DataFrame:
    """
    Flatten player data to a Polars DataFrame.

    Args:
        data: Raw player data dict

    Returns:
        Polars DataFrame with a single row (flattened player data)
    """
    from brawlstar_project.entities.player.models.player import (
        create_flattened_player_data,
    )

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


def flatten_club_data(data: dict) -> pl.DataFrame:
    """
    Flatten club data to a Polars DataFrame.

    Args:
        data: Raw club data dict

    Returns:
        Polars DataFrame with a single row (flattened club data)
    """
    from brawlstar_project.entities.club.models.club import (
        create_flattened_club_data,
    )

    flattened = create_flattened_club_data(data)
    return pl.DataFrame([flattened.model_dump()])


def fetch_club_data(client, club) -> dict:
    """
    Fetch club data and save it to the pipeline.

    Args:
        client: BrawlStars API client
        club: Club entity

    Returns:
        Club data dictionary
    """
    print("📋 Fetching club data...")
    club_data = client.get_club(club.formatted_tag)
    save_club_data_partitioned(club_data, club.tag)
    return club_data


def fetch_club_members_data(client, club) -> dict:
    """
    Fetch club members data and save it to the pipeline.

    Args:
        client: BrawlStars API client
        club: Club entity

    Returns:
        Club members data dictionary
    """
    print("👥 Fetching club members data...")
    club_members = client.get_club_members(club.formatted_tag)
    save_club_members_data_partitioned(club_members, club.tag)
    return club_members


def flatten_club_members_data(data: dict) -> pl.DataFrame:
    """
    Flatten club members data to a Polars DataFrame.

    Args:
        data: Raw club members data dict (with "items" key)

    Returns:
        Polars DataFrame with normalized club members info
    """
    from brawlstar_project.entities.club.models.members import (
        create_flattened_club_members_data,
    )

    flattened = create_flattened_club_members_data(data)
    return pl.DataFrame(flattened.items)
