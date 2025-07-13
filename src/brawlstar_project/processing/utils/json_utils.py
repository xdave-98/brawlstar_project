import json
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import polars as pl


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
    from brawlstar_project.entities.player.models import PlayerData

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
    from brawlstar_project.entities.player.models import BattlelogData

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
    from brawlstar_project.entities.club.models import ClubData

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
    from brawlstar_project.entities.club.models import ClubMembersData

    # Validate data with Pydantic model
    club_members_data = ClubMembersData.model_validate(data)
    validated_data = club_members_data.model_dump()

    # Save to partitioned structure
    save_json_data_partitioned(validated_data, club_tag, "club", "club_members.json")
    return validated_data


def convert_jsons_to_parquet_per_date_partitioned(
    ingested_base_dir: str,
    raw_base_dir: str,
    data_type: str,  # "player" or "club"
    json_filename: str,
    parquet_filename: str,
    flatten_func: Callable[[dict], pl.DataFrame],
):
    """
    Convert JSON files to Parquet files for partitioned structure.

    Args:
        ingested_base_dir: Base directory where JSON data is stored
        raw_base_dir: Base directory to write Parquet files
        data_type: Data type ("player" or "club")
        json_filename: JSON filename to convert
        parquet_filename: Parquet filename to create
        flatten_func: Function to flatten JSON data to DataFrame
    """
    ingested_path = Path(ingested_base_dir)
    raw_path = Path(raw_base_dir)

    # Find all data_type directories
    data_type_dirs = list(ingested_path.glob(f"{data_type}/*"))

    for data_type_dir in data_type_dirs:
        # Find all date directories for this tag
        date_dirs = list(data_type_dir.glob("*"))

        for date_dir in date_dirs:
            date_str = date_dir.name
            json_file = date_dir / json_filename

            if not json_file.exists():
                continue

            # Read JSON data
            with open(json_file, "r") as f:
                data = json.load(f)

            # Flatten data to DataFrame
            df = flatten_func(data)

            if df.is_empty():
                continue

            # Create output directory structure (without tag level)
            output_dir = raw_path / data_type / date_str
            output_dir.mkdir(parents=True, exist_ok=True)

            # Save as Parquet
            parquet_file = output_dir / parquet_filename
            df.write_parquet(str(parquet_file))

            print(f"Converted: {json_file} -> {parquet_file}")


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


# Flatten functions that convert JSON data to DataFrames
def flatten_player_data(data: dict) -> pl.DataFrame:
    """
    Flatten player data to DataFrame.

    Args:
        data: Raw player data from JSON

    Returns:
        DataFrame with flattened player data
    """
    from brawlstar_project.entities.player.models import (
        create_flattened_player_data,
    )

    try:
        flattened = create_flattened_player_data(data)
        return pl.DataFrame([flattened.model_dump()])
    except Exception as e:
        print(f"Error flattening player data: {e}")
        return pl.DataFrame()


def flatten_battlelog_data(data: dict) -> pl.DataFrame:
    """
    Flatten battlelog data to DataFrame.

    Args:
        data: Raw battlelog data from JSON

    Returns:
        DataFrame with flattened battlelog data
    """
    from brawlstar_project.entities.player.models import (
        create_flattened_battle_data,
    )

    try:
        # Extract player tag from the first battle or use a default
        player_tag = "#UNKNOWN"
        if data.get("items") and len(data["items"]) > 0:
            # Try to find player tag in the first battle
            first_battle = data["items"][0]
            if "battle" in first_battle and "teams" in first_battle["battle"]:
                for team in first_battle["battle"]["teams"]:
                    if team and len(team) > 0:
                        player_tag = team[0].get("tag", "#UNKNOWN")
                        break

        flattened_battles = create_flattened_battle_data(data, player_tag)
        if flattened_battles:
            return pl.DataFrame([battle.model_dump() for battle in flattened_battles])
        else:
            return pl.DataFrame()
    except Exception as e:
        print(f"Error flattening battlelog data: {e}")
        return pl.DataFrame()


def flatten_club_data(data: dict) -> pl.DataFrame:
    """
    Flatten club data to DataFrame.

    Args:
        data: Raw club data from JSON

    Returns:
        DataFrame with flattened club data
    """
    from brawlstar_project.entities.club.models import create_flattened_club_data

    try:
        flattened = create_flattened_club_data(data)
        return pl.DataFrame([flattened.model_dump()])
    except Exception as e:
        print(f"Error flattening club data: {e}")
        return pl.DataFrame()


def flatten_club_members_data(data: dict) -> pl.DataFrame:
    """
    Flatten club members data to DataFrame.

    Args:
        data: Raw club members data from JSON

    Returns:
        DataFrame with flattened club members data
    """
    from brawlstar_project.entities.club.models import (
        create_flattened_club_members_data,
    )

    try:
        flattened = create_flattened_club_members_data(data)
        if flattened.items:
            return pl.DataFrame([member for member in flattened.items])
        else:
            return pl.DataFrame()
    except Exception as e:
        print(f"Error flattening club members data: {e}")
        return pl.DataFrame()
