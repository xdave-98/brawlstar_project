import os
import json
from datetime import datetime
import polars as pl
from pathlib import Path
from typing import Dict, Any
from brawlstar_project.player.models import PlayerData, FlattenedPlayerData, create_flattened_player_data


def save_player_data(
    data: dict, player_tag: str, base_dir: str = "data/ingested"
) -> str:
    """
    Save player data to JSON file with Pydantic validation.

    Args:
        data: Raw player data from Brawl Stars API
        player_tag: Player tag identifier
        base_dir: Base directory for data storage

    Returns:
        Path to saved JSON file
    """
    # Validate data with Pydantic model
    player_data = PlayerData.model_validate(data)

    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = os.path.join(base_dir, player_tag, today)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "player.json")

    # Save validated data as JSON
    with open(file_path, "w") as f:
        json.dump(player_data.model_dump(), f, indent=2)

    print(f"Data are saved in: {file_path}")
    return file_path


def flatten_player_data(data: Dict[str, Any]) -> FlattenedPlayerData:
    """
    Flatten player data to extract only essential fields using Pydantic validation.

    Args:
        data: Raw player data from Brawl Stars API

    Returns:
        FlattenedPlayerData instance with validated and flattened data
    """
    return create_flattened_player_data(data)


def convert_json_to_parquet(ingested_base_dir="data/ingested", raw_base_dir="data/raw"):
    for player_dir in Path(ingested_base_dir).iterdir():
        if not player_dir.is_dir():
            continue
        for date_dir in player_dir.iterdir():
            if not date_dir.is_dir():
                continue

            json_file = date_dir / "player.json"
            if not json_file.exists():
                print(f"Missing player.json in {date_dir}")
                continue

            # Read JSON and flatten data with Pydantic validation
            with open(json_file, "r") as f:
                raw_data = json.load(f)

            flattened_data = flatten_player_data(raw_data)

            # Convert to DataFrame using model_dump()
            df = pl.DataFrame([flattened_data.model_dump()])

            # Build output path identical to data/raw
            out_dir = Path(raw_base_dir) / player_dir.name / date_dir.name
            out_dir.mkdir(parents=True, exist_ok=True)

            parquet_file = out_dir / "player.parquet"

            # Write Parquet
            df.write_parquet(str(parquet_file))
            print(f"Converted {json_file} to {parquet_file}")
