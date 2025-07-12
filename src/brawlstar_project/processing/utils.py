import os
import json
from datetime import datetime
import polars as pl
from pathlib import Path
from typing import Dict, Any


def save_player_data(data: dict, player_tag: str, base_dir: str = "data/ingested") -> str:
    today = datetime.today().strftime("%Y-%m-%d")

    dir_path = os.path.join(base_dir, player_tag, today)

    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "player.json")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Data are saved in: {file_path}")
    return file_path


def flatten_player_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten player data to extract only essential fields.
    
    Args:
        data: Raw player data from Brawl Stars API
        
    Returns:
        Flattened dictionary with only essential fields
    """
    flattened = {
        # Basic player info
        "tag": data.get("tag", ""),
        "name": data.get("name", ""),
        "nameColor": data.get("nameColor", ""),
        "trophies": data.get("trophies", 0),
        "highestTrophies": data.get("highestTrophies", 0),
        "expLevel": data.get("expLevel", 0),
        "expPoints": data.get("expPoints", 0),
        
        # Battle statistics
        "3vs3Victories": data.get("3vs3Victories", 0),
        "soloVictories": data.get("soloVictories", 0),
        "duoVictories": data.get("duoVictories", 0),
        "bestRoboRumbleTime": data.get("bestRoboRumbleTime", 0),
        "bestTimeAsBigBrawler": data.get("bestTimeAsBigBrawler", 0),
        
        # Club info (if exists)
        "club_name": data.get("club", {}).get("name", ""),
        "club_tag": data.get("club", {}).get("tag", ""),
        
        # Brawler summary
        "total_brawlers": len(data.get("brawlers", [])),
        "maxed_brawlers": sum(1 for b in data.get("brawlers", []) if b.get("power", 0) == 11),
        "total_brawler_trophies": sum(b.get("trophies", 0) for b in data.get("brawlers", [])),
        
        # Timestamp
        "extracted_at": datetime.now().isoformat()
    }
    
    return flattened


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

            # Read JSON and flatten data
            with open(json_file, 'r') as f:
                raw_data = json.load(f)
            
            flattened_data = flatten_player_data(raw_data)
            
            # Convert to DataFrame
            df = pl.DataFrame([flattened_data])

            # Build output path identical to data/raw
            out_dir = Path(raw_base_dir) / player_dir.name / date_dir.name
            out_dir.mkdir(parents=True, exist_ok=True)

            parquet_file = out_dir / "player.parquet"

            # Write Parquet
            df.write_parquet(str(parquet_file))
            print(f"Converted {json_file} to {parquet_file}")
