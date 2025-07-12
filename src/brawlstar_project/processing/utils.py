from datetime import datetime
import os

def save_player_data(data: dict, base_dir: str = "data/raw") -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = os.path.join(base_dir, today)

    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "player.json")

    '''
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    '''
    print(f"✅ Données sauvegardées dans {file_path}")
    return file_path
