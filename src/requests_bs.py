import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
API_KEY = os.getenv("BRAWLSTARS_API_KEY")
PLAYER_TAG = os.getenv("BRAWLSTARS_PLAYER_TAG")
BASE_URL = os.getenv("BRAWLSTARS_BASE_URL")

# Validate required environment variables
if not API_KEY:
    raise ValueError("BRAWLSTARS_API_KEY not found in environment variables")
if not PLAYER_TAG:
    raise ValueError("BRAWLSTARS_PLAYER_TAG not found in environment variables")

# Format player tag for API
PLAYER_TAG = PLAYER_TAG.replace("#", "%23")

# Headers with the token
HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

def get_player(player_tag):
    url = f"{BASE_URL}players/{player_tag}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    # Check if API key is available
    if not API_KEY:
        print("❌ Error: BRAWLSTARS_API_KEY not found in .env file")
        exit(1)
    
    data = get_player(PLAYER_TAG)
    print("✅ Player data fetched successfully!")
    print(f"Player: {data.get('name', 'Unknown')}")
    print(f"Trophies: {data.get('trophies', 0)}")
    
    '''
    # Save to data folder
    os.makedirs("data/raw", exist_ok=True)
    with open("data/raw/player.json", "w") as f:
        json.dump(data, f, indent=2)

    print("✅ Data saved to data/raw/player.json")
    '''
