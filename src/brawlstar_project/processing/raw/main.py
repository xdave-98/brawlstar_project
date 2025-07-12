from brawlstar_project.processing.raw.config import ConfigLoader
from brawlstar_project.processing.raw.api_client import BrawlStarsClient

from brawlstar_project.processing.utils import save_player_data

def main():
    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)

    data = client.get_player(config.formatted_player_tag)

    save_player_data(data)

if __name__ == "__main__":
    main()
