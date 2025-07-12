import argparse

from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.player import Player
from brawlstar_project.processing.utils import save_player_data


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and save Brawl Stars player data."
    )
    parser.add_argument(
        "player_tag",
        help="Player tag (e.g., #ABC123)",
    )

    args = parser.parse_args()

    # Load configuration
    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)

    # Get player data
    player = Player(args.player_tag)
    data = client.get_player(player.formatted_tag)

    save_player_data(data, player.tag)


if __name__ == "__main__":
    main()
