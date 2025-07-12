import argparse

from brawlstar_project.entities.player import Player
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.utils import (
    save_battlelog_data,
    save_player_data,
)


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

    # Instantiate Player
    player = Player(args.player_tag)

    # Get player and player battlelog data
    player_data = client.get_player(player.formatted_tag)
    player_battlelog = client.get_battlelog(player.formatted_tag)

    # Save both player and battlelog data
    save_player_data(player_data, player.tag)
    save_battlelog_data(player_battlelog, player.tag)


if __name__ == "__main__":
    main()
