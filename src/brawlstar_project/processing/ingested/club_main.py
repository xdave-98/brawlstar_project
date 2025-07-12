import argparse

from brawlstar_project.entities.club import Club
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.utils import save_club_data, save_club_members_data


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and save Brawl Stars club data."
    )
    parser.add_argument(
        "club_tag",
        help="Club tag (e.g., #CLUB123)",
    )

    args = parser.parse_args()

    # Load configuration
    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)

    # Instantiate Club
    club = Club(args.club_tag)

    # Get club and club members data
    club_data = client.get_club(club.formatted_tag)
    club_members = client.get_club_members(club.formatted_tag)

    # Save both club and club members data
    save_club_data(club_data, club.tag)
    save_club_members_data(club_members, club.tag)


if __name__ == "__main__":
    main()
