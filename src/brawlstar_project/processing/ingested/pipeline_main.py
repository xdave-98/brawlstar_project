import argparse
import time
from typing import List

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.utils.json_utils import (
    fetch_club_members_data,
    save_battlelog_data_partitioned,
    save_player_data_partitioned,
)
from brawlstar_project.processing.utils.runner import get_runner


def extract_member_tags(club_members_data: dict) -> List[str]:
    """
    Extract member tags from club members data.

    Args:
        club_members_data: Raw club members data from API

    Returns:
        List of member tags
    """
    member_tags = []
    for member in club_members_data.get("items", []):
        member_tag = member.get("tag")
        if member_tag:
            member_tags.append(member_tag)
    return member_tags


def process_single_player(client: BrawlStarsClient, player_tag: str) -> bool:
    """
    Process a single player's data.

    Args:
        client: BrawlStars API client
        player_tag: Player tag

    Returns:
        True if successful, False otherwise
    """
    print(f"👤 Processing single player: {player_tag}")

    try:
        player = Player(player_tag)

        # Fetch player data
        print("  📊 Fetching player data...")
        player_data = client.get_player(player.formatted_tag)
        save_player_data_partitioned(player_data, player.tag)

        # Fetch battlelog data
        print("  ⚔️ Fetching battlelog data...")
        battlelog_data = client.get_battlelog(player.formatted_tag)
        save_battlelog_data_partitioned(battlelog_data, player.tag)

        print(f"  ✅ Completed data fetch for {player_tag}")
        return True

    except Exception as e:
        print(f"  ❌ Error processing {player_tag}: {e}")
        return False


def process_single_club(client: BrawlStarsClient, club_tag: str) -> bool:
    """
    Process a single club's data (club info + members list).

    Args:
        client: BrawlStars API client
        club_tag: Club tag

    Returns:
        True if successful, False otherwise
    """
    print(f"🏛️ Processing single club: {club_tag}")

    try:
        club = Club(club_tag)

        # Fetch club data
        # print("  📋 Fetching club data...")
        # club_data = fetch_club_data(client, club)

        # Fetch club members data
        print("  👥 Fetching club members data...")
        club_members_data = fetch_club_members_data(client, club)

        member_count = len(club_members_data.get("items", []))
        print(f"  ✅ Completed data fetch for {club_tag} ({member_count} members)")
        return True

    except Exception as e:
        print(f"  ❌ Error processing {club_tag}: {e}")
        return False


def process_club_with_members(
    client: BrawlStarsClient, club_tag: str, delay: float = 1.0
) -> dict:
    """
    Process club data and all its members' data.

    Args:
        client: BrawlStars API client
        club_tag: Club tag
        delay: Delay between API calls in seconds

    Returns:
        Dictionary with processing statistics
    """
    print(f"🏛️ Processing club with all members: {club_tag}")

    try:
        club = Club(club_tag)

        # Step 2: Get club members data
        print("  👥 Fetching club members data...")
        club_members_data = fetch_club_members_data(client, club)

        # Step 3: Extract member tags
        member_tags = extract_member_tags(club_members_data)
        print(f"  🎯 Found {len(member_tags)} club members to process")

        # Step 4: Process all members
        successful = 0
        failed = 0

        for i, member_tag in enumerate(member_tags, 1):
            print(f"\n  👤 Processing member {i}/{len(member_tags)}: {member_tag}")

            try:
                player = Player(member_tag)

                # Fetch player data
                player_data = client.get_player(player.formatted_tag)
                save_player_data_partitioned(player_data, player.tag)

                # Fetch battlelog data
                battlelog_data = client.get_battlelog(player.formatted_tag)
                save_battlelog_data_partitioned(battlelog_data, player.tag)

                print(f"    ✅ Completed data fetch for {member_tag}")
                successful += 1

                # Add delay between API calls
                if i < len(member_tags):
                    print(f"    ⏳ Waiting {delay}s before next API call...")
                    time.sleep(delay)

            except Exception as e:
                print(f"    ❌ Error processing {member_tag}: {e}")
                failed += 1

        return {"total": len(member_tags), "successful": successful, "failed": failed}

    except Exception as e:
        print(f"  ❌ Error processing club {club_tag}: {e}")
        return {"total": 0, "successful": 0, "failed": 0}


def main():
    """Main function to orchestrate ingestion pipeline."""
    parser = argparse.ArgumentParser(
        description="Brawl Stars data ingestion pipeline with different modes."
    )
    parser.add_argument(
        "--mode",
        choices=["player", "club", "club-players"],
        required=True,
        help="Ingestion mode: player (single), club (single), club-players (club + all members)",
    )
    parser.add_argument("--tag", required=True, help="Player or Club tag")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay for club-players mode")

    args = parser.parse_args()

    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)

    print(f"🚀 Starting {args.mode} ingestion pipeline…")
    print(f"📋 Target: {args.tag}")

    runner = get_runner(args.mode)
    result = runner.run(client, args.tag, getattr(args, "delay", 1.0))

    print("\n📊 Result:")
    print(result)

if __name__ == "__main__":
    main()
