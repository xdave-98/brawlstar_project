import argparse
from pathlib import Path

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player


def main():
    """Main function to display all Parquet data as DataFrames."""
    parser = argparse.ArgumentParser(
        description="Display Brawl Stars data from Parquet files"
    )
    parser.add_argument("--player-tag", required=True, help="Player tag to analyze")
    parser.add_argument("--club-tag", help="Club tag to analyze")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to load")

    args = parser.parse_args()

    print(f"\n🚀 Loading player data for {args.player_tag}...")
    player = Player(args.player_tag)
    player_df = player.load_player_data(Path(args.data_dir) / "raw", args.days)
    print(player_df)

    print(f"\n⚔️ Loading battlelog data for {args.player_tag}...")
    battlelog_df = player.load_battlelog_data(Path(args.data_dir) / "raw", args.days)
    print(battlelog_df)

    if args.club_tag:
        print(f"\n🏛️ Loading club data for {args.club_tag}...")
        club = Club(args.club_tag)
        club_df = club.load_club_data(Path(args.data_dir) / "raw", args.days)
        print(club_df)

        print(f"\n👥 Loading club members data for {args.club_tag}...")
        club_members_df = club.load_club_members_data(
            Path(args.data_dir) / "raw", args.days
        )
        print(club_members_df)

if __name__ == "__main__":
    main()
