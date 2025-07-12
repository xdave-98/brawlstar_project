import argparse
from pathlib import Path

from brawlstar_project.player.player import Player


def main():
    """Main function to display Parquet data."""
    parser = argparse.ArgumentParser(
        description="Display Brawl Stars player data from Parquet files"
    )
    parser.add_argument("--player-tag", required=True, help="Player tag to analyze")
    parser.add_argument(
        "--data-dir", default="data/raw", help="Directory containing Parquet files"
    )
    parser.add_argument("--days", type=int, default=7, help="Number of days to load")

    args = parser.parse_args()

    print(f"🚀 Loading Brawl Stars player data for {args.player_tag}...")

    # Create Player instance and load data
    player = Player(args.player_tag)
    df = player.load_player_data(Path(args.data_dir), args.days)

    if df is None or df.is_empty():
        print("❌ No data found!")
        return

    print(f"✅ Loaded {len(df)} records from Parquet files")

    print(df)
    print("Should show")

    # Display various analyses
    # player_analysis = PlayerAnalysis(df)
    # player_analysis.display_basic_stats()

    # print("🎉 Analysis complete!")


if __name__ == "__main__":
    main()
