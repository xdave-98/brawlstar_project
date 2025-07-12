import argparse
from pathlib import Path

from brawlstar_project.entities.player.player import Player
from brawlstar_project.processing.processed.analysis import (
    BattlelogAnalysis,
)


def main():
    """Main function to display Parquet data."""
    parser = argparse.ArgumentParser(
        description="Display Brawl Stars player data from Parquet files"
    )
    parser.add_argument("--player-tag", required=True, help="Player tag to analyze")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument("--days", type=int, default=7, help="Number of days to load")

    args = parser.parse_args()

    print(f"🚀 Loading Brawl Stars player data for {args.player_tag}...")

    # Create Player instance and load data
    player = Player(args.player_tag)
    df = player.load_player_data(Path(args.data_dir) / "raw", args.days)

    if df is None or df.is_empty():
        print("❌ No player data found!")
    else:
        print(f"✅ Loaded {len(df)} player records from Parquet files")
        print(df)
        print("Should show")

    # Load and analyze battlelog data
    print(f"\n⚔️ Loading battlelog data for {args.player_tag}...")
    battlelog_df = player.load_battlelog_data(Path(args.data_dir) / "raw", args.days)

    if battlelog_df is None or battlelog_df.is_empty():
        print("❌ No battlelog data found!")
    else:
        # Create battlelog analysis and count battles
        battlelog_analysis = BattlelogAnalysis(battlelog_df)
        total_battles = battlelog_analysis.count_total_battles()

        print(f"✅ Loaded {total_battles} battles from Parquet files")

        # Display comprehensive battle statistics
        battlelog_analysis.display_battle_stats()

    print("🎉 Analysis complete!")


if __name__ == "__main__":
    main()
