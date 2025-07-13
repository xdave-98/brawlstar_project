import argparse
import logging
from pathlib import Path

from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player
from brawlstar_project.processing.processed import BattlelogAnalysis, PlayerAnalysis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def analyze_single_player(args):
    """Analyze a single player."""
    logger.info(f"\n🚀 Loading player data for {args.player_tag}...")
    player = Player(args.player_tag)
    player_df = player.load_player_data(Path(args.data_dir) / "raw", args.days)
    logger.info(player_df)

    # Analyze player data
    if not player_df.is_empty():
        logger.info("\n" + "=" * 60)
        logger.info("📊 PLAYER ANALYSIS")
        logger.info("=" * 60)
        player_analysis = PlayerAnalysis(player_df)
        player_analysis.display_basic_stats()

    logger.info(f"\n⚔️ Loading battlelog data for {args.player_tag}...")
    battlelog_df = player.load_battlelog_data(Path(args.data_dir) / "raw", args.days)
    logger.info(battlelog_df)

    # Analyze battlelog data
    if not battlelog_df.is_empty():
        logger.info("\n" + "=" * 60)
        logger.info("⚔️ BATTLELOG ANALYSIS")
        logger.info("=" * 60)
        battlelog_analysis = BattlelogAnalysis(battlelog_df)
        battlelog_analysis.display_battlelog_count_per_player()
        battlelog_analysis.display_battle_stats()


def analyze_all_players(args):
    """Analyze all available players."""
    import polars as pl

    logger.info("\n🚀 Loading all player data...")

    # Load all player data
    raw_dir = Path(args.data_dir) / "raw"
    player_files = list(raw_dir.glob("player/*/player.parquet"))

    if not player_files:
        logger.warning("❌ No player data found!")
        return

    # Load all player data
    player_dfs = []
    for player_file in player_files:
        try:
            df = pl.read_parquet(player_file)
            player_dfs.append(df)
        except Exception as e:
            logger.warning(f"⚠️ Error loading {player_file}: {e}")

    if not player_dfs:
        logger.warning("❌ No valid player data found!")
        return

    # Combine all player data
    all_players_df = pl.concat(player_dfs)
    logger.info(f"📊 Loaded data for {all_players_df['tag'].n_unique()} unique players")

    # Analyze all players
    logger.info("\n" + "=" * 60)
    logger.info("📊 ALL PLAYERS ANALYSIS")
    logger.info("=" * 60)
    player_analysis = PlayerAnalysis(all_players_df)
    player_analysis.display_basic_stats()

    # Load all battlelog data
    logger.info("\n⚔️ Loading all battlelog data...")
    battlelog_files = list(raw_dir.glob("player/*/battlelog.parquet"))

    if battlelog_files:
        battlelog_dfs = []
        for battlelog_file in battlelog_files:
            try:
                df = pl.read_parquet(battlelog_file)
                battlelog_dfs.append(df)
            except Exception as e:
                logger.warning(f"⚠️ Error loading {battlelog_file}: {e}")

        if battlelog_dfs:
            all_battlelog_df = pl.concat(battlelog_dfs)
            logger.info(
                f"📊 Loaded battlelog data for {all_battlelog_df['player_tag'].n_unique()} unique players"
            )

            logger.info("\n" + "=" * 60)
            logger.info("⚔️ ALL PLAYERS BATTLELOG ANALYSIS")
            logger.info("=" * 60)
            battlelog_analysis = BattlelogAnalysis(all_battlelog_df)
            battlelog_analysis.display_battlelog_count_per_player()
            battlelog_analysis.display_battle_stats()
        else:
            logger.warning("❌ No valid battlelog data found!")
    else:
        logger.warning("❌ No battlelog data found!")


def main():
    """Main function to display all Parquet data as DataFrames."""
    parser = argparse.ArgumentParser(
        description="Display Brawl Stars data from Parquet files"
    )
    parser.add_argument("--player-tag", help="Player tag to analyze")
    parser.add_argument(
        "--all-players", action="store_true", help="Analyze all available players"
    )
    parser.add_argument("--club-tag", help="Club tag to analyze")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to load")

    args = parser.parse_args()

    if args.all_players:
        # Analyze all available players
        analyze_all_players(args)
    elif args.player_tag:
        # Analyze specific player
        analyze_single_player(args)
    else:
        logger.error("❌ Please specify either --player-tag or --all-players")
        return

    if args.club_tag:
        logger.info(f"\n��️ Loading club data for {args.club_tag}...")
        club = Club(args.club_tag)
        club_df = club.load_club_data(Path(args.data_dir) / "raw", args.days)
        logger.info(club_df)

        logger.info(f"\n👥 Loading club members data for {args.club_tag}...")
        club_members_df = club.load_club_members_data(
            Path(args.data_dir) / "raw", args.days
        )
        logger.info(club_members_df)


if __name__ == "__main__":
    main()
