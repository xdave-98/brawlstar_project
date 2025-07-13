import argparse
import logging
from pathlib import Path

from brawlstar_project.entities.club import Club
from brawlstar_project.processing.factory.analysis_factory import AnalysisFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main function to display all Parquet data as DataFrames."""
    parser = argparse.ArgumentParser(description="Brawl Stars processed data analysis")
    parser.add_argument("--mode", help="Analysis mode: single-player, all-players, club")
    parser.add_argument("--player-tag", help="Player tag to analyze")
    parser.add_argument(
        "--all-players", action="store_true", help="Analyze all players"
    )
    parser.add_argument("--club-tag", help="Club tag to analyze")
    parser.add_argument(
        "--data-dir", default="data", help="Data directory"
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to load")

    args = parser.parse_args()

    # Determine mode
    mode = args.mode
    if not mode:
        if args.all_players:
            mode = "all-players"
        elif args.player_tag:
            mode = "single-player"
        elif args.club_tag:
            mode = "club"
        else:
            logger.error("❌ Please specify --mode, or one of --player-tag, --all-players, or --club-tag")
            return

    factory = AnalysisFactory()
    runner = factory.get_runner(mode)
    runner.run(
        player_tag=args.player_tag,
        club_tag=args.club_tag,
        data_dir=args.data_dir,
        days=args.days,
    )

    if args.club_tag:
        logger.info(f"\n👥 Loading club data for {args.club_tag}...")
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
