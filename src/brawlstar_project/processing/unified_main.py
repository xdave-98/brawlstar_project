"""
Unified main entry point for all pipeline stages.

This module provides a single entry point that can handle
ingestion, analysis, and other pipeline stages using the factory pattern.
"""

import argparse

from brawlstar_project.processing.factory.analysis_factory import AnalysisFactory
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.ingested.factory.runner_factory import RunnerFactory


def main():
    """Unified main function for all pipeline stages."""
    parser = argparse.ArgumentParser(
        description="Brawl Stars data pipeline - unified entry point"
    )
    
    # Pipeline stage
    parser.add_argument(
        "--stage",
        choices=["ingestion", "analysis"],
        required=True,
        help="Pipeline stage to run"
    )
    
    # Mode (for both stages)
    parser.add_argument(
        "--mode",
        required=True,
        help="Mode to run (depends on stage)"
    )
    
    # Common arguments
    parser.add_argument("--tag", help="Player or Club tag")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to load")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between API calls"
    )

    args = parser.parse_args()

    if args.stage == "ingestion":
        run_ingestion(args)
    elif args.stage == "analysis":
        run_analysis(args)
    else:
        print(f"❌ Unknown stage: {args.stage}")


def run_ingestion(args):
    """Run ingestion stage."""
    if not args.tag:
        print("❌ --tag is required for ingestion stage")
        return

    print(f"🚀 Starting {args.mode} ingestion...")
    
    # Load config and create client
    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)
    
    # Get runner from factory
    try:
        factory = RunnerFactory()
        runner = factory.get_runner(args.mode)
        result = runner.run(client, args.tag, args.delay)
        print(f"\n📊 Result: {result}")
    except ValueError as e:
        print(f"❌ Error: {e}")
        print(f"Available modes: {factory.list_modes()}")


def run_analysis(args):
    """Run analysis stage."""
    print(f"📊 Starting {args.mode} analysis...")
    
    # Get runner from factory
    try:
        factory = AnalysisFactory()
        runner = factory.get_runner(args.mode)
        
        # Prepare kwargs based on mode
        kwargs = {
            "data_dir": args.data_dir,
            "days": args.days
        }
        
        if args.mode == "single-player":
            if not args.tag:
                print("❌ --tag is required for single-player analysis")
                return
            kwargs["player_tag"] = args.tag
        elif args.mode == "club":
            if not args.tag:
                print("❌ --tag is required for club analysis")
                return
            kwargs["club_tag"] = args.tag
        
        result = runner.run(**kwargs)
        print(f"\n📊 Result: {result}")
    except ValueError as e:
        print(f"❌ Error: {e}")
        print(f"Available modes: {factory.list_modes()}")


if __name__ == "__main__":
    main() 