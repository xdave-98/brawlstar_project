import argparse

from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.ingested.factory import RunnerFactory


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
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay (in seconds) between API calls for club-players mode",
    )

    args = parser.parse_args()

    config = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config.api_key, base_url=config.base_url)

    print(f"🚀 Starting {args.mode} ingestion pipeline…")
    print(f"📋 Target: {args.tag}")

    factory: RunnerFactory = RunnerFactory()  # type: ignore
    runner = factory.get_runner(args.mode)
    result = runner.run(client, args.tag, getattr(args, "delay", 1.0))  # type: ignore

    print("\n📊 Result:")
    print(result)


if __name__ == "__main__":
    main()
