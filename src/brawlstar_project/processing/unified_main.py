"""
Unified main entry point for all pipeline stages (batch mode only).

This script reads config.yaml and runs the full pipeline for all player and club tags listed, including all club members.
- Deduplicates player tags so each player is only processed once.
- Intended for batch or Airflow orchestration.
- For ad-hoc or partial runs, use the stage-specific main.py scripts.
"""

import logging
import subprocess
from datetime import datetime

from brawlstar_project.entities.club import Club
from brawlstar_project.processing.factory.runner_factory import RunnerFactory
from brawlstar_project.processing.ingested.api_client import BrawlStarsClient
from brawlstar_project.processing.ingested.config import ConfigLoader
from brawlstar_project.processing.utils import fetch_club_members_data
from brawlstar_project.processing.utils.config_utils import load_pipeline_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline_for_tag(
    tag: str, mode: str, client, delay: float = 1.0, date: str = None
):
    factory = RunnerFactory()
    runner = factory.get_runner(mode)
    logger.info(f"\n🚀 Running {mode} pipeline for tag: {tag}")
    result = runner.run(client=client, tag=tag, delay=delay)
    logger.info(f"📊 Result: {result}")


def run_stage(stage: str, date: str):
    """Run a pipeline stage by calling its main.py via subprocess."""
    logger.info(f"\n🚀 Running {stage} stage for date: {date}")
    stage_map = {
        "raw": ["uv", "run", "python", "src/brawlstar_project/processing/raw/main.py"],
        "processed": [
            "uv",
            "run",
            "python",
            "src/brawlstar_project/processing/processed/main.py",
            "--mode",
            "all",
            "--date",
            date,
        ],
        "cleaned": [
            "uv",
            "run",
            "python",
            "src/brawlstar_project/processing/cleaned/main.py",
            "--date",
            date,
        ],
    }
    cmd = stage_map[stage]
    logger.info(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main():
    try:
        config = load_pipeline_config()
    except FileNotFoundError as e:
        logger.error(str(e))
        return
    player_tags = set(config.get("default_player_tags", []))
    club_tags = config.get("default_club_tags", [])

    # Load config and create client
    config_env = ConfigLoader.from_env()
    client = BrawlStarsClient(api_key=config_env.api_key, base_url=config_env.base_url)

    # Use today's date for partitioning
    today = datetime.today().strftime("%Y-%m-%d")

    # Collect all member tags from all clubs
    all_member_tags = set()
    for club_tag in club_tags:
        club_entity = Club(club_tag)
        club_members_data = fetch_club_members_data(client, club_entity)
        member_tags = [
            member["tag"]
            for member in club_members_data.get("items", [])
            if "tag" in member
        ]
        all_member_tags.update(member_tags)
        logger.info(
            f"\n🎯 Found {len(member_tags)} members in club {club_tag} to process."
        )

    # Deduplicate: union of player_tags and all_member_tags
    all_player_tags = player_tags.union(all_member_tags)

    # Run full pipeline for each unique player
    for tag in all_player_tags:
        run_pipeline_for_tag(tag, mode="player", client=client, date=today)
    # Run full pipeline for each club
    for club_tag in club_tags:
        run_pipeline_for_tag(club_tag, mode="club", client=client, date=today)

    # Run raw, processed, and cleaned stages
    run_stage("raw", today)
    run_stage("processed", today)
    run_stage("cleaned", today)


if __name__ == "__main__":
    main()
