import logging
from datetime import datetime
from typing import Optional

import polars as pl

from brawlstar_project.constants.paths import DATA_PROCESSED_DIR, DATA_RAW_DIR
from brawlstar_project.entities.club import Club
from brawlstar_project.entities.player import Player

from .base_factory import BaseFactory, BaseRunner

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class PlayerProcessingRunner(BaseRunner):
    def run(self, date: Optional[str] = None, **kwargs):
        if date is None:
            date = datetime.today().strftime("%Y-%m-%d")
        raw_base = DATA_RAW_DIR
        processed_base = DATA_PROCESSED_DIR
        player_in = raw_base / "player" / date / "player.parquet"
        player_out = processed_base / "player" / date / "player.parquet"
        if player_in.exists():
            logger.info(f"Processing player data: {player_in}")
            player_df = pl.read_parquet(player_in)
            player_cleaned = Player.process_player_df(player_df)
            player_out.parent.mkdir(parents=True, exist_ok=True)
            player_cleaned.write_parquet(str(player_out))
            logger.info(f"Saved cleaned player data: {player_out}")
        else:
            logger.warning(f"Player data not found: {player_in}")


class BattlelogProcessingRunner(BaseRunner):
    def run(self, date: Optional[str] = None, **kwargs):
        if date is None:
            date = datetime.today().strftime("%Y-%m-%d")
        raw_base = DATA_RAW_DIR
        processed_base = DATA_PROCESSED_DIR
        battlelog_in = raw_base / "player" / date / "battlelog.parquet"
        battlelog_out = processed_base / "player" / date / "battlelog.parquet"
        if battlelog_in.exists():
            logger.info(f"Processing battlelog data: {battlelog_in}")
            battlelog_df = pl.read_parquet(battlelog_in)
            battlelog_cleaned = Player.process_battlelog_df(battlelog_df)
            battlelog_out.parent.mkdir(parents=True, exist_ok=True)
            battlelog_cleaned.write_parquet(str(battlelog_out))
            logger.info(f"Saved cleaned battlelog data: {battlelog_out}")
        else:
            logger.warning(f"Battlelog data not found: {battlelog_in}")


class ClubProcessingRunner(BaseRunner):
    def run(self, date: Optional[str] = None, **kwargs):
        if date is None:
            date = datetime.today().strftime("%Y-%m-%d")
        raw_base = DATA_RAW_DIR
        processed_base = DATA_PROCESSED_DIR
        club_in = raw_base / "club" / date / "club.parquet"
        club_out = processed_base / "club" / date / "club.parquet"
        if club_in.exists():
            logger.info(f"Processing club data: {club_in}")
            club_df = pl.read_parquet(club_in)
            club_cleaned = Club.process_club_df(club_df)
            club_out.parent.mkdir(parents=True, exist_ok=True)
            club_cleaned.write_parquet(str(club_out))
            logger.info(f"Saved cleaned club data: {club_out}")
        else:
            logger.warning(f"Club data not found: {club_in}")


class ClubMembersProcessingRunner(BaseRunner):
    def run(self, date: Optional[str] = None, **kwargs):
        if date is None:
            date = datetime.today().strftime("%Y-%m-%d")
        raw_base = DATA_RAW_DIR
        processed_base = DATA_PROCESSED_DIR
        club_members_in = raw_base / "club" / date / "club_members.parquet"
        club_members_out = processed_base / "club" / date / "club_members.parquet"
        if club_members_in.exists():
            logger.info(f"Processing club members data: {club_members_in}")
            club_members_df = pl.read_parquet(club_members_in)
            club_members_cleaned = Club.process_club_members_df(club_members_df)
            club_members_out.parent.mkdir(parents=True, exist_ok=True)
            club_members_cleaned.write_parquet(str(club_members_out))
            logger.info(f"Saved cleaned club members data: {club_members_out}")
        else:
            logger.warning(f"Club members data not found: {club_members_in}")


class AllProcessingRunner(BaseRunner):
    def run(self, date: Optional[str] = None, **kwargs):
        PlayerProcessingRunner().run(date=date)
        BattlelogProcessingRunner().run(date=date)
        ClubProcessingRunner().run(date=date)
        ClubMembersProcessingRunner().run(date=date)


class ProcessingFactory(BaseFactory):
    def __init__(self):
        super().__init__()
        self.register("player", PlayerProcessingRunner)
        self.register("battlelog", BattlelogProcessingRunner)
        self.register("club", ClubProcessingRunner)
        self.register("club_members", ClubMembersProcessingRunner)
        self.register("all", AllProcessingRunner)
