import logging
from dataclasses import dataclass

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class PlayerAnalysis:
    player_df: pl.DataFrame

    def display_basic_stats(self) -> None:
        """Display basic statistics about the data."""
        logger.info("=" * 60)
        logger.info("📊 BASIC STATISTICS")
        logger.info("=" * 60)

        logger.info(f"📈 Total records: {len(self.player_df)}")
        logger.info(f"🏷️  Unique players: {self.player_df['tag'].n_unique()}")
        logger.info(
            f"📅 Date range: {self.player_df['extracted_at'].min()} to {self.player_df['extracted_at'].max()}"
        )

        logger.info("\n🏆 TROPHY STATISTICS:")
        logger.info(f"   Average trophies: {self.player_df['trophies'].mean():.0f}")
        logger.info(f"   Max trophies: {self.player_df['trophies'].max()}")
        logger.info(f"   Min trophies: {self.player_df['trophies'].min()}")

        logger.info("\n⚔️ BATTLE STATISTICS:")
        logger.info(f"   3v3 Victories: {self.player_df['three_vs_three_victories'].sum()}")
        logger.info(f"   Solo Victories: {self.player_df['solo_victories'].sum()}")
        logger.info(f"   Duo Victories: {self.player_df['duo_victories'].sum()}")

        logger.info("\n👥 BRAWLER STATISTICS:")
        logger.info(f"   Total brawlers: {self.player_df['total_brawlers'].sum()}")
        logger.info(f"   Maxed brawlers: {self.player_df['maxed_brawlers'].sum()}")
        logger.info(
            f"   Average brawler trophies: {self.player_df['total_brawler_trophies'].mean():.0f}"
        )


@dataclass
class BattlelogAnalysis:
    battlelog_df: pl.DataFrame

    def count_total_battles(self) -> int:
        """
        Count total number of battles in the battlelog DataFrame.

        Returns:
            Total number of battles
        """
        return len(self.battlelog_df)

    def count_battles_by_mode(self) -> pl.DataFrame:
        """
        Count battles grouped by battle mode.

        Returns:
            DataFrame with mode and count columns
        """
        return self.battlelog_df.group_by("battle_mode").agg(
            pl.count().alias("battle_count")
        )

    def count_battles_by_result(self) -> pl.DataFrame:
        """
        Count battles grouped by result (victory/defeat).

        Returns:
            DataFrame with result and count columns
        """
        return self.battlelog_df.group_by("battle_result").agg(
            pl.count().alias("battle_count")
        )

    def count_battles_by_brawler(self) -> pl.DataFrame:
        """
        Count battles grouped by brawler used.

        Returns:
            DataFrame with brawler_name and count columns
        """
        return self.battlelog_df.group_by("brawler_name").agg(
            pl.count().alias("battle_count")
        )

    def display_battle_stats(self) -> None:
        """Display comprehensive battle statistics."""
        logger.info("=" * 60)
        logger.info("⚔️ BATTLE STATISTICS")
        logger.info("=" * 60)

        total_battles = self.count_total_battles()
        logger.info(f"📊 Total battles collected: {total_battles}")

        if total_battles == 0:
            logger.warning("❌ No battles found in the data!")
            return

    def display_battlelog_count_per_player(self) -> None:
        """Display the number of battlelogs per player."""
        logger.info("=" * 60)
        logger.info("📊 BATTLELOG COUNT PER PLAYER")
        logger.info("=" * 60)

        if self.battlelog_df.is_empty():
            logger.warning("❌ No battlelog data found!")
            return

        # Count battles per player
        battles_per_player = (
            self.battlelog_df.group_by("player_tag")
            .agg(pl.count().alias("battle_count"))
            .sort("battle_count", descending=True)
        )

        logger.info(f"�� Total players with battlelog data: {len(battles_per_player)}")

        if len(battles_per_player) == 0:
            logger.warning("❌ No battlelog data found!")
            return

        logger.info("\n🏆 TOP PLAYERS BY BATTLE COUNT:")
        for i, row in enumerate(battles_per_player.head(10).iter_rows(named=True), 1):
            logger.info(f"   {i:2d}. {row['player_tag']}: {row['battle_count']} battles")

        logger.info("\n📊 STATISTICS:")
        logger.info(
            f"   Average battles per player: {battles_per_player['battle_count'].mean():.1f}"
        )
        logger.info(f"   Max battles per player: {battles_per_player['battle_count'].max()}")
        logger.info(f"   Min battles per player: {battles_per_player['battle_count'].min()}")

        # Show distribution
        logger.info("\n📈 DISTRIBUTION:")
        logger.info(
            f"   Players with 1-5 battles: {len(battles_per_player.filter(pl.col('battle_count').is_between(1, 5)))}"
        )
        logger.info(
            f"   Players with 6-10 battles: {len(battles_per_player.filter(pl.col('battle_count').is_between(6, 10)))}"
        )
        logger.info(
            f"   Players with 11+ battles: {len(battles_per_player.filter(pl.col('battle_count') >= 11))}"
        )
