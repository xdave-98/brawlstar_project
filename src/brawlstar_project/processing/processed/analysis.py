from dataclasses import dataclass

import polars as pl


@dataclass
class PlayerAnalysis:
    player_df: pl.DataFrame

    def display_basic_stats(self) -> None:
        """Display basic statistics about the data."""
        print("=" * 60)
        print("📊 BASIC STATISTICS")
        print("=" * 60)

        print(f"📈 Total records: {len(self.player_df)}")
        print(f"🏷️  Unique players: {self.player_df['tag'].n_unique()}")
        print(
            f"📅 Date range: {self.player_df['extracted_at'].min()} to {self.player_df['extracted_at'].max()}"
        )

        print("\n🏆 TROPHY STATISTICS:")
        print(f"   Average trophies: {self.player_df['trophies'].mean():.0f}")
        print(f"   Max trophies: {self.player_df['trophies'].max()}")
        print(f"   Min trophies: {self.player_df['trophies'].min()}")

        print("\n⚔️ BATTLE STATISTICS:")
        print(f"   3v3 Victories: {self.player_df['three_vs_three_victories'].sum()}")
        print(f"   Solo Victories: {self.player_df['solo_victories'].sum()}")
        print(f"   Duo Victories: {self.player_df['duo_victories'].sum()}")

        print("\n👥 BRAWLER STATISTICS:")
        print(f"   Total brawlers: {self.player_df['total_brawlers'].sum()}")
        print(f"   Maxed brawlers: {self.player_df['maxed_brawlers'].sum()}")
        print(
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
        print("=" * 60)
        print("⚔️ BATTLE STATISTICS")
        print("=" * 60)

        total_battles = self.count_total_battles()
        print(f"📊 Total battles collected: {total_battles}")

        if total_battles == 0:
            print("❌ No battles found in the data!")
            return

    def display_battlelog_count_per_player(self) -> None:
        """Display the number of battlelogs per player."""
        print("=" * 60)
        print("📊 BATTLELOG COUNT PER PLAYER")
        print("=" * 60)

        if self.battlelog_df.is_empty():
            print("❌ No battlelog data found!")
            return

        # Count battles per player
        battles_per_player = (
            self.battlelog_df.group_by("player_tag")
            .agg(pl.count().alias("battle_count"))
            .sort("battle_count", descending=True)
        )

        print(f"📈 Total players with battlelog data: {len(battles_per_player)}")

        if len(battles_per_player) == 0:
            print("❌ No battlelog data found!")
            return

        print("\n🏆 TOP PLAYERS BY BATTLE COUNT:")
        for i, row in enumerate(battles_per_player.head(10).iter_rows(named=True), 1):
            print(f"   {i:2d}. {row['player_tag']}: {row['battle_count']} battles")

        print("\n📊 STATISTICS:")
        print(
            f"   Average battles per player: {battles_per_player['battle_count'].mean():.1f}"
        )
        print(f"   Max battles per player: {battles_per_player['battle_count'].max()}")
        print(f"   Min battles per player: {battles_per_player['battle_count'].min()}")

        # Show distribution
        print("\n📈 DISTRIBUTION:")
        print(
            f"   Players with 1-5 battles: {len(battles_per_player.filter(pl.col('battle_count').is_between(1, 5)))}"
        )
        print(
            f"   Players with 6-10 battles: {len(battles_per_player.filter(pl.col('battle_count').is_between(6, 10)))}"
        )
        print(
            f"   Players with 11+ battles: {len(battles_per_player.filter(pl.col('battle_count') >= 11))}"
        )
