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
        print(f"   3v3 Victories: {self.player_df['3vs3Victories'].sum()}")
        print(f"   Solo Victories: {self.player_df['soloVictories'].sum()}")
        print(f"   Duo Victories: {self.player_df['duoVictories'].sum()}")

        print("\n👥 BRAWLER STATISTICS:")
        print(f"   Total brawlers: {self.player_df['total_brawlers'].sum()}")
        print(f"   Maxed brawlers: {self.player_df['maxed_brawlers'].sum()}")
        print(
            f"   Average brawler trophies: {self.player_df['total_brawler_trophies'].mean():.0f}"
        )
