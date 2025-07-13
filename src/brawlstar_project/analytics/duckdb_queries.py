import duckdb

con = duckdb.connect()

# Test query to check available data
print("🔍 Checking available data...")
df = con.execute("""
    SELECT * FROM 'data/cleaned/fact_matches/2025-07-14/fact_matches.parquet' 
    LIMIT 5
""").df()

print("📊 Sample data from fact_matches:")
print(df)
print("\n" + "="*50 + "\n")

# Simple analytics query: count matches per day per player, ordered by total_matches (using CTE)
print("📈 Running simple analytics query with CTE...")
analytics_df = con.execute("""
    WITH match_counts AS (
        SELECT 
            battle_time_date, 
            player_tag,
            COUNT(*) AS total_matches
        FROM 'data/cleaned/fact_matches/2025-07-14/fact_matches.parquet'
        GROUP BY battle_time_date, player_tag
    )
    SELECT *
    FROM match_counts
    ORDER BY total_matches DESC, battle_time_date, player_tag
""").df()

print("📊 Matches per day per player (ordered by total_matches):")
print(analytics_df)
