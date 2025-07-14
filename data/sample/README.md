# Sample Data for Brawl Stars Analytics

This folder contains small, non-sensitive sample datasets for demo and testing purposes. These files allow you to run the Streamlit dashboard and pipeline without needing to ingest real Brawl Stars data.

## Directory Structure

Sample data is organized to mirror the production/cleaned data structure. Each table is stored as a single Parquet file:

```
data/sample/
  dim_players.parquet
  dim_clubs.parquet
  dim_maps.parquet
  dim_game_modes.parquet
  fact_matches.parquet
```

## Files & Schemas

- **dim_players.parquet**
  - `tag` (str): Unique player tag
  - `name` (str): Player name
  - `club_tag` (str, nullable): Club tag if player is in a club
  - `club_role` (str, nullable): Player's role in the club (if any)
  - `trophies` (int): Current trophy count
  - `highest_trophies` (int): Highest trophy count achieved
  - `exp_level` (int): Experience level
  - `exp_points` (int): Experience points
  - `_process_date` (date): Date the record was processed

- **dim_clubs.parquet**
  - `tag` (str): Unique club tag
  - `name` (str): Club name
  - `description` (str, nullable): Club description
  - `trophies` (int): Total club trophies
  - `required_trophies` (int): Trophies required to join
  - `member_count` (int): Number of members
  - `_process_date` (date): Date the record was processed

- **dim_maps.parquet**
  - `map_name` (str): Name of the map
  - `_process_date` (date): Date the record was processed

- **dim_game_modes.parquet**
  - `battle_mode` (str): Name of the game mode
  - `_process_date` (date): Date the record was processed

- **fact_matches.parquet**
  - `match_id` (str): Unique match identifier - <player_tag>-<battle_time>-<map_name>
  - `battle_time` (datetime): Timestamp of the match
  - `battle_time_date` (date): Date of the match
  - `player_tag` (str): Player tag
  - `club_tag` (str, nullable): Club tag (if any)
  - `map_name` (str): Name of the map played
  - `battle_mode` (str): Game mode
  - `battle_result` (str): Result of the match (e.g., victory, defeat)
  - `_process_date` (date): Date the record was processed

## Usage

- These files are used for demo/testing in local and Streamlit Cloud environments.
- The sample data is tracked in git for easy onboarding.
- The pipeline and Streamlit app will automatically detect and use this data if present.