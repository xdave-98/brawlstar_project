# Sample Data for Brawl Stars Analytics

This folder contains small, non-sensitive sample datasets for demo and testing purposes. These files allow you to run the Streamlit dashboard and pipeline without needing to ingest real Brawl Stars data.

## Directory Structure

Sample data is organized to mirror the production/cleaned data structure, with each table partitioned by date:

```
data/sample/
  dim_players/2025-07-14/dim_players.parquet
  dim_clubs/2025-07-14/dim_clubs.parquet
  dim_maps/2025-07-14/dim_maps.parquet
  dim_game_modes/2025-07-14/dim_game_modes.parquet
  fact_matches/2025-07-14/fact_matches.parquet
```

## Files & Schemas

- **dim_players.parquet**
  - `tag` (str): Unique player tag
  - `name` (str): Player name
  - `trophies` (int): Current trophy count
  - `exp_level` (int): Experience level
  - `club_tag` (str, nullable): Club tag if player is in a club

- **dim_clubs.parquet**
  - `tag` (str): Unique club tag
  - `name` (str): Club name
  - `members_count` (int): Number of members
  - `trophies` (int): Total club trophies

- **dim_maps.parquet**
  - `map_id` (int): Unique map identifier
  - `name` (str): Map name
  - `game_mode` (str): Game mode for the map

- **dim_game_modes.parquet**
  - `mode_id` (int): Unique mode identifier
  - `name` (str): Game mode name
  - `description` (str): Description of the mode

- **fact_matches.parquet**
  - `match_id` (int): Unique match identifier
  - `timestamp` (str): ISO timestamp
  - `map_id` (int): Map played
  - `mode_id` (int): Game mode played
  - `winner_tag` (str): Winning player tag
  - `duration` (int): Match duration (seconds)

## Usage

- These files are used for demo/testing in local and Streamlit Cloud environments.
- The sample data is tracked in git for easy onboarding.
- The pipeline and Streamlit app will automatically detect and use this data if present.