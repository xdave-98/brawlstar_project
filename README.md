![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/github/license/xdave-98/brawlstar_project)
![Build](https://github.com/xdave-98/brawlstar_project/actions/workflows/ci.yml/badge.svg)
![Portfolio](https://img.shields.io/badge/portfolio-project-orange)

# Brawl Stars Data Engineering Project

A robust, production-grade data pipeline and analytics dashboard for Brawl Stars, built to demonstrate advanced data engineering skills using Python, Polars, DuckDB, and modern best practices.

---

## 🏆 What is Brawl Stars?

[Brawl Stars](https://supercell.com/en/games/brawlstars/) is a popular real-time multiplayer mobile game by Supercell. Players compete in various game modes, collect characters ("Brawlers"), and join clubs for team-based play.

The game has two main components:
- **Players**: Each player is identified by a unique player tag (ID) and has associated stats, progression, and battle history.
- **Clubs**: Social groups of players, each identified by a unique club tag (ID), with their own stats, membership, and activity.

---

## ℹ️ About This Project

This project is **personal** and designed to showcase my expertise in both:

- **Data Engineering**: End-to-end pipeline (ingestion, validation, transformation, analytics, dashboard), dimensional modeling, analytics, and scalable architecture. The pipeline is organized using a **medallion architecture**—where `raw` = **bronze**, `processed` = **silver**, and `cleaned` = **gold**—to ensure data quality and clear separation of concerns at each stage.
- **Programming & Software Engineering**: Modern Python, OOP, Factory/Design patterns, code hierarchy, reproducibility, automated testing, and best practices for maintainable, production-grade code. 
The codebase is structured to follow strong software engineering principles, including the SOLID principles, to ensure clarity, extensibility, and maintainability.

> **Note:** This project is **not intended for production use**. Its primary goal is to demonstrate my strengths and approach as a data engineer developer. I am fully aware that many aspects can be improved or made more robust (see the TODO/Must-Have Improvements section below), but I am proud of this solid foundation and the learning it represents.

The goal is to demonstrate not only my ability to build robust data pipelines and analytics, but also my commitment to clean, extensible, and reproducible codebases.

> **Questions or feedback?** Feel free to open a discussion in the [GitHub Discussions](https://github.com/xdave-98/brawlstar_project/discussions) section, or contact me directly.

---

## 📦 Data Sources

- **Brawl Stars API**: Player profiles, battle logs, club data
  - Official API documentation: [https://developer.brawlstars.com/](https://developer.brawlstars.com/)
  - You must register for a free API token to access the data. The API provides endpoints for player stats, battle logs, club information, rankings, and more.
- **Data Lake Structure**: See [`data/sample/README.md`](data/sample/README.md) for full details

---

## 🗂️ Project Structure

```
brawlstar_project/
├── data/                        # Data lake (see data/sample/README.md for schema)
│   ├── ingested/                # Raw JSON from Brawl Stars API
│   ├── raw/                     # Parquet-converted raw data
│   ├── processed/               # Aggregated/derived features
│   ├── cleaned/                 # Final analytics-ready datasets
│   └── sample/                  # Sample data for demo/testing
├── src/
│   └── brawlstar_project/
│       ├── analytics/           # Analytical SQL queries (DuckDB)
│       │   ├── club_queries.py
│       │   ├── global_queries.py
│       │   ├── player_queries.py
│       │   └── duckdb_utils.py
│       ├── constants/           # Project-wide constants (paths, config)
│       ├── entities/            # Data models and domain entities
│       │   ├── player/
│       │   │   ├── player.py            # Player data utilities
│       │   │   └── models/
│       │   │       ├── player.py        # Player Pydantic model
│       │   │       └── battlelog.py     # Battlelog Pydantic model
│       │   ├── club/
│       │   │   ├── club.py              # Club data utilities
│       │   │   └── models/
│       │   │       ├── club.py          # Club Pydantic model
│       │   │       └── members.py       # Club members Pydantic model
│       │   └── tag_entity.py            # Tag entity abstraction
│       ├── processing/          # ETL pipeline modules
│       │   ├── ingested/        # Data ingestion from API
│       │   │   ├── api_client.py
│       │   │   ├── config.py
│       │   │   └── main.py
│       │   ├── raw/             # JSON to Parquet conversion
│       │   │   └── main.py
│       │   ├── processed/       # Data processing and aggregation
│       │   │   └── main.py
│       │   ├── cleaned/         # Data cleaning and final tables
│       │   │   ├── fact_matches.py
│       │   │   ├── dim_clubs.py
│       │   │   ├── dim_game_modes.py
│       │   │   ├── dim_maps.py
│       │   │   ├── dim_players.py
│       │   │   ├── main.py
│       │   │   └── base_dimension_processor.py
│       │   ├── factory/         # (If present) Factory pattern for pipeline orchestration
│       │   └── utils/           # Shared ETL utilities
│       │       ├── json_utils.py
│       │       └── config_utils.py
├── streamlit_app/               # Streamlit dashboard app
│   └── main.py
├── tests/                       # Unit and integration tests
├── .pre-commit-config.yaml      # Pre-commit hooks for linting/testing
├── pyproject.toml               # Project dependencies and tool config
└── README.md                    # Project documentation
```

**Notes:**
- All data model schemas and sample data structure are described in [`data/sample/README.md`](data/sample/README.md).
- The `src/brawlstar_project/processing` directory is organized by ETL stage: `ingested` (raw API), `raw` (conversion), `processed` (aggregation), `cleaned` (final analytics tables).
- The `entities` directory contains all Pydantic models and domain logic for players, clubs, and tags.
- The `analytics` directory contains all analytical SQL queries, organized by domain (player, club, global).
- The `streamlit_app` directory contains the dashboard code.

---

## 🔄 Data Flow

```
API Request → JSON Validation (Pydantic) → Parquet Conversion → Processing → Analysis
     ↓                ↓                        ↓                ↓           ↓
  ingested/       Pydantic               raw/            processed/    cleaned/
```

- **API Request**: Fetch data from the Brawl Stars API
- **JSON Validation (Pydantic)**: Validate and parse raw JSON using Pydantic models
- **Parquet Conversion**: Store validated data as Parquet files for efficient analytics
- **Processing**: Aggregate, join, and transform data for analytics
- **Analysis**: Final cleaned datasets ready for dashboarding and exploration

---

## 🗃️ Data Modeling

The project uses a **star schema** (star model) for analytics, with the following key tables:

- **dim_players**: Player attributes (tag, name, club, etc.)
- **dim_clubs**: Club attributes (tag, name, members, etc.)
- **fact_matches**: Match-level facts (player, club, mode, result, timestamp, etc.)

All models are defined using Pydantic for type safety and validation.  
See [`data/sample/README.md`](data/sample/README.md) for detailed schema and data structure.

---

## 📓 Notebooks

Jupyter notebooks are included in the project to help you understand the data and explore it interactively. You can use these notebooks to:
- Inspect and visualize the raw, processed, or cleaned data
- Prototype new analyses or transformations
- Experiment with feature engineering or custom metrics

Feel free to run and modify the notebooks for your own exploration or to extend the project further.

---

## ⚡ Quickstart

Follow these steps to install, configure, and run the project—either locally with your own data, or on Streamlit Cloud with sample data.

### 1. Prerequisites
- Python 3.12+
- [UV](https://github.com/astral-sh/uv) package manager
- Brawl Stars API key (register at [https://developer.brawlstars.com/](https://developer.brawlstars.com/))

### 2. Install dependencies
```bash
uv sync
```

### 3. Configure your environment
```bash
cp .env.example .env
# Edit .env to add your BRAWLSTARS_API_KEY and (optionally) set BRAWLSTARS_DATA_ROOT to your data directory
```

### 4. Ingest and process your own data (local mode)

- For most users, you should use the Makefile targets:
  ```bash
  make run-unified-pipeline PLAYER_TAG=#YOURTAG
  make run-streamlit
  ```
  This will run the full pipeline and launch the dashboard.

- The other stage-specific targets (like `make run-ingested`, `make run-player-raw`, etc.) are available if you want to run or debug dedicated parts of the workflow.
- See the `Makefile` for a full list of available commands and options.

### 5. Run the Streamlit dashboard
- **Locally with your data:**
  ```bash
  make run-streamlit
  ```
  The app will use your local cleaned data (`data/cleaned/`) by default, or the directory specified in your `.env` as `BRAWLSTARS_DATA_ROOT`.

- **On Streamlit Cloud:**
  - The app will automatically use the fixed sample data in `data/sample/` (no API key or custom config needed).
  - This is ideal for demo/testing or if you want to see the dashboard without ingesting your own data.

### 6. Load your own data/config
- To use your own data, set the `BRAWLSTARS_DATA_ROOT` variable in your `.env` to point to your data directory (e.g., `data/cleaned/`).
- You can customize the ingestion and processing by editing the config files in `src/brawlstar_project/processing/ingested/config.py` or by extending the pipeline.

### 7. Test & Lint
```bash
make test
make lint
make format
```

---

## 🧪 Testing

The project includes comprehensive testing:

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test categories
make test-pydantic
```

**Test Structure:**
- **Unit Tests:** Test individual functions and classes
- **Integration Tests:** Test data pipeline end-to-end
- **Model Tests:** Validate Pydantic model behavior

---

## 🛠️ Code Quality

**Linting & Formatting**
```bash
# Check code quality
make lint

# Auto-fix issues
make fix

# Format code
make format

# Sort imports
make sort-imports
```

**Pre-commit Hooks:**  
The project uses pre-commit hooks to ensure code quality:
- **Ruff:** Linting and formatting
- **isort:** Import sorting
- **pytest:** Test execution
- **Black-compatible:** 88-character line length

*See the Makefile for all available commands.*

---

## 🚦 Streamlit App Data Source

The dashboard auto-selects the data source:
- **Local**: Uses your local cleaned data (`data/cleaned/`)
- **Streamlit Cloud**: Uses sample data (`data/sample/`)
- Override with `BRAWLSTARS_DATA_ROOT` in your `.env` if needed.

> **Note for Streamlit Cloud users:** The sample data provided is static and intended for demonstration purposes only. Do not expect fresh or up-to-date data when running the app on Streamlit Cloud.

---

## 🧩 Key Technologies

- **Python 3.12+**
- **Polars**: Fast DataFrame analytics; used for all data transformation, cleaning, and Parquet file operations throughout the ETL pipeline.
- **DuckDB**: Embedded analytics database; used for efficient SQL analytics and dashboard queries (via the analytics/ modules and Streamlit app).
- **Pydantic**: Data validation; used for strict schema validation and parsing of all ingested JSON data from the Brawl Stars API.
- **UV**: Modern Python package and environment manager; used for reproducible, fast dependency management and environment setup.
- **Streamlit**: Interactive dashboard for data exploration and analytics.
- **Ruff, isort**: Linting & formatting for code quality and consistency.
- **pytest**: Testing framework for unit and integration tests.
- **pre-commit**: Code quality automation (linting, formatting, and tests before every commit).

---

## 🚧 TODO / Must-Have Improvements

### Quick Wins & Easy Improvements
- Continue to clean and enrich the data (e.g., add support for 'tie' results, not just win/loss)
- Collect and store additional features (e.g., trophyChange per match, more granular player stats)
- Add more unit tests (current tests are for demo purposes and not exhaustive)
- Implement a raw masking strategy for player or club tags that contain irrelevant or sensitive properties
- Add more visualizations and metrics to the Streamlit dashboard
- Improve error handling and logging throughout the pipeline
- Refactor and document code for even greater clarity and maintainability

### Advanced & Industrial-Scale Improvements

- **Data Source & Access**
  - Ideally, contact Supercell to explore access to a premium API (with fewer constraints) or direct connection to their flat/raw data. This would enable more robust, scalable, and real-time data engineering workflows.

- **Cloud & Scalability**
  - Enable cloud scalability (deploy on GCP, AWS or Azure)
  - Use Apache Iceberg for open table format, enabling efficient historical data management and time travel queries

- **Orchestration & Automation**
  - Orchestrate the pipeline with Airflow to manage dependencies and scheduling between medallion layers
  - Build a full CI/CD pipeline for automated testing, deployment, and monitoring

- **Real-Time & Streaming**
  - Integrate Cloud Pub/Sub (or Kafka) for real-time event ingestion
  - Add Flink or Spark Streaming for real-time/near-real-time data processing

- **Batch & Distributed Processing**
  - Use Spark for large-scale batch processing and distributed analytics at scale

- **Data Modeling & History**
  - Implement Change Data Capture (CDC) for incremental updates
  - Support Slowly Changing Dimensions (SCD):
    - **Type 2** for full historical tracking (each change creates a new record)
    - **Type 3** for partial history (track previous and current values)

- **Monitoring & Analytics**
  - Implement robust monitoring, alerting, and data quality checks
  - Add advanced analytics (e.g., churn prediction, player segmentation, anomaly detection)

---