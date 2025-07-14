# Brawl Stars Data Analysis Project

A robust, production-grade data pipeline for Brawl Stars player and battlelog analytics, built with Python, Polars, and Pydantic. This project demonstrates best practices in data engineering: modular ETL, type safety, reproducibility, and automated testing.

---

## 🚀 Features

- **Ingestion**: Fetches player and battlelog data from the Brawl Stars API
- **Validation**: Uses Pydantic models for strict schema validation
- **Transformation**: Converts JSON to Parquet using Polars for blazing-fast analytics
- **Modular Pipeline**: Clean separation between ingestion, raw, and processed data
- **Testing**: Comprehensive unit and integration tests with pytest
- **Linting & Formatting**: Enforced by Ruff and isort (88 char line length, Black style)
- **Pre-commit Hooks**: Ensures code quality and test passing before every commit

---

## 🗂️ Project Structure

```
brawlstar_project/
├── data/                          # Data lake (gitignored)
│   ├── ingested/                  # Raw JSON from API
│   │   └── #PLAYER_TAG/
│   │       └── YYYY-MM-DD/
│   │           ├── player.json    # Player profile data
│   │           └── battlelog.json # Battle history data
│   ├── raw/                       # Converted Parquet files
│   │   └── #PLAYER_TAG/
│   │       └── YYYY-MM-DD/
│   │           ├── player.parquet
│   │           └── battlelog.parquet
│   ├── processed/                 # Aggregated/transformed data
│   └── cleaned/                   # Final cleaned datasets
├── src/
│   └── brawlstar_project/
│       ├── player/                # Player data models and utilities
│       │   ├── models/
│       │   │   ├── player.py     # Player data Pydantic models
│       │   │   └── battlelog.py  # Battlelog data Pydantic models
│       │   └── player.py         # Player data loading utilities
│       └── processing/            # ETL pipeline modules
│           ├── ingested/          # Data ingestion from API
│           │   ├── api_client.py  # Brawl Stars API client
│           │   ├── config.py      # API configuration
│           │   └── main.py        # Ingestion orchestration
│           ├── raw/               # JSON to Parquet conversion
│           │   └── main.py        # Conversion orchestration
│           ├── processed/         # Data processing and analysis
│           │   ├── analysis.py    # Data analysis functions
│           │   └── main.py        # Processing orchestration
│           ├── cleaned/           # Data cleaning utilities
│           └── utils.py           # Shared ETL utilities
├── tests/                         # Unit and integration tests
├── Makefile                       # Automation commands
├── pyproject.toml                 # Dependencies & tool config
├── .pre-commit-config.yaml        # Pre-commit hooks
└── README.md
```

---

## 📊 Data Structure

### Data Lake Organization

The project follows a data lake pattern with clear separation of concerns:

#### 1. **Ingested Data** (`data/ingested/`)
- **Location**: `data/ingested/#PLAYER_TAG/YYYY-MM-DD/`
- **Format**: JSON files with validated schema
- **Files**:
  - `player.json`: Player profile and statistics
  - `battlelog.json`: Battle history and match details

#### 2. **Raw Data** (`data/raw/`)
- **Location**: `data/raw/#PLAYER_TAG/YYYY-MM-DD/`
- **Format**: Parquet files optimized for analytics
- **Files**:
  - `player.parquet`: Flattened player data
  - `battlelog.parquet`: Normalized battle records

#### 3. **Processed Data** (`data/processed/`)
- **Purpose**: Aggregated metrics and derived features
- **Format**: Parquet files with calculated statistics

#### 4. **Cleaned Data** (`data/cleaned/`)
- **Purpose**: Final datasets ready for analysis
- **Format**: Parquet files with quality-assured data

### Data Flow

```
API Request → JSON Validation → Parquet Conversion → Processing → Analysis
     ↓              ↓                    ↓              ↓           ↓
  ingested/     Pydantic           raw/          processed/    cleaned/
```

---

## ⚡ Quickstart

### 1. Prerequisites

- Python 3.12+
- UV package manager
- Brawl Stars API key

### 2. Install dependencies

```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 3. Set up environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your API key
# BRAWLSTARS_API_KEY=your_api_key_here
```

### 4. Run the pipeline

```bash
# Fetch player & battlelog data from API
make run-player-ingestion PLAYER_TAG=#YOURTAG

# Convert JSON to Parquet
make run-player-raw

# Display processed data
make run-player-processed PLAYER_TAG=#YOURTAG
```

### 5. Run tests & lint

```bash
make test
make lint
make format
```

---

## 📊 Streamlit App Data Source Selection

The Streamlit dashboard automatically selects the data source based on your environment:

- **Local Development:**
  - By default, uses your local cleaned data (`data/cleaned/`).
  - To override, set the `BRAWLSTARS_DATA_ROOT` environment variable (e.g., in `.env.local`) to your preferred data directory.

- **Streamlit Cloud:**
  - Automatically uses the sample data in `data/sample/` (included in the repo) for demo/testing.

### How it works

The app uses a utility function (`get_data_root()`) to determine the data directory:

1. If `BRAWLSTARS_DATA_ROOT` is set, that directory is used.
2. If running on Streamlit Cloud, `data/sample/` is used.
3. Otherwise, defaults to `data/cleaned/`.

**Example for local override:**

Add to your `.env.local`:
```
BRAWLSTARS_DATA_ROOT=data/cleaned
```

**No changes are needed for Streamlit Cloud deployments.**

---

## 🧩 Key Technologies

- **Python 3.12+**: Modern Python with type hints
- **Polars**: Lightning-fast DataFrame library
- **Pydantic**: Type-safe data validation
- **UV**: Fast Python package manager
- **isort**: Import sorting (Black-compatible)
- **Ruff**: Linting & formatting
- **pytest**: Testing framework
- **pre-commit**: Git hooks for code quality

---

## 📝 Example Usage

### Basic Data Loading

```python
from brawlstar_project.player.player import Player

# Load player data
player = Player("#PC0PPLRU")
player_data = player.load_player_data(Path("data/raw"), days=7)
battlelog_data = player.load_battlelog_data(Path("data/raw"), days=7)
```

### Custom Analysis

```python
from brawlstar_project.processing.processed.analysis import analyze_player_performance

# Analyze player performance over time
performance_df = analyze_player_performance("data/raw", "#PC0PPLRU", days=30)
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

### Test Structure

- **Unit Tests**: Test individual functions and classes
- **Integration Tests**: Test data pipeline end-to-end
- **Model Tests**: Validate Pydantic model behavior

---

## 🛠️ Code Quality

### Linting & Formatting

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

### Pre-commit Hooks

The project uses pre-commit hooks to ensure code quality:

- **Ruff**: Linting and formatting
- **isort**: Import sorting
- **pytest**: Test execution
- **Black-compatible**: 88-character line length

---

## 🔧 Development Setup

### 1. Clone and setup

```bash
git clone <your-repo>
cd brawlstar_project
uv sync
```

### 2. Install pre-commit hooks

```bash
pre-commit install
```

### 3. Create environment file

```bash
cp .env.example .env
# Edit .env with your API key
```

### 4. Run initial pipeline

```bash
make run-player-ingestion PLAYER_TAG=#YOURTAG
make run-player-raw
```

---

## 📈 Data Analysis Examples

### Player Statistics

```python
import polars as pl
from pathlib import Path

# Load player data
df = pl.read_parquet("data/raw/#PC0PPLRU/2025-01-15/player.parquet")

# Basic statistics
print(df.describe())

# Trophy progression
trophy_stats = df.select([
    "name", "trophies", "highestTrophies", "powerLeaguePoints"
])
```

### Battle Analysis

```python
# Load battlelog data
battle_df = pl.read_parquet("data/raw/#PC0PPLRU/2025-01-15/battlelog.parquet")

# Battle statistics
battle_stats = battle_df.group_by("mode").agg([
    pl.count().alias("total_battles"),
    pl.col("result").value_counts().alias("win_loss")
])
```

---

## 🚀 Next Steps

- [ ] Add Jupyter notebooks for data exploration
- [ ] Build dashboards with Streamlit or Plotly
- [ ] Implement automated data quality checks
- [ ] Add more comprehensive battle analysis
- [ ] Create scheduled data pipeline with cron/Airflow
- [ ] Add data visualization components

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Run linting: `make lint`
6. Submit a pull request

---

## 📄 License

MIT License - see LICENSE file for details

---

## 📞 Support

For questions or issues:
- Open an issue on GitHub
- Check the documentation in the `docs/` folder
- Review the test examples for usage patterns