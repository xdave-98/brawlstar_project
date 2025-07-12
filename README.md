# Brawl Stars Data Analysis Project

A robust, production-grade data pipeline for Brawl Stars player and battlelog analytics, built with Python, Polars, and Pydantic.  
This project demonstrates best practices in data engineering: modular ETL, type safety, reproducibility, and automated testing.

---

## 🚀 Features

- **Ingestion**: Fetches player and battlelog data from the Brawl Stars API.
- **Validation**: Uses Pydantic models for strict schema validation.
- **Transformation**: Converts JSON to Parquet using Polars for blazing-fast analytics.
- **Modular Pipeline**: Clean separation between ingestion, raw, and processed data.
- **Testing**: Comprehensive unit and integration tests with pytest.
- **Linting & Formatting**: Enforced by Ruff and isort.
- **Pre-commit Hooks**: Ensures code quality and test passing before every commit.

---

## 🗂️ Project Structure

```
brawlstar_project/
├── data/                  # Data lake: ingested, raw, processed, cleaned
├── src/
│   └── brawlstar_project/
│       ├── player/        # Player & battlelog Pydantic models
│       └── processing/    # ETL pipeline: ingested, raw, processed
├── tests/                 # Unit and integration tests
├── Makefile               # Automation commands
├── pyproject.toml         # Dependencies & tool config
├── .pre-commit-config.yaml
└── README.md
```

---

## ⚡ Quickstart

### 1. Install dependencies

```bash
uv pip install -r requirements.txt  # or use `uv sync` if using pyproject.toml
```

### 2. Set up environment

- Copy `.env.example` to `.env` and fill in your Brawl Stars API key.

### 3. Run the pipeline

```bash
make run-player-ingestion   # Fetch player & battlelog data from API
make run-player-raw         # Convert JSON to Parquet
```

### 4. Run tests & lint

```bash
make test
make lint
make format
```

---

## 🧩 Key Technologies

- **Python 3.12+**
- **Polars**: Lightning-fast DataFrame library
- **Pydantic**: Type-safe data validation
- **isort**: Import sorting (Black-compatible)
- **Ruff**: Linting & formatting
- **pytest**: Testing
- **pre-commit**: Git hooks for code quality

---

## 📝 Example Usage

```python
from brawlstar_project.player.player import Player

player = Player("PC0PPLRU")
df = player.load_player_data(Path("data/raw"), days=7)
battlelog_df = player.load_battlelog_data(Path("data/raw"), days=7)
```

---

## 🧪 Testing

- All code is covered by unit and integration tests.
- Run `make test` to execute the full test suite.

---

## 🛠️ Code Quality

- Imports and formatting are enforced by isort and Ruff.
- Pre-commit hooks run tests and lint before every commit.

---

## 📈 Next Steps

- Add Jupyter notebooks for data exploration.
- Build dashboards or reports from Parquet data.
- Schedule the pipeline for regular updates.

---

## License

MIT