# ============================================================================
# Configuration
# ============================================================================

# Default player tag (you should put your own player tag here)
DEFAULT_PLAYER_TAG = "PC0PPLRU"

# Default club tag (you should put your own club tag here)
DEFAULT_CLUB_TAG = "80Y22P29J"

# Default date (today)
DEFAULT_DATE = $(shell date +%Y-%m-%d)

# Use PLAYER_TAG from command line if defined, else use default
PLAYER_TAG ?= $(DEFAULT_PLAYER_TAG)

# Use CLUB_TAG from command line if defined, else use default
CLUB_TAG ?= $(DEFAULT_CLUB_TAG)

# Use DATE from command line if defined, else use default
DATE ?= $(DEFAULT_DATE)

# ============================================================================
# Data Pipeline Commands
# ============================================================================

# Ingestion Commands
run-player-ingestion:
	@echo "🔷 Ingest player data for tag: $(PLAYER_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py --mode player --tag $(PLAYER_TAG)

run-club-ingestion:
	@echo "🔷 Ingest club data for tag: $(CLUB_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py --mode club --tag $(CLUB_TAG)

run-club-players-ingestion:
	@echo "🔷 Ingest club data and ALL its members' data for tag: $(CLUB_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py --mode club-players --tag $(CLUB_TAG)

# Conversion Command
run-raw-conversion:
	@echo "🔷 Convert JSON files to Parquet"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

# Analysis Commands
run-single-player-analysis:
	@echo "🔷 Run single player analysis"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --mode single-player --player-tag $(PLAYER_TAG) --data-dir data

run-all-players-analysis:
	@echo "🔷 Run analysis on all available players"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --mode all-players --data-dir data

run-club-analysis:
	@echo "🔷 Run club analysis"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --mode club --club-tag $(CLUB_TAG) --data-dir data

# Process (Cleaning) Command
run-process:
	@echo "🔷 Run batch processing (cleaning) for mode: $(MODE) and date: $(DATE)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --mode $(MODE) --date $(DATE)

# Gold Layer (Cleaned) Command
run-gold:
	@echo "🏆 Run gold layer processing for date: $(DATE)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/cleaned/main.py --date $(DATE)

# Analytics Commands
run-duckdb-queries:
	@echo "📊 Running DuckDB analytics queries..."
	PYTHONPATH=src uv run python src/brawlstar_project/analytics/duckdb_queries.py

# ============================================================================
# Complete Pipeline Commands
# ============================================================================

run-player-pipeline:
	@echo "👤 Complete player pipeline: ingestion + conversion + analysis"
	@echo "📥 Step 1: Ingest player data..."
	$(MAKE) run-player-ingestion
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-conversion
	@echo "📊 Step 3: Analyze data..."
	$(MAKE) run-single-player-analysis

run-club-pipeline:
	@echo "🏛️ Complete club pipeline: ingestion + conversion + analysis"
	@echo "📥 Step 1: Ingest club data..."
	$(MAKE) run-club-ingestion
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-conversion
	@echo "📊 Step 3: Analyze data..."
	$(MAKE) run-club-analysis

run-club-players-pipeline:
	@echo "🚀 Complete club-players pipeline: ingestion + conversion + analysis"
	@echo "📥 Step 1: Ingest club and all members' data..."
	$(MAKE) run-club-players-ingestion
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-conversion
	@echo "📊 Step 3: Analyze all players..."
	$(MAKE) run-all-players-analysis

# =========================================================================
# Streamlit App Command
# =========================================================================

run-streamlit:
	@echo "🚀 Running Streamlit app..."
	PYTHONPATH=src streamlit run streamlit_app/main.py

# ============================================================================
# Development Commands
# ============================================================================

# Testing
test:
	@echo "✅ Running all tests..."
	PYTHONPATH=src uv run pytest

run-test: test

test-pydantic:
	@echo "✅ Running Pydantic model tests..."
	PYTHONPATH=src uv run pytest tests/test_pydantic_models.py -v

test-coverage:
	@echo "✅ Running tests with coverage..."
	PYTHONPATH=src uv run pytest --cov=src/brawlstar_project --cov-report=html --cov-report=term

# Code Quality
lint:
	@echo "🔍 Running Ruff checks..."
	uv run ruff check .

fix:
	@echo "🛠️  Fixing lint issues with Ruff..."
	uv run ruff check . --fix

format:
	@echo "🎨 Formatting code with Ruff and isort..."
	uv run ruff format .
	uv run isort .

# ============================================================================
# Cleanup Commands
# ============================================================================

clean:
	@echo "🧹 Cleaning __pycache__ and ruff cache..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache

clean-data:
	@echo "🗑️  Cleaning all data directories..."
	rm -rf data/ingested data/raw data/processed

clean-ingested:
	@echo "🗑️  Cleaning ingested data..."
	rm -rf data/ingested

clean-raw:
	@echo "🗑️  Cleaning raw data..."
	rm -rf data/raw

clean-processed:
	@echo "🗑️  Cleaning processed data..."
	rm -rf data/processed

clean-all: clean clean-data
	@echo "🧹 Complete cleanup done!"

# ============================================================================
# Help
# ============================================================================

help:
	@echo "📋 Available commands:"
	@echo ""
	@echo "🔷 Data Pipeline:"
	@echo "  run-player-ingestion      - Ingest single player data"
	@echo "  run-club-ingestion        - Ingest club data"
	@echo "  run-club-players-ingestion - Ingest club + all members"
	@echo "  run-raw-conversion        - Convert JSON to Parquet"
	@echo "  run-single-player-analysis - Analyze single player"
	@echo "  run-all-players-analysis  - Analyze all players"
	@echo "  run-club-analysis         - Analyze club data"
	@echo "  run-process              - Batch process (clean/transform) mode (MODE, DATE required)"
	@echo "  run-gold                 - Gold layer processing (DATE required)"
	@echo "  run-duckdb-queries       - Run DuckDB analytics queries"
	@echo ""
	@echo "🚀 Complete Pipelines:"
	@echo "  run-player-pipeline       - Complete player pipeline"
	@echo "  run-club-pipeline         - Complete club pipeline"
	@echo "  run-club-players-pipeline - Complete club-players pipeline"
	@echo ""
	@echo "🛠️  Development:"
	@echo "  test                      - Run all tests"
	@echo "  lint                      - Run linting"
	@echo "  format                    - Format code"
	@echo "  clean                     - Clean cache files"
	@echo "  clean-data                - Clean all data"
	@echo ""
	@echo "🌐 Streamlit:"
	@echo "  run-streamlit             - Run the Streamlit dashboard app"

.PHONY: help test lint fix format clean clean-data clean-ingested clean-raw clean-processed clean-all run-player-pipeline run-club-pipeline run-club-players-pipeline
