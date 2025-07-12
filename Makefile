# ============================================================================
# Configuration
# ============================================================================

# Default player tag (you should put your own player tag here)
DEFAULT_PLAYER_TAG = "PC0PPLRU"

# Default club tag (you should put your own club tag here)
DEFAULT_CLUB_TAG = "80Y22P29J"

# Use PLAYER_TAG from command line if defined, else use default
PLAYER_TAG ?= $(DEFAULT_PLAYER_TAG)

# Use CLUB_TAG from command line if defined, else use default
CLUB_TAG ?= $(DEFAULT_CLUB_TAG)

# ============================================================================
# ETL Pipeline Commands
# ============================================================================

run-player-ingestion-stage:
	@echo "🔷 Ingest player data for tag: $(PLAYER_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/pipeline_main.py --mode player --tag $(PLAYER_TAG)

run-club-ingestion-stage:
	@echo "🔷 Ingest club data for tag: $(CLUB_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/pipeline_main.py --mode club --tag $(CLUB_TAG)

run-club-players-ingestion-stage:
	@echo "🔷 Ingest club data and ALL its members' data for tag: $(CLUB_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/pipeline_main.py --mode club-players --tag $(CLUB_TAG)

run-raw-stage:
	@echo "🔷 Convert JSON files to Parquet"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

run-player-processed:
	@echo "🔷 Display processed player data"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --player-tag $(PLAYER_TAG)

run-processed-stage:
	@echo "🔷 Show all processed data (player and club)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --player-tag $(PLAYER_TAG) --club-tag $(CLUB_TAG)



# Complete pipeline commands
run-player-pipeline:
	@echo "👤 Complete player pipeline: ingestion + raw + processed"
	@echo "📥 Step 1: Ingest player data..."
	$(MAKE) run-player-ingestion-stage
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-stage
	@echo "📊 Step 3: Show processed data..."
	$(MAKE) run-processed-stage

run-club-pipeline:
	@echo "🏛️ Complete club pipeline: ingestion + raw + processed"
	@echo "📥 Step 1: Ingest club data..."
	$(MAKE) run-club-ingestion-stage
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-stage
	@echo "📊 Step 3: Show processed data..."
	$(MAKE) run-processed-stage

run-club-players-pipeline:
	@echo "🚀 Complete club-players pipeline: ingestion + raw + processed"
	@echo "📥 Step 1: Ingest club and all members' data..."
	$(MAKE) run-club-players-ingestion-stage
	@echo "🔄 Step 2: Convert to Parquet..."
	$(MAKE) run-raw-stage
	@echo "📊 Step 3: Show processed data..."
	$(MAKE) run-processed-stage

# ============================================================================
# Tests
# ============================================================================

test:
	@echo "✅ Running all tests..."
	PYTHONPATH=src uv run pytest

test-pydantic:
	@echo "✅ Running Pydantic model tests..."
	PYTHONPATH=src uv run pytest tests/test_pydantic_models.py -v

test-coverage:
	@echo "✅ Running tests with coverage..."
	PYTHONPATH=src uv run pytest --cov=src/brawlstar_project --cov-report=html --cov-report=term

run-test: test

# ============================================================================
# Linting & Formatting
# ============================================================================

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

sort-imports:
	@echo "🔃 Sorting imports..."
	uv run isort .

# ============================================================================
# Clean up
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

clean-player-data:
	@echo "🗑️  Cleaning player data for tag: $(PLAYER_TAG)"
	rm -rf data/ingested/$(PLAYER_TAG) data/raw/*/$(PLAYER_TAG)

clean-club-data:
	@echo "🗑️  Cleaning club data for tag: $(CLUB_TAG)"
	rm -rf data/ingested/$(CLUB_TAG) data/raw/*/$(CLUB_TAG)

clean-all: clean clean-data
	@echo "🧹 Complete cleanup done!"

.PHONY: run-player-ingestion run-player-raw run-player-processed test lint fix format clean clean-data clean-ingested clean-raw clean-processed clean-player-data clean-club-data clean-all run-player-pipeline run-club-pipeline
