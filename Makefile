# ============================================================================
# Configuration
# ============================================================================

# Default player tag (you should put your own player tag here)
DEFAULT_PLAYER_TAG = "PC0PPLRU"

# Use PLAYER_TAG from command line if defined, else use default
PLAYER_TAG ?= $(DEFAULT_PLAYER_TAG)

# ============================================================================
# ETL Pipeline Commands
# ============================================================================

run-player-ingestion:
	@echo "🔷 Ingest player data for tag: $(PLAYER_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py $(PLAYER_TAG)

run-player-raw:
	@echo "🔷 Convert JSON files to Parquet"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

run-player-processed:
	@echo "🔷 Display processed player data"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --player-tag $(PLAYER_TAG)

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

.PHONY: run-player-ingestion run-player-raw run-player-processed test lint fix format clean
