# ============================================================================
# Development & Utility Commands
# ============================================================================

# ============================================================================
# Stage-specific commands (for development or future step-by-step orchestration only)
# You should NOT use the run-<stage> commands except for dev purpose or
# further orchestration by stage step (e.g., through Airflow orchestration)
# ============================================================================

run-ingested:
	@echo "🚀 Running ingestion for all tags in config.yaml (mode: club-players)..."
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py --mode club-players

run-raw:
	@echo "🚀 Running raw stage: converting all ingested JSON to Parquet..."
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

run-processed:
	@echo "🚀 Running processed stage: cleaning and processing silver data for all entities (today)..."
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --mode all --date $(shell date +%Y-%m-%d)

run-cleaned:
	@echo "🚀 Running cleaned stage: processing gold layer for today..."
	PYTHONPATH=src uv run python src/brawlstar_project/processing/cleaned/main.py --date $(shell date +%Y-%m-%d)

# ============================================================================
# Main commands for data processing and dashboard
# These are the two commands to use for processing data:
# - run-unified-pipeline: full batch pipeline for all players and clubs in config.yaml
# - run-streamlit: launch the Streamlit dashboard
# ============================================================================

run-unified-pipeline:
	@echo "🚀 Running unified batch pipeline for all players and clubs in config.yaml..."
	PYTHONPATH=src uv run python src/brawlstar_project/processing/unified_main.py

run-streamlit:
	@echo "🚀 Running Streamlit app..."
	PYTHONPATH=src streamlit run streamlit_app/main.py

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

# Cleanup

clean:
	@echo "🧹 Cleaning __pycache__ and ruff cache..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache

clean-data:
	@echo "🗑️  Cleaning all data directories..."
	rm -rf data/ingested data/raw data/processed data/cleaned

clean-ingested:
	@echo "🗑️  Cleaning ingested data..."
	rm -rf data/ingested

clean-raw:
	@echo "🗑️  Cleaning raw data..."
	rm -rf data/raw

clean-processed:
	@echo "🗑️  Cleaning processed data..."
	rm -rf data/processed

clean-cleaned:
	@echo "🗑️  Cleaning cleaned data..."
	rm -rf data/cleaned

clean-all: clean clean-data
	@echo "🧹 Complete cleanup done!"

# Help

help:
	@echo "📋 Available commands:"
	@echo ""
	@echo "🚀 Unified Pipeline:"
	@echo "  run-unified-pipeline      - Run the unified batch pipeline for all players and clubs in config.yaml"
	@echo "  run-ingested             - Run the ingestion stage for all tags in config.yaml (mode: club-players)"
	@echo "  run-raw                  - Run the raw stage: convert all ingested JSON to Parquet"
	@echo "  run-processed            - Run the processed stage: clean/process silver data for all entities (today)"
	@echo "  run-cleaned              - Run the cleaned stage: process gold layer for today"
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

.PHONY: help test lint fix format clean clean-data clean-ingested clean-raw clean-processed clean-all run-unified-pipeline run-test test-pydantic test-coverage run-streamlit
