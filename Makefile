# Default player tag (You should put your player_tag)
DEFAULT_PLAYER_TAG = "PC0PPLRU"

# Use PLAYER_TAG from command line if defined, else use default
PLAYER_TAG ?= $(DEFAULT_PLAYER_TAG)

run-player-ingestion:
	@echo "Get player tag: $(PLAYER_TAG)"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/ingested/main.py $(PLAYER_TAG)

run-player-raw:
	@echo "Convert json file to parquet"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

run-player-processed:
	@echo "Display processed player data"
	PYTHONPATH=src uv run python src/brawlstar_project/processing/processed/main.py --player-tag $(PLAYER_TAG)

test:
	@echo "Running all tests..."
	PYTHONPATH=src uv run pytest

test-pydantic:
	@echo "Running Pydantic model tests..."
	PYTHONPATH=src uv run pytest tests/test_pydantic_models.py -v

test-coverage:
	@echo "Running tests with coverage..."
	PYTHONPATH=src uv run pytest --cov=src/brawlstar_project --cov-report=html --cov-report=term

run-test: test

lint:
	uv run ruff check .

fix:
	uv run ruff check . --fix

format:
	uv run ruff format .
	uv run isort .

sort-imports:
	uv run isort .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache