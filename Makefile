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

run-test:
	PYTHONPATH=src uv run pytest

lint:
	uv run ruff check .

fix:
	uv run ruff check . --fix

format:
	uv run ruff format .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache