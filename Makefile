run:
	PYTHONPATH=src uv run python src/brawlstar_project/processing/raw/main.py

lint:
	uv run ruff check .

fix:
	uv run ruff check . --fix

format:
	uv run ruff format .

clean:
	rm -rf __pycache__
	rm -rf .ruff_cache