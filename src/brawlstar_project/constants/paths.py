from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
DATA_INGESTED_DIR = PROJECT_ROOT / "data" / "ingested"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DATA_CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned" 