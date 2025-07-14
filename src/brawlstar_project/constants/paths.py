import os
from pathlib import Path

"""
Robust path constants for project directories.
- PROJECT_ROOT: Uses $PROJECT_ROOT env var if set, else tries CWD if it contains src/ and data/, else falls back to path relative to this file.
- All other paths are defined relative to PROJECT_ROOT.
"""


def _find_project_root() -> Path:
    # 1. Use environment variable if set
    env_root = os.environ.get("PROJECT_ROOT")
    if env_root:
        root = Path(env_root).resolve()
        if (root / "src").exists() and (root / "data").exists():
            return root
    # 2. Try CWD if it looks like project root
    cwd = Path.cwd().resolve()
    if (cwd / "src").exists() and (cwd / "data").exists():
        return cwd
    # 3. Fallback: relative to this file (default)
    return Path(__file__).resolve().parents[3]


PROJECT_ROOT = _find_project_root()
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_INGESTED_DIR = PROJECT_ROOT / "data" / "ingested"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DATA_CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"


def get_data_root() -> Path:
    """
    Returns the root directory for data, depending on environment:
    - Uses BRAWLSTARS_DATA_ROOT env var if set (recommended for local dev)
    - Uses data/sample/ if running on Streamlit Cloud
    - Defaults to data/cleaned/ for local runs
    """
    # 1. Environment variable override (local dev)
    data_root = os.environ.get("BRAWLSTARS_DATA_ROOT")
    if data_root:
        return Path(data_root).resolve()

    # 2. Detect Streamlit Cloud (by env var)
    if (
        os.environ.get("STREAMLIT_CLOUD", "0") == "1"
        or "STREMLIT_SHARED_SECRET" in os.environ
    ):
        return PROJECT_ROOT / "data" / "sample"

    # 3. Default: local cleaned data
    return DATA_CLEANED_DIR
