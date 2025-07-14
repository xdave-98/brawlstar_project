from pathlib import Path

import yaml

from brawlstar_project.constants.paths import CONFIG_PATH


def load_pipeline_config(config_path: Path = CONFIG_PATH):
    """
    Load the pipeline config YAML file.
    Args:
        config_path (Path): Path to the config YAML file.
    Returns:
        dict: Parsed config dictionary.
    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
