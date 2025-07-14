
import yaml

from brawlstar_project.constants.paths import CONFIG_DIR


def merge_configs(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if isinstance(value, list) and key in result and isinstance(result[key], list):
            # Merge lists, remove duplicates, preserve order (base first, then local additions)
            result[key] = list(dict.fromkeys(result[key] + value))
        else:
            result[key] = value
    return result


def load_pipeline_config():
    """
    Load the pipeline config YAML file from CONFIG_DIR, merging with config.local.yaml if it exists.
    Lists are merged (union), other keys are overridden by local config.
    Returns:
        dict: Parsed config dictionary, with local overrides if present.
    Raises:
        FileNotFoundError: If the default config file does not exist.
    """
    config_path = CONFIG_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Look for config.local.yaml in the same directory
    local_config_path = CONFIG_DIR / "config.local.yaml"
    if local_config_path.exists():
        with open(local_config_path, "r") as f:
            local_config = yaml.safe_load(f)
        config = merge_configs(config, local_config)
    return config
