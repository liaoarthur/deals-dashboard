"""Config loader — reads config.yaml and environment variables."""

import os
import yaml

_config = None


def get_config():
    """Load and cache the YAML config file."""
    global _config
    if _config is not None:
        return _config

    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    with open(config_path, 'r') as f:
        _config = yaml.safe_load(f)

    return _config


def get_weights(lead_type):
    """Return the weight dict for a given lead type. Falls back to 'other'."""
    config = get_config()
    weights = config.get('weights', {})
    return weights.get(lead_type, weights.get('other', {'opportunity_size': 0.5, 'person_role': 0.5}))


def get_opportunity_size_config():
    return get_config().get('opportunity_size', {})


def get_person_role_config():
    return get_config().get('person_role', {})


def get_dedup_window():
    return get_config().get('dedup', {}).get('window_seconds', 60)
