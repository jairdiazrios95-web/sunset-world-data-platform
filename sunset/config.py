"""Centralized validated JSON settings, override file via SUNSET_CONFIG."""
import json, os
from pathlib import Path

DEFAULT = Path('config/pipeline.json')
def load(path=None):
    target=Path(path or os.getenv('SUNSET_CONFIG') or DEFAULT)
    config=json.loads(target.read_text(encoding='utf-8'))
    if config.get('dataset_mode') != 'synthetic':
        raise ValueError('Only synthetic datasets permitted in this demo')
    profiles = config.get('profiles')
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError('profiles must be a non-empty object')
    for name, profile in profiles.items():
        if not isinstance(profile, dict):
            raise ValueError(f'{name} must define a profile object')
        for key in ('leads', 'payments', 'comments'):
            value = profile.get(key)
            if type(value) is not int or value <= 0:
                raise ValueError(f'{name}.{key} must be a positive integer')
    parquet = config.get('parquet')
    batch_size = parquet.get('batch_size') if isinstance(parquet, dict) else None
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError('parquet.batch_size must be a positive integer')
    return config
