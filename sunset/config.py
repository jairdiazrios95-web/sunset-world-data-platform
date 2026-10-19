"""Centralized validated JSON settings, override file via SUNSET_CONFIG."""
import json, os
from pathlib import Path

DEFAULT = Path('config/pipeline.json')
def load(path=None):
    target=Path(path or os.getenv('SUNSET_CONFIG') or DEFAULT)
    config=json.loads(target.read_text(encoding='utf-8'))
    assert config['dataset_mode']=='synthetic', 'Only synthetic datasets permitted in this demo'
    for name, profile in config['profiles'].items():
        for key in ('leads','payments','comments'):
            if not isinstance(profile[key],int) or profile[key] <= 0:
                raise ValueError(f'{name}.{key} must be a positive integer')
    if config['parquet']['batch_size'] <= 0: raise ValueError('batch_size must be positive')
    return config
