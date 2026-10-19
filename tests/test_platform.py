import json
from pathlib import Path
from sunset.config import load
from sunset.generator import build
from sunset.audit import audit
from rag.build_embeddings import chunks
from streaming.producer import build_event

def test_config():
    c=load(Path('config/pipeline.json'))
    assert c['profiles']['volume']['payments']==6_900_000

def test_generator_integrity(tmp_path):
    build(tmp_path,100,60,120)
    result=audit(tmp_path)
    assert result['status']=='PASS', result
    assert result['row_counts']['leads_ventas']==100

def test_idempotent_generator(tmp_path):
    build(tmp_path,100,60,120)
    data=(tmp_path/'leads_ventas.csv').read_bytes()
    build(tmp_path,100,60,120)
    assert (tmp_path/'leads_ventas.csv').read_bytes()==data

def test_chunk_overlap_and_validation():
    assert chunks('abcdefghij',6,2)==['abcdef','efghij']
    import pytest
    with pytest.raises(ValueError):chunks('hello',4,4)

def test_streaming_event_id_replay():
    args=('call-1',17,'test','2026-01-01T00:00:00Z')
    assert build_event(*args)['event_id']==build_event(*args)['event_id']
