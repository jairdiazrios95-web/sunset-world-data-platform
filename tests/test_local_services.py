"""Local integration of rules/pre-embedding metadata (not real Kafka or vectors)."""
from pathlib import Path
from sunset.generator import build
from sunset.offline_services import execute


def test_event_policy_and_chunks_are_idempotent(tmp_path):
    src=tmp_path/'src';build(src,80,50,110)
    report=execute(src/'comentarios.csv',tmp_path/'offline.sqlite')
    assert report['status']=='PASS'
    assert report['unique_transcript_events']==5
    assert report['alerts_detected']==2
    assert report['unique_preembedding_chunks']==110
    assert report['replay_idempotent']
    assert report['embeddings']=='NOT_GENERATED'
