"""Real standard-library execution for streaming RULES and PRE-embedding prep.

This is NOT Kafka/Spark, does NOT create vectors, and does NOT query Mantis.
An opt-in, explicitly named local verification of code paths and idempotence.
"""
from __future__ import annotations
import argparse,csv,json,sqlite3
from pathlib import Path
from streaming.producer import make_events
from streaming.rules import detect
from rag.build_embeddings import redact,chunks,chunk_identity,digest


def execute(comments_csv:Path,db_file:Path,chunk_size=380,overlap=40,call_id='DEMO-001'):
    db_file.parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(db_file) as db:
        db.executescript('''
        CREATE TABLE IF NOT EXISTS incoming_event(event_id TEXT PRIMARY KEY,call_id TEXT,sequence INT,transcript TEXT,rules TEXT);
        CREATE TABLE IF NOT EXISTS chunk_metadata(chunk_id TEXT PRIMARY KEY,source_id TEXT,content TEXT,content_hash TEXT,model_id TEXT);
        ''')
        event_count=0
        found_alerts=0
        events=make_events(call_id,'2026-10-17T00:00:00Z')
        comments_read=0
        def replay():
            nonlocal event_count, found_alerts
            for e in events:
                matches=detect(e['transcript'])
                event_count+=1
                found_alerts+=bool(matches)
                db.execute('''INSERT INTO incoming_event(event_id,call_id,sequence,transcript,rules)
                    VALUES(?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET transcript=excluded.transcript,rules=excluded.rules''',
                    (e['event_id'],e['call_id'],e['sequence'],e['transcript'],json.dumps(matches)))
            nonlocal comments_read
            with comments_csv.open(encoding='utf-8',newline='') as stream:
                for row in csv.DictReader(stream):
                    comments_read+=1
                    for i,content in enumerate(chunks(redact(row['texto']),chunk_size,overlap)):
                        db.execute('''INSERT INTO chunk_metadata(chunk_id,source_id,content,content_hash,model_id)
                         VALUES(?,?,?,?,?) ON CONFLICT(chunk_id) DO UPDATE SET content=excluded.content,content_hash=excluded.content_hash''',
                         (chunk_identity('comment',row['comentario_id'],i,'PREPROCESS_ONLY'),row['comentario_id'],
                          content,digest(content),'PREPROCESS_ONLY'))
        replay()
        first={'events':db.execute('SELECT COUNT(*) FROM incoming_event').fetchone()[0],
               'chunk_metadata':db.execute('SELECT COUNT(*) FROM chunk_metadata').fetchone()[0]}
        replay()
        second={'events':db.execute('SELECT COUNT(*) FROM incoming_event').fetchone()[0],
                'chunk_metadata':db.execute('SELECT COUNT(*) FROM chunk_metadata').fetchone()[0]}
        if first!=second:raise AssertionError('Replay duplicates events or chunks')
        return {'status':'PASS','mode':'OFFLINE_RULES_AND_TEXT_PREPROCESS_ONLY',
                'unique_transcript_events':first['events'],
                'alerts_detected':db.execute("SELECT COUNT(*) FROM incoming_event WHERE rules<>'[]'").fetchone()[0],
                'unique_preembedding_chunks':first['chunk_metadata'],
                'comments_read':comments_read//2,'replay_idempotent':True,
                'kafka':'NOT_EXECUTED','spark':'NOT_EXECUTED',
                'embeddings':'NOT_GENERATED','pgvector':'NOT_EXECUTED','mantis':'NOT_EXECUTED'}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--comments',type=Path,default=Path('data/sample/comentarios.csv'))
    p.add_argument('--db',type=Path,default=Path('reports/runs/offline_services.sqlite'))
    p.add_argument('--report',type=Path,default=Path('reports/offline_services_verified.json'))
    a=p.parse_args()
    result=execute(a.comments,a.db)
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
