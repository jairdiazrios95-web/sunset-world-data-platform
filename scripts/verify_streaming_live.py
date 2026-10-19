"""Proves Kafka -> Spark -> PostgreSQL via the actual broker and streaming sink.

Must run after Kafka, topic creation and Spark Structured Streaming are up.
FAILS on timeout, missing DB rows, missing broker events, and replay duplicates.
"""
from __future__ import annotations
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def verify(timeout:int,report:Path)->dict:
    if timeout<5:raise ValueError('Need a reasonable startup timeout')
    import psycopg
    from confluent_kafka import Producer,Consumer,KafkaException
    from streaming.producer import make_events
    bootstrap=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
    dsn=os.environ['SUNSET_PG_DSN']
    topics=[os.environ.get('KAFKA_TOPIC_PROCESSED','transcripciones_procesadas'),
            os.environ.get('KAFKA_TOPIC_ALERTS','alertas')]
    raw=os.environ.get('KAFKA_TOPIC_RAW','transcripciones_raw')
    run_id='VERIFY-'+uuid4().hex[:12]
    events=make_events(run_id,datetime.now(timezone.utc).isoformat())
    expected_ids={e['event_id'] for e in events}
    consumer=Consumer({'bootstrap.servers':bootstrap,'group.id':run_id,'auto.offset.reset':'latest',
                       'enable.auto.commit':False})
    producer=Producer({'bootstrap.servers':bootstrap,'enable.idempotence':True,'acks':'all'})
    collected={topic:set() for topic in topics}
    try:
        consumer.subscribe(topics)
        # Establish partitions assignment before sending events to avoid consumer races.
        started=time.monotonic()
        while not consumer.assignment() and time.monotonic()-started<min(30,timeout):
            consumer.poll(0.5)
        if not consumer.assignment():
            raise TimeoutError('Kafka consumer assignment failed')
        for item in events:
            producer.produce(raw,key=item['call_id'],value=json.dumps(item))
        if producer.flush(20):
            raise RuntimeError('Kafka publish not acknowledged')
        deadline=time.monotonic()+timeout
        db_count=alerts=0
        while time.monotonic()<deadline:
            msg=consumer.poll(0.5)
            if msg and not msg.error():
                try: payload=json.loads(msg.value())
                except (ValueError,TypeError): payload={}
                if payload.get('event_id') in expected_ids:
                    collected[msg.topic()].add(payload['event_id'])
            with psycopg.connect(dsn) as con:
                with con.cursor() as cur:
                    cur.execute('SELECT COUNT(*) FROM ops.call_center_processed WHERE call_id=%s',(run_id,))
                    db_count=cur.fetchone()[0]
                    cur.execute('SELECT COUNT(*) FROM ops.call_center_alert WHERE call_id=%s',(run_id,))
                    alerts=cur.fetchone()[0]
            if db_count==5 and alerts==2 and len(collected[topics[0]])==5 and len(collected[topics[1]])==2:
                break
        else:
            raise AssertionError(f'Stream mismatch db={db_count}, alerts={alerts}, topics={[(t,len(v)) for t,v in collected.items()]}')
        # A replay with the same event keys must not duplicate PostgreSQL rows.
        for item in events:
            producer.produce(raw,key=item['call_id'],value=json.dumps(item))
        if producer.flush(20):raise RuntimeError('Kafka replay not acknowledged')
        time.sleep(8)
        with psycopg.connect(dsn) as con:
            with con.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM ops.call_center_processed WHERE call_id=%s',(run_id,))
                rows_replayed=cur.fetchone()[0]
                cur.execute('SELECT COUNT(*) FROM ops.call_center_alert WHERE call_id=%s',(run_id,))
                alerts_replayed=cur.fetchone()[0]
        if (rows_replayed,alerts_replayed)!=(5,2):
            raise AssertionError(f'Replay duplicates/unexpected counts: {rows_replayed}/{alerts_replayed}')
        outcome={'status':'PASS','engine':'Kafka -> Spark Structured Streaming -> PostgreSQL',
                 'simulation':False,'run_id':run_id,'events':5,'alerts':2,
                 'processed_topic_events':len(collected[topics[0]]),
                 'alert_topic_events':len(collected[topics[1]]),
                 'postgres_rows_after_replay':rows_replayed,'postgres_alerts_after_replay':alerts_replayed}
        report.parent.mkdir(parents=True,exist_ok=True)
        report.write_text(json.dumps(outcome,indent=2)+'\n')
        return outcome
    finally:
        consumer.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--timeout',type=int,default=240)
    parser.add_argument('--report',type=Path,default=Path('reports/real_streaming_evidence.json'))
    args=parser.parse_args()
    print(json.dumps(verify(args.timeout,args.report),indent=2))
