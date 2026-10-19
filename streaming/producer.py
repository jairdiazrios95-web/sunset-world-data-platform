"""Deterministic sample Kafka producer with event keying and delivery ack.
Requires confluent-kafka for idempotent producer configuration.
"""
from __future__ import annotations
import argparse,json,os
from datetime import datetime,timezone
from uuid import NAMESPACE_URL,uuid5

EXAMPLE_TEXTS=[
 'Buenos días, ¿cómo podemos ayudarle?',
 'Le garantizo rentabilidad asegurada',
 'Nuestros beneficios están sujetos al contrato',
 'Sin ningún riesgo puede adquirir su membresía',
 'Le enviaremos la información por escrito',
]

def build_event(call_id,seq,text,event_time):
    if not call_id or seq<0:raise ValueError('Invalid call or sequence')
    return {'event_id':str(uuid5(NAMESPACE_URL,f'{call_id}:{seq}')),
            'call_id':call_id,'sequence':seq,'event_time':event_time,
            'transcript':text,'schema_version':1}

def make_events(call_id:str, at_time:str, texts=EXAMPLE_TEXTS):
    return [build_event(call_id,i+1,text,at_time) for i,text in enumerate(texts)]

def main():
    from confluent_kafka import Producer
    p=argparse.ArgumentParser()
    p.add_argument('--bootstrap',default=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092'))
    p.add_argument('--topic',default=os.getenv('KAFKA_TOPIC_RAW','transcripciones_raw'))
    p.add_argument('--call-id',default='SYNTHETIC-CALL-001')
    a=p.parse_args()
    producer=Producer({'bootstrap.servers':a.bootstrap,'enable.idempotence':True,
                       'acks':'all','message.timeout.ms':30000})
    timestamp=datetime.now(timezone.utc).isoformat()
    for event in make_events(a.call_id,timestamp):
        producer.produce(a.topic,key=event['call_id'],value=json.dumps(event))
    pending=producer.flush(30)
    if pending:raise RuntimeError(f'{pending} Kafka messages not acknowledged')
    print(f'Published {len(EXAMPLE_TEXTS)} synthetic events')

if __name__=='__main__':main()
