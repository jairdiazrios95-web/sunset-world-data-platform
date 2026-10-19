"""Spark 3.5 Structured Streaming: Kafka -> SQL policy rules -> PostgreSQL and Kafka.

Guarantee: microbatch replay is at-least-once; PostgreSQL ON CONFLICT(event_id)
provides logical idempotence. Kafka output is at-least-once: consumers must
also dedupe event_id. End-to-end exactly-once is NOT claimed.
"""
from __future__ import annotations
import os
from streaming.rules import RULES


def write_db_batch(df,batch_id):
    """Partition-local upserts avoid collecting all streaming events on driver."""
    def write_partition(iterator):
        import psycopg
        dsn=os.environ['SUNSET_PG_DSN']
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                for r in iterator:
                    d=r.asDict(recursive=True)
                    cur.execute('''INSERT INTO ops.call_center_processed
                        (event_id,call_id,sequence,event_time,transcript,has_alert,matched_pattern)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(event_id) DO UPDATE SET
                        transcript=excluded.transcript,has_alert=excluded.has_alert,
                        matched_pattern=excluded.matched_pattern,
                        processed_at=now()''',
                        (d['event_id'],d['call_id'],d['sequence'],d['event_time'],
                         d['transcript'],d['has_alert'],d['matched_pattern']))
                    if d['has_alert']:
                        cur.execute('''INSERT INTO ops.call_center_alert
                          (event_id,call_id,event_time,transcript,matched_pattern)
                          VALUES (%s,%s,%s,%s,%s)
                          ON CONFLICT(event_id) DO UPDATE SET
                          transcript=excluded.transcript,
                          matched_pattern=excluded.matched_pattern''',
                          (d['event_id'],d['call_id'],d['event_time'],
                           d['transcript'],d['matched_pattern']))
                    else:
                        cur.execute('DELETE FROM ops.call_center_alert WHERE event_id=%s',
                                    (d['event_id'],))
            conn.commit()
    df.foreachPartition(write_partition)


def main():
    from pyspark.sql import SparkSession,functions as F,types as T
    spark=SparkSession.builder.appName('Sunset-Streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    schema=T.StructType([
        T.StructField('event_id',T.StringType()),T.StructField('call_id',T.StringType()),
        T.StructField('sequence',T.LongType()),T.StructField('event_time',T.StringType()),
        T.StructField('transcript',T.StringType()),T.StructField('schema_version',T.IntegerType())])
    bootstrap=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:29092')
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap)
         .option('subscribe',os.environ.get('KAFKA_TOPIC_RAW','transcripciones_raw'))
         .option('startingOffsets','earliest').load())
    events=(raw.select(F.from_json(F.col('value').cast('string'),schema).alias('p')).select('p.*')
            .filter(F.col('event_id').isNotNull() & F.col('call_id').isNotNull()
                    & F.col('transcript').isNotNull() & F.col('event_time').isNotNull()))
    matched=F.lit(None).cast('string')
    for rule,regex in RULES.items():
        matched=F.when(F.col('transcript').rlike('(?i)'+regex),F.lit(rule)).otherwise(matched)
    processed=(events.withColumn('matched_pattern',matched)
               .withColumn('has_alert',F.col('matched_pattern').isNotNull()))
    checkpoint=os.environ.get('CHECKPOINT_DIR','/checkpoints')
    db=(processed.writeStream.foreachBatch(write_db_batch)
        .option('checkpointLocation',checkpoint+'/postgres').start())
    # Publishing to both topics is explicitly at-least-once; event IDs support dedupe.
    def publish(df,epoch):
        payload=(df.select(F.col('call_id').cast('string').alias('key'),
                    F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias('value')))
        payload.write.format('kafka').option('kafka.bootstrap.servers',bootstrap)\
               .option('topic',os.environ.get('KAFKA_TOPIC_PROCESSED','transcripciones_procesadas'))\
               .save()
        alerts=df.filter(F.col('has_alert'))
        if not alerts.isEmpty():
            alerts.select(F.col('call_id').cast('string').alias('key'),
               F.to_json(F.struct(*[F.col(c) for c in alerts.columns])).alias('value'))\
               .write.format('kafka').option('kafka.bootstrap.servers',bootstrap)\
               .option('topic',os.environ.get('KAFKA_TOPIC_ALERTS','alertas')).save()
    kafka=(processed.writeStream.foreachBatch(publish)
           .option('checkpointLocation',checkpoint+'/kafka').start())
    spark.streams.awaitAnyTermination()

if __name__=='__main__':main()
