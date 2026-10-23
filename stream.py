"""Kafka to quality report with bounded event-time deduplication."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

SCHEMA = 'transaction_id string, event_time timestamp, amount double, currency string'


def run(args):
    spark = SparkSession.builder.appName('transaction-quality').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    if args.source == 'kafka':
        raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', args.brokers)
            .option('subscribe', 'transactions').option('startingOffsets', 'earliest').load())
        events = raw.select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('e')).select('e.*')
    else:
        events = spark.readStream.schema(SCHEMA).option('maxFilesPerTrigger',1).json(args.input)
    events = events.withColumn('quality_status', F.when(
        F.col('transaction_id').isNotNull() & F.col('event_time').isNotNull()
        & (F.col('amount') > 0) & (F.col('amount') < float('inf')) & (F.col('currency') == 'USD'), 'valid').otherwise('invalid'))
    # Malformed timestamps cannot enter event-time state. Audit all parsed rows separately.
    audit = (events.writeStream.format('parquet').option('path', f'{args.output}/audit')
        .option('checkpointLocation', f'{args.output}/checkpoints/audit').start())
    dedup = (events.filter('event_time IS NOT NULL AND transaction_id IS NOT NULL')
        .withWatermark('event_time', '10 minutes').dropDuplicatesWithinWatermark(['transaction_id']))

    def report(batch, batch_id):
        batch.persist()
        try:
            # Deterministic batch paths make replay idempotent per preserved checkpoint.
            base = f'{args.output}/batches/{batch_id}'
            batch.write.mode('overwrite').parquet(f'{base}/rows')
            batch.groupBy('quality_status').agg(F.count('*').alias('rows'), F.sum('amount').alias('amount_total')).write.mode('overwrite').json(f'{base}/metrics')
            batch.filter("quality_status = 'valid'").select('transaction_id','event_time','amount','currency').write.mode('overwrite').parquet(f'{base}/reporting')
        finally:
            batch.unpersist()

    query = (dedup.writeStream.foreachBatch(report).option('checkpointLocation', f'{args.output}/checkpoints/quality').start())
    try:
        query.awaitTermination(args.seconds)
        Path(args.output).mkdir(parents=True, exist_ok=True)
        Path(args.output, 'spark-progress.json').write_text(json.dumps(query.recentProgress, indent=2))
    finally:
        query.stop(); audit.stop(); spark.stop()


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--source', choices=['files','kafka'], default='files')
    p.add_argument('--input', default='input'); p.add_argument('--output', default='output/streaming')
    p.add_argument('--brokers', default='localhost:9092'); p.add_argument('--seconds', type=int, default=30)
    run(p.parse_args())
