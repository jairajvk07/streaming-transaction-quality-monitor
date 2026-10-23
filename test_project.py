import json
import os
import unittest
from pathlib import Path

class FixtureContracts(unittest.TestCase):

    def test_transaction_fixture_has_duplicates_and_late_event(self):
        rows = [json.loads(x) for x in Path('sample_transactions.jsonl').read_text().splitlines()]
        self.assertEqual(rows[0], rows[1])
        self.assertLess(rows[4]['event_time'], rows[3]['event_time'])
        self.assertLess(rows[2]['amount'], 0)

@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1 with Java and Spark installed')
class SparkIntegration(unittest.TestCase):

    def test_late_event_after_watermark_advances(self):
        import tempfile
        from pyspark.sql import SparkSession
        from stream import SCHEMA
        with tempfile.TemporaryDirectory() as tmp:
            spark = SparkSession.builder.master('local[2]').getOrCreate()
            source = Path(tmp, 'input')
            source.mkdir()
            events = spark.readStream.schema(SCHEMA).json(str(source))
            query = events.withWatermark('event_time', '10 minutes').dropDuplicatesWithinWatermark(['transaction_id']).writeStream.format('memory').queryName('late_test').outputMode('append').start()
            try:

                def emit(index, key, time):
                    Path(source, f'{index}.json').write_text(json.dumps({'transaction_id': key, 'event_time': time, 'amount': 1, 'currency': 'USD'}))
                    query.processAllAvailable()
                emit(1, 'first', '2026-01-01T12:00:00Z')
                emit(2, 'advance', '2026-01-01T12:30:00Z')
                emit(3, 'late', '2026-01-01T11:00:00Z')
                self.assertEqual(spark.sql("select * from late_test where transaction_id='late'").count(), 0)
                self.assertGreater(sum((op.get('numRowsDroppedByWatermark', 0) for p in query.recentProgress for op in p.get('stateOperators', []))), 0)
            finally:
                query.stop()
                spark.stop()

    def test_streaming_dedup_quality_and_audit(self):
        import tempfile
        from pyspark.sql import SparkSession
        from stream import run
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            input_dir = Path(tmp, 'input')
            input_dir.mkdir()
            (input_dir / 'transactions.jsonl').write_text(Path('sample_transactions.jsonl').read_text())
            run(argparse.Namespace(source='files', input=str(input_dir), output=tmp, seconds=25))
            spark = SparkSession.builder.master('local[2]').getOrCreate()
            try:
                rows = spark.read.parquet(f'{tmp}/batches/*/rows')
                self.assertEqual(rows.filter("transaction_id = 'T01'").count(), 1)
                self.assertEqual(rows.filter("transaction_id = 'T02' AND quality_status = 'invalid'").count(), 1)
                self.assertEqual(spark.read.parquet(f'{tmp}/audit').count(), 6)
            finally:
                spark.stop()
if __name__ == '__main__':
    unittest.main()
