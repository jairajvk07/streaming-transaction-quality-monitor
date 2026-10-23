"""Publish synthetic fixtures in separate batches to advance the watermark."""
import json
import time
from pathlib import Path
from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode())
    for line in Path('sample_transactions.jsonl').read_text().splitlines():
        event = json.loads(line)
        producer.send('transactions', key=event['transaction_id'].encode(), value=event).get(timeout=15)
        producer.flush()
        time.sleep(6)
    producer.close()
