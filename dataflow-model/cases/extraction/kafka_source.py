import time
import logging
import apache_beam as beam
from confluent_kafka import Consumer, KafkaError

class ReadFromKafkaConfluent(beam.DoFn):
    """
    Un DoFn que lee de Kafka usando confluent-kafka.
    """
    def __init__(self, bootstrap_servers, topic, group_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id or f'beam-reader-{int(time.time())}'
        self.consumer = None
        self.running = False

    def start_bundle(self):
        if not self.consumer:
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe([self.topic])
            self.running = True

    def process(self, element):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Kafka Error: {msg.error()}")
                        continue
                
                yield msg.value().decode('utf-8')
        finally:
            pass

    def teardown(self):
        if self.consumer:
            self.consumer.close()
