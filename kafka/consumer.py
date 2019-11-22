from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer

from .kafka_config import load_consumer_config


class KafkaConsumer:

    def __init__(self, topic, key_schema_path, value_schema_path):
        self.key_schema = avro.load(key_schema_path)
        self.value_schema = avro.load(value_schema_path)
        self.consumer = AvroConsumer(load_consumer_config())
        self.topic = topic

    def __enter__(self):
        self.consumer.subscribe([self.topic])

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

    def poll(self, timeout=None):
        return self.consumer.poll(timeout)

    def process_messages(self, callback, timeout=None):
        with self:
            while True:
                msg = self.consumer.poll(timeout)

                if msg is None:
                    continue

                if msg.error():
                    raise RuntimeError(msg.error())

                callback(msg.key(), msg.value())
