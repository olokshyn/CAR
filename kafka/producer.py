from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from .kafka_config import load_producer_config


class KafkaProducer:

    def __init__(self, topic, key_schema_path, value_schema_path):
        self.key_schema = avro.load(key_schema_path)
        self.value_schema = avro.load(value_schema_path)
        self.producer = AvroProducer(
            load_producer_config(),
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )
        self.topic = topic

    def send(self, key, value, flush=True):
        self.producer.produce(topic=self.topic, key=key, value=value)
        if flush:
            self.producer.flush()
