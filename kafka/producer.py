from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from .kafka_config import load_producer_config


class KafkaProducer:

    def __init__(self, topic, value_schema_path, key_schema_path=None):
        schema = {
            'default_value_schema': avro.load(value_schema_path)
        }
        if key_schema_path is not None:
            schema['default_key_schema'] = avro.load(key_schema_path)

        self.producer = AvroProducer(
            load_producer_config(),
            **schema
        )
        self.topic = topic

    def send(self, value, key=None, flush=True):
        self.producer.produce(topic=self.topic, value=value, key=key)
        if flush:
            self.producer.flush()
