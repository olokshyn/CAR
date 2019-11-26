import logging

from confluent_kafka import avro, OFFSET_BEGINNING, OFFSET_END
from confluent_kafka.avro import AvroConsumer

from .kafka_config import load_consumer_config

OFFSET_LATEST = 'offset_latest'


class KafkaConsumer:

    def __init__(self, topic, value_schema_path, key_schema_path=None, manual_offset=None, config=None):
        if manual_offset not in {None, OFFSET_BEGINNING, OFFSET_END, OFFSET_LATEST}:
            raise ValueError(f'Invalid manual_offset value: {manual_offset}')
        self.manual_offset = manual_offset
        schema = {
            'reader_value_schema': avro.load(value_schema_path)
        }
        if key_schema_path is not None:
            schema['reader_key_schema'] = avro.load(key_schema_path)

        self.consumer = AvroConsumer(
            {
                **load_consumer_config(),
                **(config or {})
            },
            **schema
        )
        self.topic = topic

    def __enter__(self):

        def manual_assign(consumer, partitions):
            for p in partitions:
                if self.manual_offset in {OFFSET_BEGINNING, OFFSET_END}:
                    p.offset = self.manual_offset
                elif self.manual_offset == OFFSET_LATEST:
                    offset = consumer.get_watermark_offsets(p, timeout=5)
                    if offset is None:
                        logging.debug(f'Got None offset for partition {p}')
                        last_msg_offset = OFFSET_END
                    else:
                        logging.debug(f'Got offset {offset} for partition {p}')
                        last_msg_offset = offset[1] - 1
                    p.offset = last_msg_offset
                else:
                    raise ValueError(f'Unsupported manual_offset value: {self.manual_offset}')
            consumer.assign(partitions)
            logging.info(f'Manually assigned partitions: {partitions}')

        params = {}
        if self.manual_offset is not None:
            params['on_assign'] = manual_assign
        self.consumer.subscribe([self.topic], **params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

    def poll(self, timeout=None):
        msg = self.consumer.poll(timeout)
        if msg is not None and msg.error():
            raise RuntimeError(msg.error())
        return msg

    def process_messages(self, callback, timeout=None):
        with self:
            while True:
                msg = self.consumer.poll(timeout)

                if msg is None:
                    continue

                if msg.error():
                    raise RuntimeError(msg.error())

                callback(msg.key(), msg.value())
