# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     kafka_connector
   Description :
   Author :       pikadoramon
   date：          2023/10/19
-------------------------------------------------
   Change Activity:
                   2023/10/19:
-------------------------------------------------
"""
import logging

__author__ = 'pikadoramon'
import json
import traceback
from kafka.errors import OffsetOutOfRangeError
from retrying import retry
from kafka import KafkaProducer, TopicPartition
from kafka import KafkaConsumer
from beapder.utils.rule import arg_to_iter


class KafkaProducerConnector:

    def __init__(self, bootstrap_servers, topic, params):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client = None
        self._last_reconnect = 0
        self.logger = logging.getLogger()
        self.params = params

    def _feed(self, json_item):
        self._create_producer()
        if self.client is not None:
            for item in arg_to_iter(json_item):
                self.client.send(self.topic, item)
            self.client.flush()
            return True
        else:
            return False

    def _feed_with_topic(self, topic, json_item):
        self._create_producer()
        if self.client is not None:
            for item in arg_to_iter(json_item):
                self.client.send(topic, item)
            self.client.flush()
            return True
        else:
            return False

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=5)
    def _create_producer(self):
        """Tries to establish a Kafka consumer connection"""
        if self.client:
            return self.client
        try:
            self.logger.debug("Creating new kafka producer using brokers: " +
                              str(self.bootstrap_servers))

            self.client = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                        value_serializer=lambda m: json.dumps(m, indent=0).encode('utf-8'),
                                        retries=3,
                                        linger_ms=self.params['KAFKA_PRODUCER_BATCH_LINGER_MS'],
                                        buffer_memory=self.params['KAFKA_PRODUCER_BUFFER_BYTES'])
            return self.client
        except KeyError as e:
            self.logger.error('Missing setting named {0} ex={1}'.format(e, traceback.format_exc()))
        except:
            self.logger.error("Couldn't initialize kafka producer.ex={0}".format(traceback.format_exc()))
            raise

    def _reconnect_producer(self):
        self.client = None
        return self._create_producer()

    def feed(self, topic, json_item):
        '''
        Feeds a json item into the Kafka topic

        @param json_item: The loaded json object
        :param topic:
        '''
        if topic:
            result = self._feed_with_topic(topic, json_item)
        else:
            result = self._feed(json_item)

        if result:
            self.logger.debug(
                "Successfully fed item to Kafka {0} value={1}".format(self.topic, json.dumps(json_item)))
        else:
            self.logger.error(
                "Failed to feed item into Kafka{0} value={1}".format(self.topic, json.dumps(json_item)))

    def __del__(self):
        self.close()

    def __repr__(self):
        return "[KafkaProducerConnector bootstrap_servers={} " \
               "topic={}]".format(self.bootstrap_servers, self.topic)

    def close(self):
        if self.client:
            try:
                self.client.close(timeout=30)
                self.logger.info("Successfully closed kafka producer {0}".format(self))
            except Exception as e:
                self.logger.error("Couldn't close kafka producer {0}.ex={1}".format(e, traceback.format_exc()))


class KafkaConsumerConnector:
    def __init__(self, bootstrap_servers, topic, group_id, params):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client = None
        self._last_reconnect = 0
        self.logger = None
        self.group_id = group_id
        self.params = params

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=5)
    def _create_consumer(self):
        """Tries to establish a Kafka consumer connection"""
        if self.client:
            return self.client
        """Tries to establing the Kafka consumer connection"""
        try:
            self.logger.debug("Creating new kafka consumer using "
                              "brokers={0} topic={1} group_id={2}".format(self.bootstrap_servers,
                                                                          self.topic,
                                                                          self.group_id))

            self.client = KafkaConsumer(
                self.topic,
                group_id=self.group_id,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: m.decode('utf-8'),
                consumer_timeout_ms=self.params['KAFKA_CONSUMER_TIMEOUT'],
                auto_offset_reset=self.params['KAFKA_CONSUMER_AUTO_OFFSET_RESET'],
                auto_commit_interval_ms=self.params['KAFKA_CONSUMER_COMMIT_INTERVAL_MS'],
                enable_auto_commit=self.params['KAFKA_CONSUMER_AUTO_COMMIT_ENABLE'],
                max_partition_fetch_bytes=self.params['KAFKA_CONSUMER_FETCH_MESSAGE_MAX_BYTES'])
            return self.client
        except KeyError as e:
            self.logger.error('Missing setting named {0} ex={1}'.format(e, traceback.format_exc()))
        except:
            self.logger.error(
                "Couldn't initialize kafka consumer for topic {0} .ex={1}".format(self.topic, traceback.format_exc()))
            raise

    def _reconnect_consumer(self):
        self.client = None
        return self._create_consumer()

    def next_item(self, part_num=None, offset=None):
        try:
            self._create_consumer()
            partitions = []
            if part_num:
                for i in range(part_num):
                    partitions.append(
                        TopicPartition(self.topic, i)
                    )
                if len(partitions) < 1:
                    raise RuntimeError("empty topic")
                self.logger.info("%s assign %r" % (self, partitions))
                self.client.assign(partitions)
            if offset and isinstance(offset, str):
                pairs = offset.strip(",").split(",")
                for pair in pairs:
                    partition, idx = pair.split(":", 1)
                    self.logger.info("{} {} seek {}".format(offset, partition, idx))
                    self.client.seek(TopicPartition(offset, int(partition)), int(idx))

            for message in self.client:
                if message is None:
                    self.logger.debug("no message")
                    break
                # loaded_dict = json.loads(message.value)
                yield message.value
        except OffsetOutOfRangeError:
            # consumer has no idea where they are
            self.client.seek_to_end()
            self.logger.error("Kafka offset out of range error")

    def __del__(self):
        self.close()

    def __repr__(self):
        return "[KafkaConsumerConnector bootstrap_servers={} " \
               "topic={} group_id={}]".format(self.bootstrap_servers, self.topic, self.group_id)

    def close(self):
        if self.client:
            try:
                self.client.close(timeout=30)
                self.logger.info("Successfully closed kafka consumer {0}".format(self))
            except Exception as e:
                self.logger.error("Couldn't close kafka consumer {0}.ex={1}".format(e, traceback.format_exc()))
