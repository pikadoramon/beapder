# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_kafka_connector
   Description :
   Author :       pikadoramon
   date：          2023/10/19
-------------------------------------------------
   Change Activity:
                   2023/10/19:
-------------------------------------------------
"""
import sys

sys.path.append(r"D:\projects\gitopensource\beapder\beapder")

from utils.log import log

from utils.connector.kafka_connector import KafkaProducerConnector, KafkaConsumerConnector

__author__ = 'pikadoramon'

if __name__ == '__main__':
    GF_METRICS_KAFKA_HOST = "10.190.48.8:9092,10.190.48.9:9092,10.190.48.10:9092"
    GF_METRICS_KAFKA_TOPIC = 'test_spider_reportbeapder_topic'

    params = dict(
        KAFKA_PRODUCER_BATCH_LINGER_MS=0,
        KAFKA_PRODUCER_BUFFER_BYTES=33554432,
    )
    # k = KafkaProducerConnector(bootstrap_servers=GF_METRICS_KAFKA_HOST, topic=GF_METRICS_KAFKA_TOPIC, params=params)
    # k.logger = log
    # k.feed(
    #     [
    #         {"a": 1, "b": 1},
    #         {"c": 2, "b": 22}
    #     ]
    # )
    # k.close()
    params = dict(
        KAFKA_CONSUMER_TIMEOUT=float('inf'),
        KAFKA_CONSUMER_AUTO_OFFSET_RESET='smallest',
        KAFKA_CONSUMER_COMMIT_INTERVAL_MS=5000,
        KAFKA_CONSUMER_AUTO_COMMIT_ENABLE=True,
        KAFKA_CONSUMER_FETCH_MESSAGE_MAX_BYTES=1 * 1024 * 1024
    )
    k = KafkaConsumerConnector(bootstrap_servers=GF_METRICS_KAFKA_HOST, topic=GF_METRICS_KAFKA_TOPIC, group_id=None,
                               params=params)
    k.logger = log
    for item in k.next_item():
        print(item)
    k.close()
