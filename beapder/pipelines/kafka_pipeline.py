# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     kafka_pipeline
   Description :
   Author :       pikadoramon
   date：          2023/10/19
-------------------------------------------------
   Change Activity:
                   2023/10/19:
-------------------------------------------------
"""
__author__ = 'pikadoramon'

from typing import Dict, List, Tuple

from beapder.utils.connector.kafka_connector import KafkaProducerConnector
from beapder.pipelines import BasePipeline
from beapder.utils.log import log


class KafkaPipeline(BasePipeline):
    settings = None

    def __init__(self):
        self._to_db = None
        self._topic = None

    @classmethod
    def from_settings(cls, settings):
        cls.settings = settings
        return cls()

    @property
    def to_db(self):
        if not self._to_db:
            label = self.settings.get("KAFKA_PIPELINES_LABEL", "default")
            kwargs = self.settings["KAFKA_PIPELINES_KWARGS"][label]
            self._topic = kwargs["topic"]
            self._to_db = KafkaProducerConnector(**kwargs)
        return self._to_db

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        try:
            result = self.to_db.feed(table, items)
            datas_size = len(items)
            log.info(
                "共导出 %s 条数据到 kafka: %s,  新增 %s条, 重复 %s 条"
                % (datas_size, table or self._topic, result, datas_size)
            )
            return True
        except Exception as e:
            log.exception(e)
            return False

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据
        Args:
            table: 表名
            items: 数据，[{},{},...]
            update_keys: 更新的字段, 如 ("title", "publish_time")

        Returns: 是否更新成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        log.warning("KAFKA-PIPELINE 不支持更新操作. 数据将追加到相应kafka topic")
        return self.save_items(table, items)


class KafkaSimplePipeline(KafkaPipeline):

    @property
    def to_db(self):
        if not self._to_db:
            self._topic = self.settings["KAFKA_LOG_PRODUCER_TOPIC"]
            self._to_db = KafkaProducerConnector(bootstrap_servers=self.settings["KAFKA_LOG_PRODUCER_HOSTS"],
                                                 topic=self.settings["KAFKA_LOG_PRODUCER_TOPIC"],
                                                 params=dict(KAFKA_PRODUCER_BATCH_LINGER_MS=0,
                                                             KAFKA_PRODUCER_BUFFER_BYTES=33554432))
        return self._to_db
