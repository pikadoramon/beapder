# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     distribuated_rspider
   Description :
   Author :       pikadoramon
   date：          2023/10/20
-------------------------------------------------
   Change Activity:
                   2023/10/20:
-------------------------------------------------
"""

import signal

from beapder import AirSpider
from beapder.core.mixins import DistributedRedisMixin


class DistributedRSpider(AirSpider, DistributedRedisMixin):
    __custom_setting__ = {
    }
    settings = None

    def __init__(self, thread_count=None):
        """
        DistributedRSpider是基于airspider改造分布式爬虫, 支持kafka/redis/mysql作为任务下发db
        :param thread_count: 线程数
        """
        super(DistributedRSpider, self).__init__(thread_count)
        self.max_depth = 0

    @classmethod
    def from_settings(cls, settings, **kwargs):
        if cls.settings is None:
            cls.settings = settings
        obj = cls(thread_count=settings.getint("SPIDER_THREAD_COUNT") or kwargs.get("thread_count"))
        obj.max_depth = settings.getint("MAX_DEPTH_LIMIT", 5)
        signal.signal(signal.SIGINT, obj.stop_spider)

        # 这里启动Mixin中的基类
        obj.setup_server(settings)
        return obj
