# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_generic_spider
   Description :
   Author :       pikadoramon
   date：          2023/7/11
-------------------------------------------------
   Change Activity:
                   2023/7/11:
-------------------------------------------------
"""
import sys
import time

from beapder.buffer.request_buffer import AirSpiderRequestBuffer

from beapder.db.memorydb import MemoryDB

sys.path.append(r"D:\projects\gitopensource\beapder")
import random

import beapder
from beapder import LoadSettings, Request
from beapder.utils.log import log

from beapder.utils.rule import Rule, LxmlLinkExtractor
from beapder.core.spiders.generic_spider import GenericSpider
from beapder import Item
from beapder.core.timer_schedule import add_schedule
from beapder.utils.metrics import emit_timer


class TestItem(Item):

    def __init__(self, **kwargs):
        self.title = None
        self.name = None


class TestGenericSpider(GenericSpider):
    __custom_setting__ = dict(
        USE_SESSION=True,
        TASK_MAX_CACHED_SIZE=10,
        PROXY_EXTRACT_API=''
    )

    def __init__(self, thread_count=None):
        """
        基于内存队列的爬虫，不支持分布式
        :param thread_count: 线程数
        """
        super(GenericSpider, self).__init__(thread_count)
        self.max_depth = 0
        self._compile_rules()

        self._memory_db = MemoryDB(task_max_cached_size=50)
        self._parser_controls = []
        self._request_buffer = AirSpiderRequestBuffer(
            db=self._memory_db, dedup_name=self.name
        )

    def start_callback(self):
        print("爬虫开始")

    def end_callback(self):
        print("爬虫结束")

    def start_requests(self, *args, **kws):
        yield beapder.Request("https://www.cnet.com/news/")

    def distribute_task(self):
        """
        这里负责的是只是种子队列下发
        :return:
        """
        request = beapder.Request("https://www.cnet.com/news/")

        request.parser_name = request.parser_name or self.name
        for i in range(100):
            print(i, int(time.time()), self._memory_db.size())
            self._request_buffer.put_request(request, ignore_max_size=False)

    def download_midware(self, request):
        time.sleep(10)
        return request

    def validate(self, request, response):
        if response.status_code != 200:
            raise Exception("response code not 200")  # 重试

        # if "哈哈" not in response.text:
        #     return False # 抛弃当前请求

    def parse_item(self, request, response):
        item = TestItem()
        item.table_name = "demo"
        item.title = response.bs4().title
        item.name = "112"
        # if random.randint(0, 1):
        #     raise ValueError("故意引发的错误")
        # else:
        #     if random.randint(0, 1):
        #         raise FileNotFoundError("故意引发的错误")
        yield item


def hello_print():
    log.info("hello print")


if __name__ == "__main__":
    add_schedule(5, hello_print)
    add_schedule(5, hello_print)
    add_schedule(5, hello_print)
    settings = LoadSettings()
    settings.update("MAX_DEPTH_LIMIT", 1, "instance")
    settings.update("BEAPDER_EXTENSION_METRIC_EMITTER",
                    "beapder.extensions.metrics_emitter.LogMetricsEmitter",
                    "instance")
    settings.update("LOG_METRICS_EMITTER_INTERVAL", 3)
    settings.update("SPIDER_THREAD_COUNT", 1, "instance")
    # settings.update("MAX_DEAL_REQUEST_COUNT", 10)
    settings.update("ITEM_PIPELINES", ["beapder.pipelines.file_pipeline.JsonPipeline",
                                       "beapder.pipelines.console_pipeline.ConsolePipeline"])

    t = TestGenericSpider.from_settings(settings=settings)
    t.start()
