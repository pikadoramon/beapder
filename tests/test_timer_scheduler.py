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

import random
import time

import beapder
from beapder import LoadSettings

settings = LoadSettings()
settings.update("MAX_DEPTH_LIMIT", 1, "instance")
settings.update("BEAPDER_EXTENSION_METRIC_EMITTER",
                "beapder.extensions.metrics_emitter.LogMetricsEmitter",
                "instance")
settings.update("SPIDER_THREAD_COUNT", 1, "instance")
settings.update("MAX_DEAL_REQUEST_COUNT", 1, "instance")
ITEM_PIPELINES = [
    "beapder.pipelines.console_pipeline.ConsolePipeline"
]
settings.update("ITEM_PIPELINES", [
    "beapder.pipelines.console_pipeline.ConsolePipeline"
], "instance")
print(settings)

from beapder.utils.log import log

from beapder.utils.rule import Rule, LxmlLinkExtractor
from beapder.core.spiders.generic_spider import GenericSpider
from beapder import Item
from beapder.core.timer_schedule import add_schedule


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
    rules = (
        Rule(extractor=LxmlLinkExtractor(allow_domains=("www.cnet.com", "cnet.com"),
                                         extract_xpaths=("//a/@href", "//area/@href")),
             callback='parse_item',
             process_links=lambda x: x[:1],
             follow=True),
    )

    def start_callback(self):
        print("爬虫开始")

    def end_callback(self):
        print("爬虫结束")

    def start_requests(self, *args, **kws):
        yield beapder.Request("https://www.cnet.com/news/")

    def download_midware(self, request):
        print("download_midware", request)
        time.sleep(3)
        # request.headers = {'User-Agent': ""}
        # request.proxies = {"https":"https://12.12.12.12:6666"}
        # request.cookies = {}
        return request

    def validate(self, request, response):
        print("validate", request, response)
        if response.status_code != 200:
            raise Exception("response code not 200")  # 重试

        # if "哈哈" not in response.text:
        #     return False # 抛弃当前请求

    def parse_item(self, request, response):
        print(response.bs4().title)
        print(response.xpath("//title").extract_first())
        item = TestItem()
        item.title = response.bs4().title
        item.name = "112"
        # if random.randint(0, 1):
        #     raise ValueError("故意引发的错误")
        # else:
        #     if random.randint(0, 1):
        #         raise FileNotFoundError("故意引发的错误")
        yield item


def print_hello():
    log.warning("hello world")


if __name__ == "__main__":
    add_schedule(5, print_hello)
    add_schedule(5, print_hello)

    t = TestGenericSpider.from_settings(settings=settings)
    print(settings.attr)
    t.start()
