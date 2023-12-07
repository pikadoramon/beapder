# -*- coding: utf-8 -*-
"""
Created on 2021-02-08 16:06:12
---------
@summary:
---------
@author: Boris
"""

import beapder
from items import *


class TestSpider(beapder.Spider):
    __custom_setting__ = {
        "PROXY_EXTRACT_API": "",
        "ITEM_PIPELINES": "beapder.pipelines.console_pipeline.ConsolePipeline",
        "AUTO_STOP_WHEN_SPIDER_DONE": False
    }

    def start_requests(self):
        for i in range(100):
            yield beapder.Request(f"https://spidertools.cn/#/{i}", callback=self.parse)

    def validate(self, request, response):
        if response.status_code != 200:
            raise Exception("response code not 200")  # 重试

        # if "哈哈" not in response.text:
        #     return False # 抛弃当前请求

    def parse(self, request, response):
        title = response.xpath("//title/text()").extract_first()  # 取标题
        item = spider_data_item.SpiderDataItem()  # 声明一个item
        item.title = title  # 给item属性赋值
        yield item  # 返回item， item会自动批量入库


if __name__ == '__main__':
    spider = TestSpider(redis_key="beapder3:test_spider",
                        thread_count=1)
    spider.start()
