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
import beapder
from beapder import LoadSettings

from beapder.utils.rule import Rule, LxmlLinkExtractor
from beapder.core.spiders.generic_spider import GenericSpider


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
             process_links=lambda x:x[:5],
             follow=True),
    )

    def start_callback(self):
        print("爬虫开始")

    def end_callback(self):
        print("爬虫结束")

    def start_requests(self, *args, **kws):
        yield beapder.Request("https://www.cnet.com/news/")

    def download_midware(self, request):
        # request.headers = {'User-Agent': ""}
        # request.proxies = {"https":"https://12.12.12.12:6666"}
        # request.cookies = {}
        return request

    def validate(self, request, response):
        if response.status_code != 200:
            raise Exception("response code not 200")  # 重试

        # if "哈哈" not in response.text:
        #     return False # 抛弃当前请求

    def parse_item(self, request, response):
        print(response.bs4().title)
        print(response.xpath("//title").extract_first())


if __name__ == "__main__":
    settings = LoadSettings()
    settings.update("MAX_DEPTH_LIMIT", 1, "instance")
    t = TestGenericSpider.from_settings(settings=settings)
    t.start()
