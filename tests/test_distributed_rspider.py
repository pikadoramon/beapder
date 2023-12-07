# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_distributed_rspider
   Description :
   Author :       pikadoramon
   date：          2023/10/20
-------------------------------------------------
   Change Activity:
                   2023/10/20:
-------------------------------------------------
"""
__author__ = 'pikadoramon'

from beapder.utils.load_settings import LoadSettings
from beapder.core.spiders.distribuated_rspider import DistributedRSpider


class TestDistributeRSpider(DistributedRSpider):

    def start_requests(self):
        for req in self.next_requests():
            yield req

    def parse(self, request, response):
        print(request, response)


if __name__ == '__main__':
    settings = LoadSettings()
    settings.update("MAX_DEPTH_LIMIT", 1, "instance")

    settings.update("LOG_METRICS_EMITTER_INTERVAL", 10)
    settings.update("SPIDER_THREAD_COUNT", 1, "instance")
    # settings.update("MAX_DEAL_REQUEST_COUNT", 10)
    settings.update("ITEM_PIPELINES", ["beapder.pipelines.file_pipeline.JsonPipeline",
                                       "beapder.pipelines.console_pipeline.ConsolePipeline"])
    t = TestDistributeRSpider.from_settings(settings)
    t.start()
