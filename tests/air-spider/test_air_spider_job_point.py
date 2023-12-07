# -*- coding: utf-8 -*-
"""
Created on 2023-12-06 20:23:02
---------
@summary:
---------
@author: Administrator
"""
import signal

import beapder
from beapder.extensions.job_points import DefaultJobPointExtension
from beapder.utils.load_settings import LoadSettings
from beapder.utils.rule import arg_to_iter


class TestAirSpiderJobPoint(beapder.AirSpider):
    __custom_setting__ = {
        "PROXY_EXTRACT_API": "",
    }

    def start_requests(self):

        for i in range(1000):
            yield beapder.Request("https://spidertools.cn")
            if i > 100:
                print("主动触发stop_spider")
                self.stop_spider(signal.CTRL_C_EVENT)

    def parse(self, request, response):
        # 提取网站title
        self.stop_spider(signal.CTRL_C_EVENT)
        print(response.xpath("//title/text()").extract_first())
        # 提取网站描述
        print(response.xpath("//meta[@name='description']/@content").extract_first())
        print("网站地址: ", response.url)


if __name__ == "__main__":
    # 推荐使用app.py作为入口启动spider
    settings = LoadSettings()
    settings.update("MAX_CHECK_TIME_IN_BUFFER", 10)
    settings.update("REQUEST_JOB_POINT_CONFIGS",
                    {'path': 'D:\\projects\\gitopensource\\beapder\\tests\\air-spider\\test_job_point', 'chunksize': 30}
                    )
    settings.update("EXTENSIONS", ["beapder.extensions.job_points.DefaultJobPointExtension"])
    TestAirSpiderJobPoint.from_settings(settings).start()
