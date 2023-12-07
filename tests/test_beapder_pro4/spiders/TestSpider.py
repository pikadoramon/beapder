# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     TestSpider
   Description :
   Author :       pikadoramon
   date：          2023/9/11
-------------------------------------------------
   Change Activity:
                   2023/9/11:
-------------------------------------------------
"""
from beapder.utils import tools

__author__ = 'pikadoramon'


class TestSpider:
    @classmethod
    def from_settings(self, settings):
        # print(settings.attr)
        return self()

    def start(self):
        for i in range(10):
            print("sleep 1")
            tools.delay_time(1)
