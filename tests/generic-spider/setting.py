# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     settings
   Description :
   Author :       pikadoramon
   date：          2023/7/14
-------------------------------------------------
   Change Activity:
                   2023/7/14:
-------------------------------------------------
"""
__author__ = 'pikadoramon'

from beapder.setting import *

ITEM_PIPELINES = [
    "beapder.pipelines.file_pipeline.JsonPipeline",
]

LOG_COLOR = True
