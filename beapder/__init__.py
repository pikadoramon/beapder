# -*- coding: utf-8 -*-
"""
Created on 2020/4/21 10:41 PM
---------
@summary:
---------
@author: Boris
@email: boris_liu@foxmail.com
"""
import os
import re
import sys

sys.path.insert(0, re.sub(r"([\\/]items$)|([\\/]spiders$)", "", os.getcwd()))

__all__ = [
    "AirSpider",
    "Spider",
    "TaskSpider",
    "BatchSpider",
    "BaseParser",
    "TaskParser",
    "BatchParser",
    "Request",
    "Response",
    "Item",
    "UpdateItem",
    "ArgumentParser",
]

from beapder.core.spiders import AirSpider, Spider, TaskSpider, BatchSpider
from beapder.core.base_parser import BaseParser, TaskParser, BatchParser
from beapder.network.request import Request
from beapder.network.response import Response
from beapder.network.item import Item, UpdateItem
from beapder.utils.custom_argparse import ArgumentParser
from beapder.utils.load_settings import LoadSettings

