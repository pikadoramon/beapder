# -*- coding: utf-8 -*-
"""爬虫配置文件"""
# 默认不导入beapder配置文件, 按需启动
# from beapder.setting import *
APP_NAME = "DEFAULT_APP_NAME"

APOLLO = 'test'
APOLLO_NAMESPACES = ['application', 'db']
APOLLO_CONFIGURES = [
    {
        "APOLLO_NAMESPACES": ['DEFAULT_APP_NAMESPACE', ],
        "APOLLO_APPID": "apollo-crawler-apps",
        "APOLLO_SECRET": ""
    }
]
