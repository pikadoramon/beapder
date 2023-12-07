# -*- coding: utf-8 -*-
import copy
import signal
from typing import Sequence

from beapder.core.spiders.air_spider import AirSpider
from beapder.network.request import Request
from beapder.utils.log import log
from beapder.utils.rule import arg_to_iter, Rule


# TODO:
#  1. 完善下载中间件, 增加深度限制
#  2. 深度遍历, 层序遍历需要完善
#  3. 解绑inflxudb作为监控选择

class GenericSpider(AirSpider):
    __custom_setting__ = {
    }
    settings = None
    rules: Sequence[Rule] = ()

    def __init__(self, thread_count=None):
        """
        基于airspider改造分布式爬虫, 支持kafka/redis/mysql作为任务下发db
        :param thread_count: 线程数
        """
        super(GenericSpider, self).__init__(thread_count)
        self.max_depth = 0
        self._compile_rules()

    @classmethod
    def from_settings(cls, settings, **kwargs):
        if cls.settings is None:
            cls.settings = settings
        obj = cls(thread_count=settings.getint("SPIDER_THREAD_COUNT") or kwargs.get("thread_count"))
        obj.max_depth = settings.getint("MAX_DEPTH_LIMIT", 5)
        signal.signal(signal.SIGINT, obj.stop_spider)
        return obj

    def _compile_rules(self):
        for rule in self.rules:
            rule._compile(spider=self)

    def _build_request(self, rule_index, link, request_kwargs):
        if request_kwargs is None:
            request_kwargs = {}

        request = Request(
            method='GET',
            url=link,
            meta=dict(rule=rule_index),
            callback=self.parse,
            **request_kwargs
        )
        return request

    def parse(self, request, response):
        """
        这个parse函数是作为通用generic_spider默认解析函数
        原则上，不需要重写该函数
        :param request:
        :param response:
        :return:
        """
        depth = request.meta.get("depth", 0)
        for index, rule in enumerate(arg_to_iter(self.rules)):

            if rule.callback:
                request.meta["cb_kwargs"] = rule.cb_kwargs
                cb_res = rule.callback(request, response)
                for request_or_item in arg_to_iter(cb_res):
                    yield request_or_item

            if rule.follow:
                lnk_extractor = rule.link_extractor
                links = lnk_extractor.extract_links(response)

                for link in arg_to_iter(rule.process_links(links)):
                    if self.max_depth and depth >= self.max_depth:
                        log.debug(
                            "[%(parser)s] Ignoring link (depth  %(depth)d >= %(maxdepth)d): %(requrl)s ",
                            {'maxdepth': self.max_depth,
                             'requrl': link,
                             'depth': depth,
                             'parser': request.parser_name},
                        )
                        continue
                    request_kwargs = {
                        "headers": dict(Referer=response.url),
                    }
                    follow_res = self._build_request(index, link, request_kwargs)
                    for request_or_item in arg_to_iter(follow_res):
                        if isinstance(request_or_item, Request):
                            request_or_item.meta["depth"] = depth + 1
                            yield copy.copy(rule.process_request(request_or_item, response))
                        else:
                            yield request_or_item
