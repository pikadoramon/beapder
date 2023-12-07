# -*- coding: utf-8 -*-
"""
Created on 2018-06-19 17:17
---------
@summary: request 管理器， 负责缓冲添加到数据库中的request
---------
@author: Boris
@email: boris_liu@foxmail.com
"""

import collections
import threading

from beapder.utils.load_settings import LoadSettings

setting = LoadSettings()
import beapder.utils.tools as tools
from beapder.db.memorydb import MemoryDB
from beapder.db.redisdb import RedisDB, ContainDB
from beapder.dedup import Dedup
from beapder.utils.log import log
from beapder.utils.generic import int_or_none
from beapder.network.request import Request

MAX_URL_COUNT = 1000  # 缓存中最大request数


class ContainRequestDB(ContainDB):

    def add(self, item, ignore_max_size=False):
        super().add(tools.dumps_json(item.to_dict()), ignore_max_size)

    def get(self):
        item = super().get()
        if item:
            return Request.from_dict(tools.get_json(item))
        return item


class AirSpiderRequestBuffer:
    dedup = None

    def __init__(self, db=None, dedup_name: str = None, max_scraped_size: int = None):
        self._db = db or MemoryDB()
        self._max_scraped_size = int_or_none(max_scraped_size)
        self._scraped_count = 0
        if not self.__class__.dedup and setting.REQUEST_FILTER_ENABLE:
            if dedup_name:
                self.__class__.dedup = Dedup(
                    name=dedup_name, to_md5=False, **setting.REQUEST_FILTER_SETTING
                )  # 默认使用内存去重
            else:
                self.__class__.dedup = Dedup(
                    to_md5=False, **setting.REQUEST_FILTER_SETTING
                )  # 默认使用内存去重

    def is_exist_request(self, request):
        if (
                request.filter_repeat
                and setting.REQUEST_FILTER_ENABLE
                and not self.__class__.dedup.add(request.fingerprint)
        ):
            log.debug("request已存在  url = %s" % request.url)
            return True
        return False

    def put_request(self, request, ignore_max_size=True):
        if self.is_exist_request(request):
            return
        elif self._max_scraped_size is None or self._max_scraped_size < 0:
            self._db.add(request, ignore_max_size=ignore_max_size)
            return
        elif self._scraped_count > self._max_scraped_size:
            log.warning("request超过最大设置数 > %d, 忽略请求 %s" % (self._max_scraped_size, request))
            return
        self._scraped_count += 1
        self._db.add(request, ignore_max_size=ignore_max_size)

    def get_requests_count(self):
        return self._db.size()


class RequestBuffer(AirSpiderRequestBuffer, threading.Thread):
    def __init__(self, redis_key):
        AirSpiderRequestBuffer.__init__(self, db=RedisDB(), dedup_name=redis_key)
        threading.Thread.__init__(self)

        self._thread_stop = False
        self._is_adding_to_db = False

        self._requests_deque = collections.deque()
        self._del_requests_deque = collections.deque()

        self._table_request = setting.TAB_REQUESTS.format(redis_key=redis_key)
        self._table_failed_request = setting.TAB_FAILED_REQUESTS.format(
            redis_key=redis_key
        )

    def run(self):
        self._thread_stop = False
        while not self._thread_stop:
            try:
                self.__add_request_to_db()
            except Exception as e:
                log.exception(e)

            tools.delay_time(1)

    def stop(self):
        self._thread_stop = True
        self._started.clear()

    def put_request(self, request):
        self._requests_deque.append(request)

        if self.get_requests_count() > MAX_URL_COUNT:  # 超过最大缓存，主动调用
            self.flush()

    def put_del_request(self, request):
        self._del_requests_deque.append(request)

    def put_failed_request(self, request, table=None):
        try:
            request_dict = request.to_dict
            self._db.zadd(
                table or self._table_failed_request, request_dict, request.priority
            )
        except Exception as e:
            log.exception(e)

    def flush(self):
        try:
            self.__add_request_to_db()
        except Exception as e:
            log.exception(e)

    def get_requests_count(self):
        return len(self._requests_deque)

    def is_adding_to_db(self):
        return self._is_adding_to_db

    def __add_request_to_db(self):
        request_list = []
        prioritys = []
        callbacks = []

        while self._requests_deque:
            request = self._requests_deque.popleft()
            self._is_adding_to_db = True

            if callable(request):
                # 函数
                # 注意：应该考虑闭包情况。闭包情况可写成
                # def test(xxx = xxx):
                #     # TODO 业务逻辑 使用 xxx
                # 这么写不会导致xxx为循环结束后的最后一个值
                callbacks.append(request)
                continue

            priority = request.priority

            # 如果需要去重并且库中已重复 则continue
            if self.is_exist_request(request):
                continue
            else:
                request_list.append(str(request.to_dict))
                prioritys.append(priority)

            if len(request_list) > MAX_URL_COUNT:
                self._db.zadd(self._table_request, request_list, prioritys)
                request_list = []
                prioritys = []

        # 入库
        if request_list:
            self._db.zadd(self._table_request, request_list, prioritys)

        # 执行回调
        for callback in callbacks:
            try:
                callback()
            except Exception as e:
                log.exception(e)

        # 删除已做任务
        if self._del_requests_deque:
            request_done_list = []
            while self._del_requests_deque:
                request_done_list.append(self._del_requests_deque.popleft())

            # 去掉request_list中的requests， 否则可能会将刚添加的request删除
            request_done_list = list(set(request_done_list) - set(request_list))

            if request_done_list:
                self._db.zrem(self._table_request, request_done_list)

        self._is_adding_to_db = False


# BEAPDER @ pikadoramon
# 用于分布式抓取的db, 优先本地queue, 当本地queue满队, 则下发到下一个队列中
class SharedRequestBuffer(AirSpiderRequestBuffer):

    def __init__(self, local_q=None, shared_q=None, local_cache_count=100, shared_cache_count=100,
                 dedup_name: str = None, max_scraped_size: int = None):
        self._db = local_q or MemoryDB()
        self.local_cache_count = int_or_none(local_cache_count)
        self._shared_db = shared_q
        self.shared_cache_count = int_or_none(shared_cache_count)
        self._max_scraped_size = int_or_none(max_scraped_size)
        self._scraped_count = 0
        if not self.__class__.dedup and setting.REQUEST_FILTER_ENABLE:
            if dedup_name:
                self.__class__.dedup = Dedup(
                    name=dedup_name, to_md5=False, **setting.REQUEST_FILTER_SETTING
                )  # 默认使用内存去重
            else:
                self.__class__.dedup = Dedup(
                    to_md5=False, **setting.REQUEST_FILTER_SETTING
                )  # 默认使用内存去重

    def is_exist_request(self, request):
        if (
                request.filter_repeat
                and setting.REQUEST_FILTER_ENABLE
                and not self.__class__.dedup.add(request.fingerprint)
        ):
            log.debug("request已存在  url = %s" % request.url)
            return True
        return False

    def put_request(self, request, ignore_max_size=True):
        if self.is_exist_request(request):
            return
        elif self._max_scraped_size is None or self._max_scraped_size < 0:
            if self._db.size() < self.local_cache_count:
                self._db.add(request, ignore_max_size=ignore_max_size)
            elif self._shared_db and self._shared_db.size() < self.local_cache_count:
                self._shared_db.add(request.to_dict(), ignore_max_size=ignore_max_size)
            else:
                log.warning("超过可以缓存的最大数量%d/%d, 忽略请求 %s" % (self.local_cache_count, self.shared_cache_count, request))
            return
        elif self._scraped_count > self._max_scraped_size:
            log.warning("request超过最大设置数 > %d, 忽略请求 %s" % (self._max_scraped_size, request))
            return

        self._scraped_count += 1
        if self._db.size() < self.local_cache_count:
            self._db.add(request, ignore_max_size=ignore_max_size)
        elif self._shared_db and self._shared_db.size() < self.local_cache_count:
            self._shared_db.add(request.to_dict(), ignore_max_size=ignore_max_size)
        else:
            log.warning("超过可以缓存的最大数量%d/%d, 忽略请求 %s" % (self.local_cache_count, self.shared_cache_count, request))
