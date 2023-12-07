# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     distributed_mixin
   Description :
   Author :       pikadoramon
   date：          2023/10/19
-------------------------------------------------
   Change Activity:
                   2023/10/19:
-------------------------------------------------
"""
import time
from typing import Iterable

from beapder.utils import tools
from beapder.utils.rule import arg_to_iter
from beapder.utils.log import log
from beapder.db.redisdb import RedisDB

__author__ = 'pikadoramon'


class DistributedRedisMixin:
    """Mixin class to implement reading urls from a redis queue."""
    """这是子类用来读取队列中的任务进行采集"""
    distributed_task_key = None
    distributed_batch_size = None

    server = None
    settings = None

    # Idle start time
    spider_idle_start_time = int(time.time())
    max_idle_time = None
    max_deal_request_count = 0
    max_idle_request_count = 0
    idle_delay = 2

    fetch_data = None
    count_size = None

    def _next_request(self):
        found = 0
        datas = self.fetch_data(self.distributed_task_key, self.distributed_batch_size)
        for data in datas:
            reqs = self.make_request_from_data(data)
            if isinstance(reqs, Iterable):
                for req in reqs:
                    yield req
                    # XXX: should be here?
                    found += 1
                    log.info(f'start req url:{req.url}')
            elif reqs:
                yield reqs
                found += 1
            else:
                log.debug(f"Request not made from data: {data}")

        if found:
            log.debug(f"Read {found} requests from '{self.distributed_task_key}'")

    def make_request_from_data(self, data):
        raise NotImplementedError

    def setup_server(self, settings):
        if self.server is not None:
            return

        if settings is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            settings = getattr(self, 'settings', None)

        if settings is None:
            raise ValueError("settings is required")

        self.max_idle_time = settings.getint("MAX_IDLE_TIME", 300)
        assert isinstance(self.max_idle_time,
                          int) and self.max_idle_time > 0, "MAX_IDLE_TIME must be an integer and greater than 0"

        if self.distributed_task_key is None:
            self.distributed_task_key = settings.get('DISTRIBUTE_TASK_START_URLS_KEY')

        if self.distributed_task_key is None:
            raise ValueError("distributed_task_key is required")

        self.distributed_task_key = self.distributed_task_key.format(name=self.name, app_name=settings.get("APP_NAME"))

        if not self.distributed_task_key.strip():
            raise ValueError("redis_key must not be empty")

        if self.distributed_batch_size is None:
            # TODO: Deprecate this setting (REDIS_START_URLS_BATCH_SIZE).
            self.distributed_batch_size = settings.getint(
                'DISTRIBUTE_TASK_START_URLS_BATCH_SIZE',
                settings.getint('THREAD_COUNT'),
            )

        try:
            self.distributed_batch_size = int(self.distributed_batch_size)
            assert self.distributed_batch_size > 0
        except (TypeError, ValueError, AssertionError):
            raise ValueError("distributed_batch_size must be an integer and greater than 0")

        self.idle_delay = self.settings.getint("DOWNLOAD_IDLE_DELAY", 2)
        assert self.idle_delay > 0, "DOWNLOAD_IDLE_DELAY must be greater than 0"

        self.server = RedisDB(
            ip_ports=settings.get("REDIS_TASK_HOSTS"),
            db=settings.getint("REDIS_TASK_DB", 0),
            user_pass=settings.get("REDIS_TASK_PWD"),
            url=None,
            decode_responses=True,
            service_name=None,
            max_connections=16,
            transaction=False,
        )

        if settings.getbool('DISTRIBUTE_TASK_START_URLS_AS_SET', False):
            log.info(
                f"DISTRIBUTE_TASK_START_URLS_AS_SET=true SPOP=rand DISTRIBUTE_TASK_START_URLS_KEY={self.distributed_task_key}")
            self.fetch_data = self.server.sget
            self.count_size = self.server.sget_count
        elif settings.getbool('DISTRIBUTE_TASK_START_URLS_AS_ZSET', False):
            log.info(
                f"DISTRIBUTE_TASK_START_URLS_AS_SET=true ZSCORE=minus DISTRIBUTE_TASK_START_URLS_KEY={self.distributed_task_key}")
            self.fetch_data = self.pop_priority_queue
            self.count_size = self.server.zget_count
        elif settings.getbool('DISTRIBUTE_TASK_START_URLS_AS_LIST', False):
            log.info(
                f"DISTRIBUTE_TASK_START_URLS_AS_LIST=true LPOP=true DISTRIBUTE_TASK_START_URLS_KEY={self.distributed_task_key}")
            self.fetch_data = self.pop_list_queue
            self.count_size = self.server.lget_count
        elif settings.getbool('DISTRIBUTE_TASK_START_URLS_CUSTOM_QUEUE', False):
            log.info(
                f"DISTRIBUTE_TASK_START_URLS_CUSTOM_QUEUE=true LPOP=true DISTRIBUTE_TASK_START_URLS_KEY={self.distributed_task_key}")
            self.fetch_data = self.pop_list_queue
            self.count_size = self.queue_get_count
        else:
            log.info(
                f"DISTRIBUTE_TASK_START_URLS_AS_LIST=default LPOP=true DISTRIBUTE_TASK_START_URLS_KEY={self.distributed_task_key}")
            self.fetch_data = self.pop_list_queue
            self.count_size = self.server.lget_count

    def queue_get_count(self, table):
        raise NotImplementedError

    def pop_list_queue(self, redis_key, batch_size):
        with self.server.get_redis_obj().pipeline(transaction=self.server.transaction) as pipe:
            pipe.lrange(redis_key, 0, batch_size - 1)
            pipe.ltrim(redis_key, batch_size, -1)
            datas, _ = pipe.execute()
        return datas

    def pop_priority_queue(self, redis_key, batch_size):
        with self.server.get_redis_obj().pipeline(transaction=self.server.transaction) as pipe:
            pipe.zrevrange(redis_key, 0, batch_size - 1)
            pipe.zremrangebyrank(redis_key, -batch_size, -1)
            datas, _ = pipe.execute()
        return datas

    def dont_close_spider(self):

        if self.server is not None:
            count_size = self.count_size(self.distributed_task_key)
            log.info(f"pending task[{self.distributed_task_key}] has {count_size}")
            if count_size > 0:
                self.spider_idle_start_time = int(time.time())

        idle_time = int(time.time()) - self.spider_idle_start_time
        if self.max_idle_time != 0 and idle_time >= self.max_idle_time:
            spider_idle_start_time = tools.timestamp_to_date(self.spider_idle_start_time)
            log.error(f"max_idle_time over {self.max_idle_time}s. no any task since {spider_idle_start_time}")
            return False
        return True

    def next_requests(self):
        """
        这里负责的是只是种子队列下发
        :return:
        """

        deal_request_count = 0
        idle_request_count = 0

        while True:

            for request in arg_to_iter(self._next_request()):
                idle_request_count = 0

                yield request

                deal_request_count += 1

                if 0 < self.max_deal_request_count <= deal_request_count:
                    log.warning(
                        f"当前任务数{deal_request_count} 已经超过最大任务数 > MAX_DEAL_REQUEST_COUNT={self.max_deal_request_count}, "
                        "退出获取任务线程")
                    log.info("当前处于close_spider状态. 因此当任务为空时, 获取任务线程退出")
                    self.stop_spider()
                    return

            log.debug(f"Empty Queue. DOWNLOAD_DELAY={self.idle_delay}")
            idle_request_count += 1
            if 0 < self.max_idle_request_count <= idle_request_count:
                log.warning(
                    f"当前空闲数{idle_request_count} 已经超过最大空闲数 > MAX_IDLE_REQUEST_COUNT={self.max_idle_request_count}, "
                    "退出获取任务线程")
                break
            tools.delay_time(self.idle_delay)
            if not self.dont_close_spider():
                break

        log.info("当前处于close_spider状态. 因此当任务为空时, 获取任务线程退出")
        self.stop_spider()
