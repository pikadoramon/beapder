# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     beapder_schedule
   Description :
   Author :       pikadoramon
   date：          2023/7/25
-------------------------------------------------
   Change Activity:
                   2023/7/25:
-------------------------------------------------
"""
from abc import ABC

from beapder import BaseParser, Request, Item
from beapder.core.parser_control import ParserControl
from beapder.utils import metrics

__author__ = 'pikadoramon'

from beapder.core.scheduler import BaseScheduler
from beapder.utils.load_settings import LoadSettings
from beapder.utils.rule import arg_to_iter
from beapder.utils.log import log

setting = LoadSettings()


class BeapderScheduler(BaseScheduler, ABC):
    __custom_setting__ = {}

    def __init__(
            self,
            request_buffer,
            item_buffer,
            collector,
            thread_count=None,
            begin_callback=None,
            end_callback=None,
            delete_keys=(),
            keep_alive=None,
            auto_start_requests=None,
            batch_interval=0,
            wait_lock=True,
            task_table=None,
            **kwargs,
    ):
        """
        @summary: 调度器
        ---------
        @param redis_key: 爬虫request及item存放redis中的文件夹
        @param thread_count: 线程数，默认为配置文件中的线程数
        @param begin_callback: 爬虫开始回调函数
        @param end_callback: 爬虫结束回调函数
        @param delete_keys: 爬虫启动时删除的key，类型: 元组/bool/string。 支持正则
        @param keep_alive: 爬虫是否常驻，默认否
        @param auto_start_requests: 爬虫是否自动添加任务
        @param batch_interval: 抓取时间间隔 默认为0 天为单位 多次启动时，只有当前时间与第一次抓取结束的时间间隔大于指定的时间间隔时，爬虫才启动
        @param wait_lock: 下发任务时否等待锁，若不等待锁，可能会存在多进程同时在下发一样的任务，因此分布式环境下请将该值设置True
        @param task_table: 任务表， 批次爬虫传递
        ---------
        @result:
        """
        super(BeapderScheduler, self).__init__()

        for key, value in self.__class__.__custom_setting__.items():
            if key == "AUTO_STOP_WHEN_SPIDER_DONE":  # 兼容老版本的配置
                setting.update("KEEP_ALIVE", not value, "instance")
            else:
                setting.update(key, value, "instance")

        self._request_buffer = request_buffer
        self._item_buffer = item_buffer
        self._collector = collector
        self._parsers = []
        self._parser_controls = []
        self._parser_control_obj = ParserControl

        # 兼容老版本的参数
        if "auto_stop_when_spider_done" in kwargs:
            self._keep_alive = not kwargs.get("auto_stop_when_spider_done")
        else:
            self._keep_alive = (
                keep_alive if keep_alive is not None else setting.KEEP_ALIVE
            )
        self._auto_start_requests = (
            auto_start_requests
            if auto_start_requests is not None
            else setting.SPIDER_AUTO_START_REQUESTS
        )
        self._batch_interval = batch_interval

        self._begin_callback = (
            begin_callback
            if begin_callback
            else lambda: log.info("\n********** beapder begin **********")
        )
        self._end_callback = (
            end_callback
            if end_callback
            else lambda: log.info("\n********** beapder end **********")
        )

        if thread_count:
            setting.update("SPIDER_THREAD_COUNT", thread_count, "instance")
        self._thread_count = setting.SPIDER_THREAD_COUNT

        self._is_notify_end = False  # 是否已经通知结束
        self._last_task_count = 0  # 最近一次任务数量
        self._last_check_task_count_time = 0
        self._stop_heartbeat = False  # 是否停止心跳

        self._project_total_state_table = "{}_total_state".format(self._project_name)
        self._is_exist_project_total_state_table = False

        self._last_check_task_status_time = 0
        self.wait_lock = wait_lock

        self.init_metrics()
        # 重置丢失的任务
        self.reset_task()

        self._stop_spider = False

        # 定义计时器
        self._timer = timer_scheduler

    def init_metrics(self):
        """
        初始化打点系统
        """
        metrics.init(**setting.METRICS_OTHER_ARGS)

    def add_parser(self, parser, **kwargs):
        parser = parser(**kwargs)  # parser 实例化
        if isinstance(parser, BaseParser):
            self._parsers.append(parser)
        else:
            raise ValueError("类型错误，爬虫需继承beapder.BaseParser或beapder.BatchParser")

    def __add_task(self):
        self.spider_begin()
        for parser in self._parsers:
            results = parser.start_requests()

            for result in arg_to_iter(results):
                result_type = None
                if isinstance(result, Request):
                    result.parser_name = result.parser_name or parser.name
                    self._request_buffer.put_request(result)
                    result_type = 1

                elif isinstance(result, Item):
                    self._item_buffer.put_item(result)
                    result_type = 2

                if callable(result):  # callbale的request可能是更新数据库操作的函数
                    if result_type == 1:
                        self._request_buffer.put_request(result)
                    elif result_type == 2:
                        self._item_buffer.put_item(result)
                else:
                    raise TypeError(
                        "start_requests yield result type error, expect Request、Item、callback func, bug get type: {}".format(
                            type(result)
                        )
                    )

            self._request_buffer.flush()
            self._item_buffer.flush()

    def run(self):
        pass

    def _start(self):
        # 下发新任务
        if self._auto_start_requests:  # 自动下发
            self.__add_task()

    def all_thread_is_done(self):
        pass

    def check_task_status(self):
        pass

    def delete_tables(self, delete_keys):
        pass

    def _stop_all_thread(self):
        pass

    def send_msg(self, msg, level="debug", message_prefix=""):
        pass

    def spider_begin(self):
        """
                @summary: start_monitor_task 方式启动，此函数与spider_end不在同一进程内，变量不可共享
                ---------
                ---------
                @result:
                """

        if self._begin_callback:
            self._begin_callback()

        for parser in self._parsers:
            parser.start_callback()

    def spider_end(self):
        self.record_end_time()

        if self._end_callback:
            self._end_callback()

        for parser in self._parsers:
            if not self._keep_alive:
                parser.close()
            parser.end_callback()

        if not self._keep_alive:
            # 关闭webdirver
            Request.render_downloader and Request.render_downloader.close_all()

            # 关闭打点
            metrics.close()
        else:
            metrics.flush()

    def record_end_time(self):
        pass

    def is_reach_next_spider_time(self):
        pass

    def join(self, timeout=None):
        """
        重写线程的join
        """
        if not self._started.is_set():
            return

        super().join()

    def heartbeat(self):
        pass

    def heartbeat_start(self):
        pass

    def heartbeat_stop(self):
        self._stop_heartbeat = True

    def have_alive_spider(self, heartbeat_interval=10):
        return False

    def reset_task(self, heartbeat_interval=10):
        raise NotImplementedError

    def stop_spider(self):
        self._stop_spider = True
