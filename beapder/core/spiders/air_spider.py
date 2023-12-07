# -*- coding: utf-8 -*-
"""
Created on 2020/4/22 12:05 AM
---------
@summary: 基于内存队列的爬虫，不支持分布式
---------
@author: Boris
@email: boris_liu@foxmail.com
"""
import os
import signal
import traceback
from threading import Thread

import beapder.utils.tools as tools
from beapder.buffer.item_buffer import ItemBuffer
from beapder.buffer.request_buffer import AirSpiderRequestBuffer
from beapder.core.base_parser import BaseParser
from beapder.core.parser_control import AirSpiderParserControl
from beapder.core.timer_schedule import timer_scheduler
from beapder.db.memorydb import MemoryDB
from beapder.extensions import BaseExtension
from beapder.network.request import Request
from beapder.utils import metrics
from beapder.utils.rule import arg_to_iter
from beapder.utils.log import log


class AirSpider(BaseParser, Thread):
    __custom_setting__ = {
    }
    settings = None
    _extensions = []

    def __init__(self, thread_count=None):
        """
        基于内存队列的爬虫，不支持分布式
        :param thread_count: 线程数
        """
        super(AirSpider, self).__init__()

        for key, value in self.__class__.__custom_setting__.items():
            self.settings.update(key, value, "instance")

        if thread_count:
            self.settings.update("SPIDER_THREAD_COUNT", thread_count, "instance")
        self._thread_count = self.settings.SPIDER_THREAD_COUNT

        self._memory_db = MemoryDB()
        self._parser_controls = []
        self._item_buffer = ItemBuffer(redis_key="air_spider")
        self._request_buffer = AirSpiderRequestBuffer(
            db=self._memory_db, dedup_name=self.name
        )

        self._stop_spider = False

        self._close_reason = "正常停止"
        self._close_spider_num = 1
        self._close_spider_max_num = 3
        # 这里定义一个计时器
        self._timer = timer_scheduler

        self.init_metrics()

    @property
    def pending_buffer(self):
        return self._memory_db

    def init_metrics(self):
        """
        初始化打点系统
        """
        metrics.init(**self.settings.METRICS_OTHER_ARGS)

    @classmethod
    def from_settings(cls, settings, **kwargs):
        if cls.settings is None:
            cls.settings = settings
        obj = cls(thread_count=settings.getint("SPIDER_THREAD_COUNT") or
                               kwargs.get("thread_count"))
        extension_classes = settings.get("EXTENSIONS")
        for extension_clz in arg_to_iter(extension_classes):
            if isinstance(extension_clz, str):
                ext_obj = tools.import_cls(extension_clz)
                assert issubclass(ext_obj, BaseExtension), "无法实例 `{}`".format(extension_clz)
                cls._extensions.append(ext_obj.from_settings(cls.settings))
                log.info("实例 extension {}".format(extension_clz))
            elif issubclass(extension_clz, BaseExtension):
                cls._extensions.append(extension_clz.from_settings(cls.settings))
                log.info("实例 extension {}".format(extension_clz.__class__.__name__))
            else:
                raise AssertionError("无法实例 `{}`".format(extension_clz))
        signal.signal(signal.SIGINT, obj.stop_spider)
        return obj

    def start_callback(self):
        log.info("start_callback开始, 正在使用调用扩展列表实例")
        for ext in self._extensions:
            ext.open_spider(self)

    def end_callback(self):
        log.info("end_callback开始, 正在调用extension列表实例")
        if self._extensions and isinstance(self._extensions, list):
            for ext in self._extensions[::-1]:
                ext.close_spider(self)
            return

        if not isinstance(self._extensions, list):
            log.error("end_callback异常, extension列表类型错误")

    def distribute_task(self):
        """
        这里负责的是只是种子队列下发
        :return:
        """

        max_buffer_count = self.settings.getint("MAX_REQUEST_BUFFER_COUNT")
        busy_delay = self.settings.getint("DOWNLOAD_BUSY_DELAY")
        busy_start_time = 0
        assert max_buffer_count > 0, "MAX_REQUEST_BUFFER_COUNT must be greater than 0"
        assert busy_delay > 0, "DOWNLOAD_BUSY_DELAY must be greater than 0"
        for request in arg_to_iter(self.start_requests()):
            if not isinstance(request, Request):
                raise ValueError("仅支持 yield Request")
            request.parser_name = request.parser_name or self.name
            self._request_buffer.put_request(request, ignore_max_size=False)
            if self._stop_spider:
                log.warning("你已经关闭spider, 将忽略后续请求数据")
                break
            while self._request_buffer.get_requests_count() > max_buffer_count:
                if tools.get_current_timestamp() - busy_start_time > 300:
                    log.warning("请求buffer已满 请检查爬虫进程是否正常消费数据 {} > {}".format(self._request_buffer.get_requests_count(),
                                                                            max_buffer_count))
                    busy_start_time = tools.get_current_timestamp()
                tools.delay_time(busy_delay)

        _has_next_requests = hasattr(self, "next_requests")

        if _has_next_requests and not self._stop_spider:
            for request in arg_to_iter(getattr(self, "next_requests")()):
                if not isinstance(request, Request):
                    raise ValueError("仅支持 yield Request")

                request.parser_name = request.parser_name or self.name
                self._request_buffer.put_request(request, ignore_max_size=False)
                if self._stop_spider:
                    log.warning("你已经关闭spider, 将忽略后续请求数据")
                    break
                while self._request_buffer.get_requests_count() > max_buffer_count:
                    if tools.get_current_timestamp() - busy_start_time > 300:
                        log.warning(
                            "请求buffer已满 请检查爬虫进程是否正常消费数据 {} > {}".format(self._request_buffer.get_requests_count(),
                                                                        max_buffer_count))
                        busy_start_time = tools.get_current_timestamp()
                    tools.delay_time(busy_delay)

    def all_thread_is_done(self):
        for _ in range(3):  # 降低偶然性, 因为各个环节不是并发的，很有可能当时状态为假，但检测下一条时该状态为真。一次检测很有可能遇到这种偶然性
            # 检测 parser_control 状态
            for parser_control in self._parser_controls:
                if not parser_control.is_not_task():
                    return False

            # 检测 任务队列 状态
            if self._close_reason == "正常停止":
                if not self._memory_db.empty():
                    return False

            # 检测 item_buffer 状态
            if (
                    self._item_buffer.get_items_count() > 0
                    or self._item_buffer.is_adding_to_db()
            ):
                return False

            tools.delay_time(1)

        return True

    def run(self):
        self.start_callback()
        # 检查buffer是否依旧存留requests对象
        buffer_ttl = self.settings.getint("MAX_CHECK_TIME_IN_BUFFER", 300)
        assert buffer_ttl

        for _ in range(self._thread_count):
            parser_control = AirSpiderParserControl(
                memory_db=self._memory_db,
                request_buffer=self._request_buffer,
                item_buffer=self._item_buffer,
            )
            parser_control.add_parser(self)
            parser_control.start()
            self._parser_controls.append(parser_control)

        # 启动计时器线程
        self._timer.start()
        self._item_buffer.start()
        self.distribute_task()

        log.info("buffer存留request任务对象 {}个 最大检查时长为{}".format(
            self._request_buffer.get_requests_count(),
            buffer_ttl))
        start_record_time = tools.get_current_timestamp()
        while self._request_buffer.get_requests_count() > 0:
            log.debug("当前buffer存留request任务对象 {}个. 10秒后再次检测".format(self._request_buffer.get_requests_count()))
            tools.delay_time(10)
            if tools.get_current_timestamp() - start_record_time > buffer_ttl:
                log.warning("超过检查时长 {}秒, 当前退出检测程序. 剩余buffer {}个".format(
                    buffer_ttl,
                    self._request_buffer.get_requests_count()))
                break
        log.info("退出检测程序. 剩余buffer {}个".format(self._request_buffer.get_requests_count()))

        error_streak = 0
        while True:
            try:
                if self._stop_spider or self.all_thread_is_done():
                    # 停止 parser_controls
                    for parser_control in self._parser_controls:
                        parser_control.stop()

                    # 关闭item_buffer
                    self._item_buffer.stop()

                    # 关闭webdirver
                    Request.render_downloader and Request.render_downloader.close_all()

                    if self._stop:
                        log.info("爬虫被停止[{}]".format(self._close_reason))
                    else:
                        log.info("无任务，爬虫结束")
                    break
                error_streak = 0
            except Exception as e:
                log.error("程序异常 {}. 详细原因如下".format(e))
                log.error(traceback.format_exc(limit=10))
                error_streak += 1

            if error_streak > 60:
                log.critical("程序执行解析连续错误异常超过{} > 60. 退出接下来的爬虫进程".format(error_streak))
                break

            tools.delay_time(1)  # 1秒钟检查一次爬虫状态

        self.end_callback()
        # 为了线程可重复start
        self._started.clear()
        # 关闭打点
        metrics.close()

    def join(self, timeout=None):
        """
        重写线程的join
        """
        if not self._started.is_set():
            return

        super().join()

    def stop_spider(self, signal_num=signal.SIGINT, frame_type=None):
        if signal_num == signal.SIGINT:

            if self._close_spider_num >= self._close_spider_max_num:
                log.warning("[{}/{}]捕获到退出信号. 直接退出程序, 余下请求将会丢失".format(self._close_spider_num,
                                                                      self._close_spider_max_num))
                os._exit(2)
            if self._close_spider_num < self._close_spider_max_num:
                log.info("[{}/{}]捕获到退出信号, 现在发出关闭爬虫信号. 待请求结束将关闭爬虫进程".format(self._close_spider_num,
                                                                           self._close_spider_max_num))
                self._close_spider_num += 1
                self._close_reason = "人为终止"

        self._stop_spider = True
        self._close_reason = "信号终止"
