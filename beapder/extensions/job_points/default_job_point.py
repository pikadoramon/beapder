# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     disk_job_point
   Description : 用途进行断点续爬的操作。将buffer中的request写入本地job-point 在下次启动时，读取job-point进行实例化重新采集
   Author :       pikadoramon
   date：          2023/12/6
-------------------------------------------------
   Change Activity:
                   2023/12/6:
-------------------------------------------------
"""
from beapder.extensions import BaseExtension

__author__ = 'pikadoramon'

import os
from beapder.utils.tools import import_cls
from beapder.utils.rule import arg_to_iter
from beapder.utils.log import log


class DefaultJobPointExtension(BaseExtension):
    _serialize_call = None
    _deserialize_call = None
    _job_point = None
    _job_point_backend = None

    def open_spider(self, spider):
        if len(self.get_job_point):
            log.info("已经启动JobPoint功能, 当前任务{}条. 若想恢复当前JobPoint任务, "
                     "请在start-request中调用recovery_jobpoints/recovery_jobpoint方法".format(len(self.get_job_point)))
        else:
            log.info("已经启动JobPoint功能, 当前任务0条. 无需恢复任务")

    def close_spider(self, spider):
        count = 0
        try:
            while spider.pending_buffer.size() > 0:
                req = spider.pending_buffer.get()
                self.save_jobpoint(req)
                count += 1
            log.info("job-point保存请求request总计 {}条".format(count))
        except Exception as e:
            log.error("job-point保存请求request. 已完成保存request总计 {}条. 失败原因: {}".format(count, e))
        finally:
            self.close()

    @property
    def get_job_point(self):
        return self._job_point

    def save_jobpoints(self, requests):
        for request in arg_to_iter(requests):
            self.save_jobpoint(request)

    def save_jobpoint(self, request):
        self._job_point.push(self._serialize_call(request))

    def recovery_jobpoints(self, count):
        if len(self._job_point) == 0:
            return None

        request_list = []
        while len(self._job_point) > 0 and count > 0:
            count -= 1
            req = self._job_point.pop()
            if req is None:
                break
            request_list.append(self._deserialize_call(req))
        return request_list

    def recovery_jobpoint(self):
        if len(self._job_point) == 0:
            return None

        req = self._job_point.pop()
        if req is None:
            return None
        return self._deserialize_call(req)

    @classmethod
    def from_settings(cls, settings):

        ser = settings.get("REQUEST_JOB_POINT_SERIALIZE")
        dser = settings.get("REQUEST_JOB_POINT_DESERIALIZE")
        config = settings.get("REQUEST_JOB_POINT_CONFIGS")
        job_point_backend = config.get("path")
        if not os.path.exists(os.path.dirname(job_point_backend)):
            raise FileNotFoundError("job_point_backend={} 未创建数据目录 dir={}".format(job_point_backend,
                                                                                 os.path.dirname(job_point_backend)))

        backend_clz = settings.get("REQUEST_JOB_POINT_BACKEND")

        log.info("实例JobPoint {}".format(settings.get("REQUEST_JOB_POINT_CLASS")))
        log.info("实例JobPoint参数 backend={} ser={} dser={} config={}".format(backend_clz, ser, dser, config))
        obj = cls()
        obj._serialize_call = import_cls(ser)
        obj._deserialize_call = import_cls(dser)
        obj._job_point_backend = job_point_backend
        obj._job_point = import_cls(backend_clz)() if config is None or len(config) == 0 \
            else import_cls(backend_clz)(**config)
        return obj

    def close(self):
        log.info("执行JobPoint Close 操作 {}".format(self._job_point))
        if self._job_point is None:
            return
        if len(self._job_point) == 0 and os.path.exists(self._job_point_backend):
            try:
                self._job_point.close()
                if os.path.isfile(self._job_point_backend):
                    os.remove(self._job_point_backend)
                elif os.path.isdir(self._job_point_backend):
                    os.removedirs(self._job_point_backend)
            except Exception as e:
                log.error("关闭JobPoint失败, 原因: {}".format(e))
        else:
            try:
                self._job_point.close()
                self._job_point = None
            except Exception as e:
                log.error("关闭JobPoint失败, 原因: {}".format(e))

    def flush(self):
        pass
