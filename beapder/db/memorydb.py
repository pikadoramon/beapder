# -*- coding: utf-8 -*-
"""
Created on 2020/4/21 11:42 PM
---------
@summary: 基于内存的队列，代替redis
---------
@author: Boris
@email: boris_liu@foxmail.com
"""
import queue
from queue import PriorityQueue
from beapder.utils.log import log


class MemoryDB:
    def __init__(self, task_max_cached_size=100):
        self.priority_queue = PriorityQueue(maxsize=task_max_cached_size)
        self.unfinished_tasks = 0

    def add(self, item, ignore_max_size=False):
        """
        添加任务
        :param item: 数据: 支持小于号比较的类 或者 （priority, item）
        :param ignore_max_size: queue满时是否等待，为True时无视队列的maxsize，直接往里塞
        :return:
        """
        if ignore_max_size:
            self.priority_queue._put(item)
            self.unfinished_tasks += 1
        else:
            self.priority_queue.put(item)

    def get(self):
        """
        获取任务
        :return:
        """
        try:
            item = self.priority_queue.get(timeout=1)
            return item
        except:
            return

    def empty(self):
        return self.priority_queue.empty()

    def size(self):
        return self.priority_queue.qsize()


# 用于进行分布式存放任务
class SharedDB:

    def __init__(self, local_db, shared_db,
                 local_max_cached_size=100,
                 has_checkpoint=True):
        self._queue = local_db
        self._shared_queue = shared_db
        self.unfinished_tasks = 0
        self.local_max_cached_size = local_max_cached_size
        self._has_any_item = True
        self._has_checkpoint = has_checkpoint

    def add(self, item, ignore_max_size=False):
        """
        添加任务
        :param item: 数据: 支持小于号比较的类 或者 （priority, item）
        :param ignore_max_size: queue满时是否等待，为True时无视队列的maxsize，直接往里塞
        :return:
        """
        if ignore_max_size:
            self._has_any_item = True
            self._queue.put(item)
            return

        if self._queue.qsize() < self.local_max_cached_size:
            self._has_any_item = True
            self._queue.put(item)
            return

        self._shared_queue.put(item)
        return

    def get(self):
        """
        获取任务
        :return:
        """
        try:
            if self._has_any_item:
                item = self._queue.get(timeout=1)
            else:
                item = self._shared_queue.get()
            return item
        except queue.Empty:
            self._has_any_item = False
            item = self._shared_queue.get()
            return item

    def empty(self):
        return self._queue.empty() and self._shared_queue.empty()

    def size(self):
        return self._queue.qsize() + self._shared_queue.size()

    def local_size(self):
        return self._queue.qsize()

    def shared_size(self):
        return self._shared_queue.size()

    def __del__(self):
        if self._has_checkpoint:
            self.set_checkpoint()

    def set_checkpoint(self):
        local_size = self.local_size()
        while self.local_size() > 0:
            try:
                item = self._queue.get(timeout=1)
                if item:
                    self._shared_queue.add(item, True)
            except:
                pass
        log.info("set checkpoint %d" % local_size)
