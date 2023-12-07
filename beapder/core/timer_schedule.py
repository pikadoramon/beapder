# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     timer_schedule
   Description :
   Author :       pikadoramon
   date：          2023/7/20
-------------------------------------------------
   Change Activity:
                   2023/7/20:
-------------------------------------------------
"""
import heapq
import logging
import threading
import weakref
from collections import namedtuple
from beapder.utils import tools
from beapder.utils.log import log

timer_weakref = weakref.WeakValueDictionary()
callback_info = namedtuple("CallInfo", ("interval", "cid", "fn", "method"))


class Timer(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(Timer, self).__init__(*args, **kwargs)
        self.heap = []
        self.next_trigger_time = None
        self.last_trigger_time = None
        self.recent_trigger_time = None
        self._lock = threading.Lock()

    def timer(self):

        while True:
            while len(self.heap) < 1:
                tools.delay_time(1)

            next_timer, cbinfo = self.heap[0]
            remaining_time = next_timer - tools.get_current_timestamp()
            self.next_trigger_time = next_timer

            if remaining_time <= 0 or cbinfo.cid not in timer_weakref:
                with self._lock as _:
                    heapq.heappop(self.heap)
            if remaining_time > 0:
                tools.delay_time(1)
                continue

            if cbinfo.cid in timer_weakref:
                try:
                    _callback = timer_weakref[cbinfo.cid]
                    self.last_trigger_time = tools.get_current_timestamp()
                    if cbinfo.method is None:
                        _callback()
                    else:
                        getattr(_callback, cbinfo.method)()
                    current_time = tools.get_current_timestamp()
                    timer = (current_time + cbinfo.interval, cbinfo)
                    heapq.heappush(self.heap, timer)
                except Exception as e:
                    logging.error("[{}] Error executing the timer callback function: {}".format(cbinfo.fn, e))

    def run(self) -> None:
        self.timer()


timer_scheduler = Timer(name="timer-schedule", daemon=True)
timer_scheduler.setDaemon(True)


def add_schedule(interval, callback):
    global callback_info, timer_weakref, timer_scheduler
    assert isinstance(interval, (int, float)) and interval > 1, f"interval {interval} must be greater than 1"
    assert callable(callback), "the callback function must be callable"

    if not hasattr(callback, "__name__"):
        fn = callback.__class__.__name__
    else:
        fn = callback.__name__

    _cid = id(callback)
    if _cid in timer_weakref:
        log.warning(f"加入调度器失败. "
                    f"方法 {fn} 已经在可执行队列中")
        return

    timer_weakref[_cid] = callback
    _cinfo = callback_info(interval, _cid, fn, None)
    current_time = tools.get_current_timestamp()

    timer = (current_time + interval, _cinfo)
    timer_scheduler.recent_trigger_time = timer[0] if timer_scheduler.recent_trigger_time is None else min(
        timer_scheduler.recent_trigger_time,
        timer[0])
    heapq.heappush(timer_scheduler.heap, timer)


def add_schedule_obj(interval, obj, method):
    global callback_info, timer_weakref, timer_scheduler
    assert isinstance(interval, (int, float)) and interval > 1, f"interval {interval} must be greater than 1"
    _callback = getattr(obj, method)
    assert callable(_callback), f"The method {method} of object {type(obj).__name__} is not callable"

    _cid = id(obj)
    fn = "{obj}.{method}".format(method=method, obj=obj.__class__.__name__)
    if _cid in timer_weakref:
        log.warning(f"加入调度器失败. "
                    f"方法 {fn} 已经在可执行队列中")
        return

    timer_weakref[_cid] = obj
    current_time = tools.get_current_timestamp()

    _cinfo = callback_info(interval, _cid, fn, method)
    timer = (current_time + interval, _cinfo)
    timer_scheduler.recent_trigger_time = timer[0] if timer_scheduler.recent_trigger_time is None else min(
        timer_scheduler.recent_trigger_time,
        timer[0])
    heapq.heappush(timer_scheduler.heap, timer)


def remove_schedule(callback):
    global timer_weakref
    _cid = id(callback)
    if _cid in timer_weakref:
        timer_weakref.pop(_cid)


def remove_schedule_obj(obj):
    global timer_weakref
    _cid = id(obj)
    if _cid in timer_weakref:
        timer_weakref.pop(_cid)
