# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_thread_timer
   Description :
   Author :       pikadoramon
   date：          2023/7/14
-------------------------------------------------
   Change Activity:
                   2023/7/14:
-------------------------------------------------
"""
import time

__author__ = 'pikadoramon'

import threading


class A:

    def __init__(self):
        print("A.__init__")

    def get(self):
        print("A.get target_func", time.time())

    def __del__(self):
        print("A.del")


class HeartbeatThread(threading.Thread):
    def __init__(self, interval, target_func):
        super().__init__()
        self.interval = interval
        self.target_func = target_func
        self._stop_event = threading.Event()
        self._timer = None

    def run(self):

        self._timer = threading.Timer(3, 2)
        self._timer.start()

    def stop(self):
        self._stop_event.set()
        if self._timer:
            self._timer.cancel()

    def _heartbeat(self):
        if not self._stop_event.is_set():
            print("_heartbeat")
            self.target_func()
            self._timer = threading.Timer(self.interval, self._heartbeat)
            self._timer.start()


def target_func():
    print("target_func", time.time())


if __name__ == '__main__':
    d = threading.Timer(2, target_func)
    d.setDaemon(True)
    d.start()
    print(time.time(), d.getName(), d.ident)
    while True:
        time.sleep(15)
        break
