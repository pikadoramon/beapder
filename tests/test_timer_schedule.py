# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_timer_schedule
   Description :
   Author :       pikadoramon
   date：          2023/7/20
-------------------------------------------------
   Change Activity:
                   2023/7/20:
-------------------------------------------------
"""
import logging

from beapder.core.timer_schedule import add_schedule_obj, add_schedule, remove_schedule, \
    remove_schedule_obj
from beapder.core.timer_schedule import timer_scheduler
from beapder.utils import tools

__author__ = 'pikadoramon'

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)


def print_5():
    logging.info("print 5")


def print_6():
    logging.info("print 6")


class A:

    def __init__(self):
        pass

    def log(self):
        logging.info("print log")
        # raise ValueError("故意的")

    def __call__(self, *args, **kwargs):
        logging.info("call")


if __name__ == '__main__':
    timer_scheduler.setDaemon(True)
    # schedule(5, print_5)
    # schedule(15, print_6)
    #
    d = A()
    add_schedule(3, print_5)
    add_schedule(5, print_6)
    # schedule(2, d)
    add_schedule_obj(10, d, "log")
    timer_scheduler.start()

    tools.delay_time(10)
    remove_schedule(print_5)
    remove_schedule(print_6)
    tools.delay_time(10)
    # remove_schedule_obj(d)
    del d
    add_schedule(2, print_5)
    tools.delay_time(10)
    print(d)
#
