# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     decorators
   Description :
   Author :       pikadoramon
   date：          2023/7/5
-------------------------------------------------
   Change Activity:
                   2023/7/5:
-------------------------------------------------
"""
import asyncio
import functools
import signal
import time
import traceback
import weakref

from beapder.utils import log

__author__ = 'pikadoramon'


# 放置装饰器相关方法
# 装饰器
class Singleton(object):
    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls(*args, **kwargs)
        return self._instance[self._cls]


class LazyProperty:
    """
    属性延时初始化，且只初始化一次
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value


def log_function_time(func):
    try:

        @functools.wraps(func)  # 将函数的原来属性付给新函数
        def calculate_time(*args, **kw):
            began_time = time.time()
            callfunc = func(*args, **kw)
            end_time = time.time()
            log.debug(func.__name__ + " run time  = " + str(end_time - began_time))
            return callfunc

        return calculate_time
    except:
        log.debug("求取时间无效 因为函数参数不符")
        return func


def run_safe_model(module_name):
    def inner_run_safe_model(func):
        try:

            @functools.wraps(func)  # 将函数的原来属性付给新函数
            def run_func(*args, **kw):
                callfunc = None
                try:
                    callfunc = func(*args, **kw)
                except Exception as e:
                    log.error(module_name + ": " + func.__name__ + " - " + str(e))
                    traceback.print_exc()
                return callfunc

            return run_func
        except Exception as e:
            log.error(module_name + ": " + func.__name__ + " - " + str(e))
            traceback.print_exc()
            return func

    return inner_run_safe_model


def memoizemethod_noargs(method):
    """Decorator to cache the result of a method (without arguments) using a
    weak reference to its object
    """
    cache = weakref.WeakKeyDictionary()

    @functools.wraps(method)
    def new_method(self, *args, **kwargs):
        if self not in cache:
            cache[self] = method(self, *args, **kwargs)
        return cache[self]

    return new_method


def retry(retry_times=3, interval=0):
    """
    普通函数的重试装饰器
    Args:
        retry_times: 重试次数
        interval: 每次重试之间的间隔

    Returns:

    """

    def _retry(func):
        @functools.wraps(func)  # 将函数的原来属性付给新函数
        def wapper(*args, **kwargs):
            for i in range(retry_times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    log.error(
                        "函数 {} 执行失败 重试 {} 次. error {}".format(func.__name__, i + 1, e)
                    )
                    time.sleep(interval)
                    if i + 1 >= retry_times:
                        raise e

        return wapper

    return _retry


def retry_asyncio(retry_times=3, interval=0):
    """
    协程的重试装饰器
    Args:
        retry_times: 重试次数
        interval: 每次重试之间的间隔

    Returns:

    """

    def _retry(func):
        @functools.wraps(func)  # 将函数的原来属性付给新函数
        async def wapper(*args, **kwargs):
            for i in range(retry_times):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    log.error(
                        "函数 {} 执行失败 重试 {} 次. error {}".format(func.__name__, i + 1, e)
                    )
                    await asyncio.sleep(interval)
                    if i + 1 >= retry_times:
                        raise e

        return wapper

    return _retry


def func_timeout(timeout):
    """
    函数运行时间限制装饰器
    注: 不支持window
    Args:
        timeout: 超时的时间

    Eg:
        @set_timeout(3)
        def test():
            ...

    Returns:

    """

    def wapper(func):
        def handle(
                signum, frame
        ):  # 收到信号 SIGALRM 后的回调函数，第一个参数是信号的数字，第二个参数是the interrupted stack frame.
            raise TimeoutError

        def new_method(*args, **kwargs):
            signal.signal(signal.SIGALRM, handle)  # 设置信号和回调函数
            signal.alarm(timeout)  # 设置 timeout 秒的闹钟
            r = func(*args, **kwargs)
            signal.alarm(0)  # 关闭闹钟
            return r

        return new_method

    return wapper
