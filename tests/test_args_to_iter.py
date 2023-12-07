# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_args_to_iter
   Description :
   Author :       pikadoramon
   date：          2023/7/11
-------------------------------------------------
   Change Activity:
                   2023/7/11:
-------------------------------------------------
"""
__author__ = 'pikadoramon'

from beapder.utils.rule import arg_to_iter


def yield_res():
    for i in range(10):
        yield i


def return_res():
    return list(range(10))


def return_int():
    return 1


def return_none():
    return None


if __name__ == '__main__':
    for i in arg_to_iter(yield_res()):
        print("yield_res", i)

    for i in arg_to_iter(return_res()):
        print("return_res", i)

    for i in arg_to_iter(return_int()):
        print("return_int", i)

    for i in arg_to_iter(return_none()):
        print("return_none", i)
