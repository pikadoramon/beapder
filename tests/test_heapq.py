# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_heapq
   Description :
   Author :       pikadoramon
   date：          2023/7/20
-------------------------------------------------
   Change Activity:
                   2023/7/20:
-------------------------------------------------
"""
__author__ = 'pikadoramon'
import heapq

heap = []


def test_heapq():
    for i in range(10, 1, -1):
        heapq.heappush(heap, i)
        print(heap)
    for i in range(1, 10):
        print(heapq.heappop(heap))


if __name__ == '__main__':
    test_heapq()
