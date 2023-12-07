# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     redis_list_queue
   Description :
   Author :       pikadoramon
   date：          2023/7/26
-------------------------------------------------
   Change Activity:
                   2023/7/26:
-------------------------------------------------
"""
from typing import Optional
from beapder.utils.rule import arg_to_iter

__author__ = 'pikadoramon'
import redis


class RedisListQueue:

    def __init__(self, db, table):
        self._db: redis.Redis = db
        self._table = table

    def push(self, item: bytes) -> None:
        if item is None:
            return None
        self._db.execute_command("RPUSH", self._table,
                                 *[e for e in arg_to_iter(item)])

    def pop(self) -> Optional[bytes]:
        return self._db.execute_command("LPOP", self._table)

    def peek(self) -> Optional[bytes]:
        return self._db.execute_command("LINDEX", self._table, "0")

    def close(self) -> None: pass

    def __len__(self):
        return self._db.execute_command("LLEN", self._table) or 0


class RedisSortedListQueue:

    def __init__(self, db, table, push_call, pop_call):
        self._db: redis.Redis = db
        self._table = table
        self.push_call = push_call
        self.pop_call = pop_call

    def push(self, item: bytes) -> None:
        if item is None:
            return None
        data = [e for e in arg_to_iter(item)]
        if self.push_call is None:
            score = [300 for _ in data]
        else:
            score = [self.push_call(e) for e in data]
        self._db.execute_command("ZADD", self._table,
                                 *[e for e in arg_to_iter(item)])

    def pop(self) -> Optional[bytes]:
        return self._db.execute_command("LPOP", self._table)

    def peek(self) -> Optional[bytes]:
        return self._db.execute_command("LINDEX", self._table, "0")

    def close(self) -> None:
        pass

    def __len__(self):
        return self._db.execute_command("LLEN", self._table) or 0
