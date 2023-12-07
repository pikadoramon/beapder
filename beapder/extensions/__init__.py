# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     __init__
   Description :
   Author :       pikadoramon
   date：          2023/7/21
-------------------------------------------------
   Change Activity:
                   2023/7/21:
-------------------------------------------------
"""
from abc import abstractmethod

__author__ = 'pikadoramon'


class _ExtensionMeta(type):
    """
    Metaclass to check queue classes against the necessary interface
    """

    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))  # pylint: disable=no-value-for-parameter

    def __subclasscheck__(cls, subclass):
        return (
                hasattr(subclass, "close_spider")
                and callable(subclass.close_spider)
                and hasattr(subclass, "open_spider")
                and callable(subclass.open_spider)
                and hasattr(subclass, "from_settings")
                and callable(subclass.from_settings)
        )


class BaseExtension(metaclass=_ExtensionMeta):

    @classmethod
    @abstractmethod
    def from_settings(cls, settings) -> object:
        raise NotImplementedError()

    @abstractmethod
    def open_spider(self, spider):
        raise NotImplementedError()

    @abstractmethod
    def close_spider(self, spider):
        raise NotImplementedError()
