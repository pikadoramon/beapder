# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     generic
   Description : 通用型工具
   Author :       pikadoramon
   date：          2023/7/5
-------------------------------------------------
   Change Activity:
                   2023/7/5:
-------------------------------------------------
"""
import collections
import contextlib
import inspect
import itertools
import re

from pytz import LazyList

__author__ = 'pikadoramon'

NULL_DEFAULT = object()
NO_VALUE = object()
DEFAULT_FUNCTION = lambda x: x


def int_or_none(v, scale=1, default=None, get_attr=None, invscale=1):
    if get_attr and v is not None:
        v = getattr(v, get_attr, None)
    try:
        return int(v) * invscale // scale
    except (ValueError, TypeError, OverflowError):
        return default


def str_or_none(v, default=None):
    return default if v is None else str(v)


def str_to_int(int_str):
    """ A more relaxed version of int_or_none """
    if isinstance(int_str, int):
        return int_str
    elif isinstance(int_str, str):
        int_str = re.sub(r'[,\.\+]', '', int_str)
        return int_or_none(int_str)


def float_or_none(v, scale=1, invscale=1, default=None):
    if v is None:
        return default
    try:
        return float(v) * invscale / scale
    except (ValueError, TypeError):
        return default


def bool_or_none(v, default=None):
    return v if isinstance(v, bool) else default


def strip_or_none(v, default=None):
    return v.strip() if isinstance(v, str) else default


def url_or_none(url):
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    return url if re.match(r'^(?:(?:https?|rt(?:m(?:pt?[es]?|fp)|sp[su]?)|mms|ftps?):)?//', url) else None


def try_call(*funcs, expected_type=None, args=[], kwargs={}):
    for f in funcs:
        try:
            val = f(*args, **kwargs)
        except (AttributeError, KeyError, TypeError, IndexError, ValueError, ZeroDivisionError):
            pass
        else:
            if expected_type is None or isinstance(val, expected_type):
                return val


def variadic(x, allowed_types=(str, bytes, dict)):
    return x if isinstance(x, collections.abc.Iterable) and not isinstance(x, allowed_types) else (x,)


def traverse_obj(
        obj, *paths, default=NULL_DEFAULT, expected_type=None, get_all=True,
        casesense=True, is_user_input=False, traverse_string=False):
    """
    Safely traverse nested `dict`s and `Sequence`s

    >>> obj = [{}, {"key": "value"}]
    >>> traverse_obj(obj, (1, "key"))
    "value"

    Each of the provided `paths` is tested and the first producing a valid result will be returned.
    The next path will also be tested if the path branched but no results could be found.
    Supported values for traversal are `Mapping`, `Sequence` and `re.Match`.
    Unhelpful values (`{}`, `None`) are treated as the absence of a value and discarded.

    The paths will be wrapped in `variadic`, so that `'key'` is conveniently the same as `('key', )`.

    The keys in the path can be one of:
        - `None`:           Return the current object.
        - `set`:            Requires the only item in the set to be a type or function,
                            like `{type}`/`{func}`. If a `type`, returns only values
                            of this type. If a function, returns `func(obj)`.
        - `str`/`int`:      Return `obj[key]`. For `re.Match`, return `obj.group(key)`.
        - `slice`:          Branch out and return all values in `obj[key]`.
        - `Ellipsis`:       Branch out and return a list of all values.
        - `tuple`/`list`:   Branch out and return a list of all matching values.
                            Read as: `[traverse_obj(obj, branch) for branch in branches]`.
        - `function`:       Branch out and return values filtered by the function.
                            Read as: `[value for key, value in obj if function(key, value)]`.
                            For `Sequence`s, `key` is the index of the value.
                            For `re.Match`es, `key` is the group number (0 = full match)
                            as well as additionally any group names, if given.
        - `dict`            Transform the current object and return a matching dict.
                            Read as: `{key: traverse_obj(obj, path) for key, path in dct.items()}`.

        `tuple`, `list`, and `dict` all support nested paths and branches.

    @params paths           Paths which to traverse by.
    @param default          Value to return if the paths do not match.
                            If the last key in the path is a `dict`, it will apply to each value inside
                            the dict instead, depth first. Try to avoid if using nested `dict` keys.
    @param expected_type    If a `type`, only accept final values of this type.
                            If any other callable, try to call the function on each result.
                            If the last key in the path is a `dict`, it will apply to each value inside
                            the dict instead, recursively. This does respect branching paths.
    @param get_all          If `False`, return the first matching result, otherwise all matching ones.
    @param casesense        If `False`, consider string dictionary keys as case insensitive.

    The following are only meant to be used by YoutubeDL.prepare_outtmpl and are not part of the API

    @param is_user_input    Whether the keys are generated from user input.
                            If `True` strings get converted to `int`/`slice` if needed.
    @param traverse_string  Whether to traverse into objects as strings.
                            If `True`, any non-compatible object will first be
                            converted into a string and then traversed into.
                            The return value of that path will be a string instead,
                            not respecting any further branching.


    @returns                The result of the object traversal.
                            If successful, `get_all=True`, and the path branches at least once,
                            then a list of results is returned instead.
                            If no `default` is given and the last path branches, a `list` of results
                            is always returned. If a path ends on a `dict` that result will always be a `dict`.

    提供的paths中的每个路径将被测试，第一个产生有效结果的路径将被返回。如果路径分支但没有找到结果，则将测试下一个路径。
    支持遍历的值有 Mapping（映射，如字典）、Sequence（序列，如列表）和 re.Match（正则表达式匹配结果）。不帮助的值（{}、None）被视为缺少值并被丢弃。
    路径将被包装在 variadic 中，所以 'key' 方便地等同于 ('key', )。
    路径中的键可以是以下之一：

    None：返回当前对象。
    set：要求集合中的唯一项为类型或函数，例如 {type}/{func}。如果是 type，则返回该类型的值。如果是函数，则返回 func(obj) 的结果。
    str/int：返回 obj[key]。对于 re.Match，返回 obj.group(key) 的结果。
    slice：分支并返回 obj[key] 中的所有值。
    Ellipsis：分支并返回所有值的列表。
    tuple/list：分支并返回所有匹配的值的列表。读作：[traverse_obj(obj, branch) for branch in branches]。
    function：分支并返回由函数过滤的值。读作：[value for key, value in obj if function(key, value)]。对于 Sequence，key 是值的索引。对于 re.Match，key 是组号（0 表示完整匹配），也可以是其他给定的组名。
    dict：转换当前对象并返回匹配的字典。读作：{key: traverse_obj(obj, path) for key, path in dct.items()}。
    tuple、list 和 dict 都支持嵌套路径和分支。

    @param paths：要遍历的路径。
    @param default：如果路径不匹配，则返回的值。如果路径的最后一个键是 dict，则它将应用于其中的每个值，按深度优先。如果使用嵌套的 dict 键，请尽量避免使用此选项。
    @param expected_type：如果是 type，只接受最终值为此类型的结果。如果是其他可调用对象，尝试在每个结果上调用该函数。如果路径的最后一个键是 dict，则它将递归应用于其中的每个值，但会考虑分支路径。
    @param get_all：如果为 False，返回第一个匹配结果；否则返回所有匹配结果。
    @param casesense：如果为 False，将字符串字典键视为不区分大小写。

    以下仅供 YoutubeDL.prepare_outtmpl 使用，不属于 API 的一部分。

    @param is_user_input：键是否由用户输入生成。如果为 True，字符串将根据需要转换为 int/slice。
    @param traverse
    """

    is_sequence = lambda x: isinstance(x, collections.abc.Sequence) and not isinstance(x, (str, bytes))
    casefold = lambda k: k.casefold() if isinstance(k, str) else k

    if isinstance(expected_type, type):
        type_test = lambda val: val if isinstance(val, expected_type) else None
    else:
        type_test = lambda val: try_call(expected_type or DEFAULT_FUNCTION, args=(val,))

    def apply_key(key, obj, is_last):
        branching = False
        result = None

        if obj is None and traverse_string:
            pass

        elif key is None:
            result = obj

        elif isinstance(key, set):
            assert len(key) == 1, 'Set should only be used to wrap a single item'
            item = next(iter(key))
            if isinstance(item, type):
                if isinstance(obj, item):
                    result = obj
            else:
                result = try_call(item, args=(obj,))

        elif isinstance(key, (list, tuple)):
            branching = True
            result = itertools.chain.from_iterable(
                apply_path(obj, branch, is_last)[0] for branch in key)

        elif key is ...:
            branching = True
            if isinstance(obj, collections.abc.Mapping):
                result = obj.values()
            elif is_sequence(obj):
                result = obj
            elif isinstance(obj, re.Match):
                result = obj.groups()
            elif traverse_string:
                branching = False
                result = str(obj)
            else:
                result = ()

        elif callable(key):
            branching = True
            if isinstance(obj, collections.abc.Mapping):
                iter_obj = obj.items()
            elif is_sequence(obj):
                iter_obj = enumerate(obj)
            elif isinstance(obj, re.Match):
                iter_obj = itertools.chain(
                    enumerate((obj.group(), *obj.groups())),
                    obj.groupdict().items())
            elif traverse_string:
                branching = False
                iter_obj = enumerate(str(obj))
            else:
                iter_obj = ()

            result = (v for k, v in iter_obj if try_call(key, args=(k, v)))
            if not branching:  # string traversal
                result = ''.join(result)

        elif isinstance(key, dict):
            iter_obj = ((k, _traverse_obj(obj, v, False, is_last)) for k, v in key.items())
            result = {
                         k: v if v is not None else default for k, v in iter_obj
                         if v is not None or default is not NULL_DEFAULT
                     } or None

        elif isinstance(obj, collections.abc.Mapping):
            result = (obj.get(key) if casesense or (key in obj) else
                      next((v for k, v in obj.items() if casefold(k) == key), None))

        elif isinstance(obj, re.Match):
            if isinstance(key, int) or casesense:
                with contextlib.suppress(IndexError):
                    result = obj.group(key)

            elif isinstance(key, str):
                result = next((v for k, v in obj.groupdict().items() if casefold(k) == key), None)

        elif isinstance(key, (int, slice)):
            if is_sequence(obj):
                branching = isinstance(key, slice)
                with contextlib.suppress(IndexError):
                    result = obj[key]
            elif traverse_string:
                with contextlib.suppress(IndexError):
                    result = str(obj)[key]

        return branching, result if branching else (result,)

    def lazy_last(iterable):
        iterator = iter(iterable)
        prev = next(iterator, NULL_DEFAULT)
        if prev is NULL_DEFAULT:
            return

        for item in iterator:
            yield False, prev
            prev = item

        yield True, prev

    def apply_path(start_obj, path, test_type):
        objs = (start_obj,)
        has_branched = False

        key = None
        for last, key in lazy_last(variadic(path, (str, bytes, dict, set))):
            if is_user_input and isinstance(key, str):
                if key == ':':
                    key = ...
                elif ':' in key:
                    key = slice(*map(int_or_none, key.split(':')))
                elif int_or_none(key) is not None:
                    key = int(key)

            if not casesense and isinstance(key, str):
                key = key.casefold()

            if __debug__ and callable(key):
                # Verify function signature
                inspect.signature(key).bind(None, None)

            new_objs = []
            for obj in objs:
                branching, results = apply_key(key, obj, last)
                has_branched |= branching
                new_objs.append(results)

            objs = itertools.chain.from_iterable(new_objs)

        if test_type and not isinstance(key, (dict, list, tuple)):
            objs = map(type_test, objs)

        return objs, has_branched, isinstance(key, dict)

    def _traverse_obj(obj, path, allow_empty, test_type):
        results, has_branched, is_dict = apply_path(obj, path, test_type)
        results = LazyList(item for item in results if item not in (None, {}))
        if get_all and has_branched:
            if results:
                return results.exhaust()
            if allow_empty:
                return [] if default is NULL_DEFAULT else default
            return None

        return results[0] if results else {} if allow_empty and is_dict else None

    for index, path in enumerate(paths, 1):
        result = _traverse_obj(obj, path, index == len(paths), True)
        if result is not None:
            return result

    return None if default is NULL_DEFAULT else default
