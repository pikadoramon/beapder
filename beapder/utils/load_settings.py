# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     load_settings
   Description : 动态加载配置参数并实现单例
   Author :       pikadoramon
   date：          2023/7/5
-------------------------------------------------
   Change Activity:
                   2023/7/5:
-------------------------------------------------
"""
import configparser
import copy
import importlib
import json
import logging
import os
import sys
import warnings

from beapder.utils.generic import NULL_DEFAULT, NO_VALUE

log = logging.getLogger()


class SingletonMeta(type):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


module = None


class _defaultSetting(metaclass=SingletonMeta):

    def __init__(self):
        self.attr = dict()
        self.attr_score = dict()

    def reset(self):
        self.attr = dict()
        self.attr_score = dict()


_config = _defaultSetting()


class LoadSettings(metaclass=SingletonMeta):

    def __init__(self):
        self._load_module()

    def _load_module(self):
        global module, _config
        if module is None:
            modulepath = os.environ.get("pythonSettings")
            module = modulepath
        if module is None:
            module = "beapder.setting"
            if os.path.exists("config.ini"):
                config = configparser.ConfigParser()
                config.read('config.ini')
                module = config["project"]["setting"]
            elif os.path.exists("setting.py"):
                module = "setting"
            elif os.path.exists("settings.py"):
                module = "settings"
        log.info("use settings `%s`" % module)
        if len(_config.attr) == 0:
            self.setmodule(module, 2)
            self.apollo_module()

    def reload_module(self, modulepath):
        global _config
        _config.reset()
        log.debug("reload settings: {}".format(modulepath))
        self.setmodule(modulepath, 2)
        self.apollo_module()

    def __getitem__(self, item):
        global _config
        value = self.get(item, NULL_DEFAULT)
        if value is NULL_DEFAULT:
            raise AttributeError("setting has no attribute '{}'".format(item))
        return value

    def setmodule(self, modulepath, score):
        log.debug("importmodule {} {}".format(modulepath, score))
        module_ = importlib.import_module(modulepath)
        valids = (int, str, dict, tuple, list)
        for key in dir(module_):
            # print(key, getattr(module_, key))
            if key.startswith("__"):
                continue
            value = getattr(module_, key)

            if isinstance(value, valids):
                value = getattr(module_, key)
                if value is None:
                    value = NO_VALUE

                if key in _config.attr and _config.attr_score[key] < score:
                    log.warning("你正在试图通过覆盖拉取下来的变量%s覆盖本地文件的变量值, 但由于本地文件变量优先于Apollo上的变量值赋值, 因此覆盖失败" % key)
                    continue

                _config.attr[key] = value
                _config.attr_score[key] = score

    def get(self, name, default=NULL_DEFAULT):
        global _config
        value = _config.attr.get(name, NULL_DEFAULT)
        if value is NULL_DEFAULT:
            value = None
        return value

    def getbool(self, name, default=False):
        """
        Get a setting value as a boolean.

        ``1``, ``'1'``, `True`` and ``'True'`` return ``True``,
        while ``0``, ``'0'``, ``False``, ``'False'`` and ``None`` return ``False``.

        For example, settings populated through environment variables set to
        ``'0'`` will return ``False`` when using this method.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        got = self.get(name, default)
        try:
            return bool(got)
        except ValueError:
            if got in ("True", "true"):
                return True
            if got in ("False", "false"):
                return False
            raise ValueError("Supported values for boolean settings "
                             "are 0/1, True/False, '0'/'1', "
                             "'True'/'False' and 'true'/'false'")

    def getint(self, name, default=0):
        """
        Get a setting value as an int.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        try:
            return int(self.get(name, default))
        except:
            return default

    def getfloat(self, name, default=0.0):
        """
        Get a setting value as a float.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        try:
            return float(self.get(name, default))
        except:
            return default

    def getlist(self, name, default=None):
        """
        Get a setting value as a list. If the setting original type is a list, a
        copy of it will be returned. If it's a string it will be split by ",".

        For example, settings populated through environment variables set to
        ``'one,two'`` will return a list ['one', 'two'] when using this method.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(name, default or [])
        if value is None:
            return default
        if isinstance(value, str):
            value = value.split(',')
        return list(value)

    def getdict(self, name, default=None):
        """
        Get a setting value as a dictionary. If the setting original type is a
        dictionary, a copy of it will be returned. If it is a string it will be
        evaluated as a JSON dictionary. In the case that it is a
        :class:`~scrapy.settings.BaseSettings` instance itself, it will be
        converted to a dictionary, containing all its current settings values
        as they would be returned by :meth:`~scrapy.settings.BaseSettings.get`,
        and losing all information about priority and mutability.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(name, default or {})
        if value is None:
            return default
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value)

    def getpriority(self, name):
        if name == 'project':
            score = 2
        elif name == 'instance':
            score = 3
        else:
            score = 1
        return score

    def update(self, key, value, priority='project', ignore_error=True):
        global _config
        score = self.getpriority(priority)
        if key.endswith("_HOST") or \
                key.endswith("_PORT") or \
                key.endswith("_PWD") or \
                key.endswith("_USER") or \
                key.endswith("_DB") or \
                key.endswith("_PASS"):
            log.warning("overwrite settings: 你正在尝试覆盖一个敏感字段{}, 禁止从命令中配置该值, 若需要则请在配置文件中配置该值. 程序退出".format(key))
            sys.exit(2)

        value_ = _config.attr.get(key, NULL_DEFAULT)
        if value is None:
            value = NO_VALUE

        if value_ is NULL_DEFAULT:
            # print(value, key, attr, priority)

            _config.attr[key] = value
            _config.attr_score[key] = score
            log.debug("write settings: {} {}->{}\n".format(key, '', value))
        else:
            if score < _config.attr_score[key]:
                if ignore_error:
                    log.warning("你正在覆盖一个比你高优先级的设置变量(%s) -> %s, 更新失败. 优先级取值 instance >> project >> 其他值" % (key, value))
                    return
                else:
                    raise ValueError(
                        "你正在覆盖一个比你高优先级的设置变量(%s) -> %s, 更新失败. 优先级取值 instance >> project >> 其他值" % (key, value))
            old_value = _config.attr[key]
            if isinstance(old_value, bool):
                value = bool(value == "true")
            elif isinstance(old_value, int):
                value = int(value)
            elif isinstance(old_value, float):
                value = float(value)
            _config.attr[key] = value
            _config.attr_score[key] = score

    def updateAll(self, other_settings: 'dict', priority='project', ignore_error=False):
        global _config
        score = self.getpriority(priority)
        for key in other_settings:
            value = _config.get(key, NULL_DEFAULT)

            if value is NULL_DEFAULT:
                _config.attr[key] = other_settings[key]
                _config.attr_score[key] = score
            else:
                if score < _config.attr_score[key]:
                    if not ignore_error:
                        raise ValueError("你正在覆盖一个比你高优先级的设置变量(%s), 更新失败. 优先级取值 instance >> project >> 其他值" % key)
                    else:
                        log.warning("你正在覆盖一个比你高优先级的设置变量(%s), 更新失败. 优先级取值 instance >> project >> 其他值" % key)
                        return
                if value is None:
                    value = NO_VALUE
                _config.attr[key] = value
                _config.attr_score[key] = score

    def __setattr__(self, key, value):
        warnings.warn("The set attribute method has been deprecated. Please use the .update method to update "
                      "parameter values.",
                      DeprecationWarning)
        raise AttributeError("Setting attribute '%s' to '%s' has failed." % (key, value))

    def __getattr__(self, item):
        if item == "module":
            return module
        if item == "attr":
            return _config.attr.copy()
        if item in _config.attr:
            value = _config.attr[item]
            if value is NULL_DEFAULT or value is NO_VALUE:
                return value
            return copy.copy(value)
        if item == "score":
            return _config.attr_score.copy()
        raise AttributeError("setting has no attribute '" + item + "'")

    def __contains__(self, item):
        return _config.attr.__contains__(item)
