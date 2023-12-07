# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_log_sensitive_text
   Description :
   Author :       pikadoramon
   date：          2023/8/30
-------------------------------------------------
   Change Activity:
                   2023/8/30:
-------------------------------------------------
"""
__author__ = 'pikadoramon'

from beapder.utils.load_settings import LoadSettings

settings = LoadSettings()
settings.update("LOG_COLOR", "false", "instance")
settings.update("LOG_IS_WRITE_TO_FILE", "true", "instance")
print(settings.attr)

if __name__ == '__main__':
    from beapder.utils.log import Log

    log = Log.new_instance()
    log.info("password=")
    log.info("passwordasdfasdfsdf")
    log.info("password=")
