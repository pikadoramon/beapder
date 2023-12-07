# -*- coding: utf-8 -*-
"""
Created on 2018-07-29 22:48:30
---------
@summary: 导出数据
---------
@author: Boris
@email:  boris_liu@foxmail.com
"""
import traceback
from typing import Dict, List, Tuple

import beapder.utils.tools as tools
from beapder.db.mysqldb import MysqlDB
from beapder.pipelines import BasePipeline
from beapder.utils.log import log


class MysqlPipeline(BasePipeline):
    settings = None

    def __init__(self):
        self._to_db = None

    @classmethod
    def from_settings(cls, settings):
        cls.settings = settings
        return cls()

    @property
    def to_db(self):
        if not self._to_db:
            if self.settings and self.settings.get("PERSIST_DB_TYPE") == "mysql":
                self._to_db = MysqlDB(ip=self.settings["PERSIST_DB_HOST"],
                                      port=self.settings["PERSIST_DB_PORT"],
                                      db=self.settings["PERSIST_DB_DBNAME"],
                                      user_name=self.settings["PERSIST_DB_USER"],
                                      user_pass=self.settings["PERSIST_DB_PWD"])
                log.info("use pipeline MysqlPipeline[host=%s port=%s db=%s user=%s]"
                         % (self.settings["PERSIST_DB_HOST"],
                            self.settings["PERSIST_DB_PORT"],
                            self.settings["PERSIST_DB_DBNAME"],
                            self.settings["PERSIST_DB_USER"],
                            )
                         )
            else:
                self._to_db = MysqlDB()
                log.info("use pipeline MysqlPipeline[host=%s port=%s db=%s user=%s]"
                         % (self.settings["MYSQL_IP"],
                            self.settings["MYSQL_PORT"],
                            self.settings["MYSQL_DB"],
                            self.settings["MYSQL_USER_NAME"],
                            )
                         )

        return self._to_db

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        add_count = None
        try:
            sql, datas = tools.make_batch_sql(table, items)
            add_count = self.to_db.add_batch(sql, datas)
            datas_size = len(datas)
            if add_count:
                log.info(
                    "共导出 %s 条数据 到 %s, 重复 %s 条" % (datas_size, table, datas_size - add_count)
                )
        except Exception as e:
            log.error("导出 %s 条数据到 %s 失败, 失败原因: %s" % (len(items), table, e))
            log.error(traceback.format_exc())

        return add_count != None

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据
        Args:
            table: 表名
            items: 数据，[{},{},...]
            update_keys: 更新的字段, 如 ("title", "publish_time")

        Returns: 是否更新成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        update_count = None
        try:
            sql, datas = tools.make_batch_sql(
                table, items, update_columns=update_keys or list(items[0].keys())
            )
            update_count = self.to_db.add_batch(sql, datas)
            if update_count:
                msg = "共更新 %s 条数据 到 %s" % (update_count // 2, table)
                if update_keys:
                    msg += " 更新字段为 {}".format(update_keys)
                log.info(msg)
        except Exception as e:
            log.error("更新 %s 条数据到 %s 失败, 失败原因: %s" % (len(items), table, e))
            log.error(traceback.format_exc())

        return update_count != None
