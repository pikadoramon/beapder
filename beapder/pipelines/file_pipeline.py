# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     json_pipeline
   Description :
   Author :       pikadoramon
   date：          2023/9/11
-------------------------------------------------
   Change Activity:
                   2023/9/11:
-------------------------------------------------
"""
__author__ = 'pikadoramon'
import json
from beapder.pipelines import BasePipeline
from typing import Dict, List, Tuple
from beapder.utils.log import log
from threading import Lock


class JsonPipeline(BasePipeline):
    """
    pipeline 是单线程的，批量保存数据的操作，不建议在这里写网络请求代码，如下载图片等
    """

    def __init__(self):
        self.fp = None
        self.file_name = None
        self.flush_size = 1 << 10
        self.current_size = 0
        self._wlock = Lock()

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        if self.fp is None:
            self.file_name = table if table.endswith(".json") else table + ".json"
            self.fp = open(self.file_name, "ab")
        for item in items:
            string = json.dumps(item, ensure_ascii=False, skipkeys=True, default=str, )
            self.current_size += self.fp.write(string.encode("utf-8"))
            self.current_size += self.fp.write(b"\n")
            if self.current_size > self.flush_size:
                self.fp.flush()
                self.current_size = 0

        log.info("【JSON文件输出】共导出 %s 条数据 到 %s" % (len(items), self.file_name))
        return True

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
        log.info("【JSON文件输出】共导出 %s 条数据 到 %s 不进行更新操作" % (len(items), self.file_name))
        return True

    def close(self):
        with self._wlock as _:
            try:
                if self.fp and self.file_name:
                    log.info("【JSON文件输出】准备关闭文件 fp={} fname={}".format(self.fp, self.file_name))
                    self.fp.close()
                    log.info("【JSON文件输出】已经关闭文件 fp={} fname={}".format(self.fp, self.file_name))
                self.fp = None
            except Exception as e:
                log.warning("【JSON文件输出】关闭文件失败 %s. 错误原因:%s" % (self.file_name, e))

    def __del__(self):
        self.close()
