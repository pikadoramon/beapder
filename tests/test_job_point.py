# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_job_point
   Description :
   Author :       pikadoramon
   date：          2023/12/6
-------------------------------------------------
   Change Activity:
                   2023/12/6:
-------------------------------------------------
"""
import json

__author__ = 'pikadoramon'
from beapder.utils.load_settings import LoadSettings
from beapder.extensions.job_points import DefaultJobPoint
from beapder.network.request import Request
from beapder.utils.tools import import_cls

if __name__ == '__main__':
    settings = LoadSettings()
    settings.update("REQUEST_JOB_POINT_CONFIGS",
                    {"path": r"D:\projects\gitopensource\beapder\tests\air-spider\test_job_point", "chunksize": 100})
    settings.update("REQUEST_JOB_POINT_SERIALIZE", "pickle.dumps")
    settings.update("REQUEST_JOB_POINT_DESERIALIZE", "pickle.loads")
    job_point = DefaultJobPoint.from_settings(settings)
    # for i in range(1000):
    #     t = Request(method="GET", url="ddddd").to_dict
    #     r = job_point.save_jobpoint(t)
    for i in range(1600):
        d = job_point.recovery_jobpoints(2)
        print(d)
        if d is None:
            break
    job_point.close()
