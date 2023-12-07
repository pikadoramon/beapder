# ITEM_PIPELINES = [
#     "beapder.pipelines.console_pipeline.ConsolePipeline"
# ]

# APP_NAME = "64508_appstores"
#
#
# APOLLO = 'test'
# APOLLO_NAMESPACES = ['application', 'db']
# APOLLO_CONFIGURES = [
#     {
#         "APOLLO_NAMESPACES":['app_64508',],
#         "APOLLO_APPID":"apollo-crawler-apps",
#         "APOLLO_SECRET":""
#     }
# ]
from beapder.setting import *

print("hello _world")

MAX_IDLE_TIME = 10
APP_NAME = 'zendao65454'
REDISDB_SERVICE_NAME = 'redis'
DISTRIBUTE_TASK_START_URLS_KEY = 'test.distribute.key.%(name)s.%(app_name)s'
DISTRIBUTE_TASK_START_URLS_BATCH_SIZE = 13
REDIS_TASK_HOSTS = '10.190.32.125:21171'
REDIS_TASK_DB = 2
REDIS_TASK_PWD = 'Like_ghtUpBPeY5!'
DISTRIBUTE_TASK_START_URLS_AS_SET = False
