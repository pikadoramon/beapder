# -*- coding: utf-8 -*-
"""
Created on 2023-09-11 17:09:02
---------
@summary:
这里是经过优化的启动入口, 输入实例化路径，运行相应的runner。类似scrapy的 `scrapy crawl <spider_name>`
通常该文件无需更改, 若需要更改请说明理由 @pikadoramon
爬虫应用入口
---------
@author: Administrator
"""
import os
# 由于项目运行目录为执行器的运行目录,为确保moduls依赖正常,请确保代码已正确配置依赖目录
# 可以将以下文件添加在py文件最上方
#
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# #2.添加项目的绝对路径，使解释器能找到这个路径
sys.path.append(os.path.dirname(BASE_DIR))

# #1.获得项目的绝对路径,必须经你验证绝对路径的正确性，sys，os必须先导入
import click
from beapder.utils.tools import import_cls
import warnings

warnings.filterwarnings("ignore")

from beapder.utils.load_settings import LoadSettings
from beapder.utils.log import Log

settings = None
logger = None


@click.group()
@click.option("--env_setting", "-e", help="通过环境变量设置settings路径", type=str, default="")
@click.option("--local_setting", "-s", multiple=True, help="命令行覆盖settings参数", type=str)
@click.option("--global_setting", "-g", help="全局配置文件", type=str, default="")
@click.option("--args", "-a", help="全局参数指定", multiple=True, type=str)
def cli(env_setting, local_setting, global_setting, args):
    global settings, logger
    if env_setting != "":
        os.environ.setdefault("pythonSettings", env_setting)

    settings = LoadSettings()
    if global_setting != "":
        settings.reload_module(global_setting)
        logger = Log.new_instance()

    for setting in local_setting:
        equal = setting.index("=")
        name, value = setting[:equal], setting[equal + 1:]
        settings.update(name, value, "instance")

    arg_dict = dict()
    for arg in args:
        equal = arg.index("=")
        name, value = arg[:equal], arg[equal + 1:]
        arg_dict[name] = value
    settings.update("CMD_ARGS", arg_dict)


@cli.command()
@click.option("--runner", "-r", help="需要运行应用的应用导入路径", type=str, default="")
@click.option("--args", "-a", help="实例参数指定", type=str, multiple=True)
def run(runner, args):
    global logger, settings
    if runner == "":
        logger.error("应用名称为空, 无法实例应用")
        sys.exit(2)

    arg_dict = dict()
    print(args)
    for arg in args:
        equal = arg.index("=")
        name, value = arg[:equal], arg[equal + 1:]
        arg_dict[name] = value
    settings.update("CMD_RUNNER_ARGS", arg_dict)

    runner_object = import_cls(runner)
    if hasattr(runner_object, "from_settings"):
        spider_object_ = runner_object.from_settings(settings)
        spider_object_.start()
    else:
        logger.error(runner + "应用无from_settings方法, 无法实例应用")
        sys.exit(2)


if __name__ == '__main__':
    # @summary:
    # 这里是经过优化的启动入口, 输入实例化路径，运行相应的runner。类似scrapy的 `scrapy crawl <spider_name>`
    # 通常该文件无需更改, 若需要更改请说明理由 @pikadoramon
    # 爬虫应用入口
    print(sys.argv, len(sys.argv))
    if len(sys.argv) > 1 and (sys.argv[1].find(" ") > -1 or sys.argv[1].find("\n") > -1):
        logger.info("你正在使用XXL-JOB环境调用, 配置click上下文手动解析参数")
        ctx = cli.make_context("xxl-job", sys.argv[1].split())
        cli.invoke(ctx)
    else:
        cli()

#
