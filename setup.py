# -*- coding: utf-8 -*-
"""
Created on 2020/4/22 10:45 PM
---------
@summary:
---------
@author: Boris
@email: boris_liu@foxmail.com
"""

from os.path import dirname, join
from sys import version_info

import setuptools

if version_info < (3, 6, 0):
    raise SystemExit("Sorry! beapder requires python 3.6.0 or later.")

with open(join(dirname(__file__), "beapder/VERSION"), "rb") as fh:
    version = fh.read().decode("ascii").strip()

with open(join(dirname(__file__), "beapder/FEAPDER_VERSION"), "rb") as fh:
    feapder_version = fh.read().decode("ascii").strip()

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

packages = setuptools.find_packages()
packages.extend(
    [
        "beapder",
        "beapder.templates",
        "beapder.templates.project_template",
        "beapder.templates.project_template.spiders",
        "beapder.templates.project_template.items",
    ]
)

requires = [
    "better-exceptions>=0.2.2",
    "DBUtils>=2.0",
    "parsel>=1.5.2",
    "PyMySQL>=0.9.3",
    "redis>=2.10.6,<4.0.0",
    "requests>=2.22.0",
    "bs4>=0.0.1",
    "ipython>=7.14.0",
    "cryptography>=3.3.2",
    "urllib3>=1.25.8",
    "loguru>=0.5.3",
    "influxdb>=5.3.1",
    "pyperclip>=1.8.2",
    "terminal-layout>=2.1.3",
    "retrying>=1.3.3",
    "queuelib>=1.5.0",
    "click>=8.0.4",
]

render_requires = [
    "webdriver-manager>=3.5.3",
    "playwright",
    "selenium>=3.141.0",
]

all_requires = [
                   "bitarray>=1.5.3",
                   "PyExecJS>=1.5.1",
                   "pymongo>=3.10.1",
                   "redis-py-cluster>=2.1.0",
               ] + render_requires

setuptools.setup(
    name="beapder",
    version=version,
    author="pikadoramon",
    license="MIT",
    author_email="beapder@github.com",
    python_requires=">=3.6",
    description="beapder是一款基于feapder[" + feapder_version + "版本]所开发的分布式爬虫框架, 追求稳定, 扩展以及可维护性。兼容scrapy和feapder工具类",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=requires,
    extras_require={"all": all_requires, "render": render_requires},
    entry_points={"console_scripts": ["beapder = beapder.commands.cmdline:execute"]},
    url="https://github.com/pikadoramon/beapder.git",
    packages=packages,
    include_package_data=True,
    classifiers=["Programming Language :: Python :: 3"],
)
