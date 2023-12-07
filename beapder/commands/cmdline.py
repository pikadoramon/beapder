# -*- coding: utf-8 -*-
"""
Created on 2020/5/8 2:24 PM
---------
@summary:
---------
@author: Boris
@email: boris_liu@foxmail.com
"""

import re
import sys
from os.path import dirname, join
import os

import requests

from beapder.commands import create_builder
from beapder.commands import retry
from beapder.commands import shell
from beapder.commands import zip

HELP = """
██████═╗███████╗ █████╗ ██████╗ ██████╗ ███████╗██████╗
██╔══██╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
█████══╗█████╗  ███████║██████╔╝██║  ██║█████╗  ██████╔╝
██╔══██╝██╔══╝  ██╔══██║██╔═══╝ ██║  ██║██╔══╝  ██╔══██╗
██║███╗ ███████╗██║  ██║██║     ██████╔╝███████╗██║  ██║
╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═════╝ ╚══════╝╚═╝  ╚═╝

Version: {version}
Document: 
    内部文档 https://github.com/pikadoramon/beapder/wiki 
    fepader公开文档 https://feapder.com

Feapder author: Boris-code
Beapder author: pikadoramon

Usage:
  beapder <command> [options] [args]
      
Available commands:
"""

NEW_VERSION_TIP = """
──────────────────────────────────────────────────────
New version available \033[31m{version}\033[0m → \033[32m{new_version}\033[0m
Run \033[33mpip install --upgrade beapder\033[0m to update!
"""

with open(join(dirname(dirname(__file__)), "VERSION"), "rb") as f:
    VERSION = f.read().decode("ascii").strip()

with open(join(dirname(dirname(__file__)), "FEAPDER_VERSION"), "rb") as f:
    FEAPDER_VERSION = f.read().decode("ascii").strip()


def _print_commands():
    print(HELP.rstrip().format(version=VERSION + "[" + FEAPDER_VERSION + "]"))
    cmds = {
        "create": "create project、spider、item and so on",
        "shell": "debug response",
        "zip": "zip project",
        "retry": "retry failed request or item",
    }
    for cmdname, cmdclass in sorted(cmds.items()):
        print("  %-13s %s" % (cmdname, cmdclass))

    print('\nUse "beapder <command> -h" to see more info about a command')


def check_new_version():
    def pypi_new_version():
        url = "https://pypi.org/simple/beapder/"
        resp = requests.get(url, timeout=3, verify=False)
        html = resp.text

        latest_version = re.findall(r"beapder-([\d.]*?).tar.gz", html)[-1]
        return latest_version

    def github_new_version():
        url = "https://raw.githubusercontent.com/pikadoramon/beapder/master/beapder/VERSION"
        resp = requests.get(url, timeout=3, verify=False)
        latest_version = resp.text.strip()
        return latest_version

    def compare_diff_version(latest_stable_version):
        now_version = VERSION
        now_stable_version = re.sub("-beta.*", "", VERSION)

        if now_stable_version < latest_stable_version or (
                now_stable_version == latest_stable_version and "beta" in now_version
        ):
            new_version = f"beapder=={latest_stable_version}"
            if new_version:
                version = f"beapder=={VERSION.replace('-beta', 'b')}"
                tip = NEW_VERSION_TIP.format(version=version, new_version=new_version)
                # 修复window下print不能带颜色输出的问题
                if os.name == "nt":
                    os.system("")
                print(tip)

    try:
        latest_stable_version = pypi_new_version()
        compare_diff_version(latest_stable_version)
    except Exception as _:
        pass
    try:
        latest_stable_version = github_new_version()
        compare_diff_version(latest_stable_version)
    except Exception as _:
        pass


def execute():
    try:
        args = sys.argv
        if len(args) < 2:
            _print_commands()
            check_new_version()
            return

        command = args.pop(1)
        if command == "create":
            create_builder.main()
        elif command == "shell":
            shell.main()
        elif command == "zip":
            zip.main()
        elif command == "retry":
            retry.main()
        else:
            _print_commands()
    except KeyboardInterrupt:
        pass

    check_new_version()


if __name__ == "__main__":
    execute()
