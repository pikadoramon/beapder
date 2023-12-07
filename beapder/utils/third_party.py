# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     third_party
   Description :
   Author :       pikadoramon
   date：          2023/7/5
-------------------------------------------------
   Change Activity:
                   2023/7/5:
-------------------------------------------------
"""
import json

import requests

from beapder.utils import log
from beapder.utils.email_sender import EmailSender
from beapder.utils.tools import reach_freq_limit

__author__ = 'pikadoramon'


def dingding_warning(
        setting, message, message_prefix=None, rate_limit=None, url=None, user_phone=None
):
    # 为了加载最新的配置
    rate_limit = rate_limit if rate_limit is not None else setting.WARNING_INTERVAL
    url = url or setting.DINGDING_WARNING_URL
    user_phone = user_phone or setting.DINGDING_WARNING_PHONE

    if not all([url, message]):
        return

    if reach_freq_limit(rate_limit, url, user_phone, message_prefix or message):
        log.info("报警时间间隔过短，此次报警忽略。 内容 {}".format(message))
        return

    if isinstance(user_phone, str):
        user_phone = [user_phone] if user_phone else []

    data = {
        "msgtype": "text",
        "text": {"content": message},
        "at": {"atMobiles": user_phone, "isAtAll": setting.DINGDING_WARNING_ALL},
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, headers=headers, data=json.dumps(data).encode("utf8")
        )
        result = response.json()
        response.close()
        if result.get("errcode") == 0:
            return True
        else:
            raise ValueError(result.get("errmsg"))
    except Exception as e:
        log.error("报警发送失败。 报警内容 {}, error: {}".format(message, e))
        return False


def email_warning(
        setting,
        message,
        title,
        message_prefix=None,
        email_sender=None,
        email_password=None,
        email_receiver=None,
        email_smtpserver=None,
        rate_limit=None,
):
    # 为了加载最新的配置
    email_sender = email_sender or setting.EMAIL_SENDER
    email_password = email_password or setting.EMAIL_PASSWORD
    email_receiver = email_receiver or setting.EMAIL_RECEIVER
    email_smtpserver = email_smtpserver or setting.EMAIL_SMTPSERVER
    rate_limit = rate_limit if rate_limit is not None else setting.WARNING_INTERVAL

    if not all([message, email_sender, email_password, email_receiver]):
        return

    if reach_freq_limit(
            rate_limit, email_receiver, email_sender, message_prefix or message
    ):
        log.info("报警时间间隔过短，此次报警忽略。 内容 {}".format(message))
        return

    if isinstance(email_receiver, str):
        email_receiver = [email_receiver]

    with EmailSender(
            username=email_sender, password=email_password, smtpserver=email_smtpserver
    ) as email:
        return email.send(receivers=email_receiver, title=title, content=message)


def linkedsee_warning(setting, message, rate_limit=3600, message_prefix=None, token=None):
    """
    灵犀电话报警
    Args:
        message:
        rate_limit:
        message_prefix:
        token:

    Returns:

    """
    if not token:
        log.info("未设置灵犀token，不支持报警")
        return

    if reach_freq_limit(rate_limit, token, message_prefix or message):
        log.info("报警时间间隔过短，此次报警忽略。 内容 {}".format(message))
        return

    headers = {"servicetoken": token, "Content-Type": "application/json"}

    url = "http://www.linkedsee.com/alarm/zabbix"

    data = {"content": message}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    return response


def wechat_warning(
        setting,
        message,
        message_prefix=None,
        rate_limit=None,
        url=None,
        user_phone=None,
        all_users: bool = None,
):
    """企业微信报警"""

    # 为了加载最新的配置
    rate_limit = rate_limit if rate_limit is not None else setting.WARNING_INTERVAL
    url = url or setting.WECHAT_WARNING_URL
    user_phone = user_phone or setting.WECHAT_WARNING_PHONE
    all_users = all_users if all_users is not None else setting.WECHAT_WARNING_ALL

    if isinstance(user_phone, str):
        user_phone = [user_phone] if user_phone else []

    if all_users is True or not user_phone:
        user_phone = ["@all"]

    if not all([url, message]):
        return

    if reach_freq_limit(rate_limit, url, user_phone, message_prefix or message):
        log.info("报警时间间隔过短，此次报警忽略。 内容 {}".format(message))
        return

    data = {
        "msgtype": "text",
        "text": {"content": message, "mentioned_mobile_list": user_phone},
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, headers=headers, data=json.dumps(data).encode("utf8")
        )
        result = response.json()
        response.close()
        if result.get("errcode") == 0:
            return True
        else:
            raise ValueError(result.get("errmsg"))
    except Exception as e:
        log.error("报警发送失败。 报警内容 {}, error: {}".format(message, e))
        return False


def feishu_warning(setting, message, message_prefix=None, rate_limit=None, url=None, user=None):
    """

    Args:
        message:
        message_prefix:
        rate_limit:
        url:
        user: {"open_id":"ou_xxxxx", "name":"xxxx"} 或 [{"open_id":"ou_xxxxx", "name":"xxxx"}]

    Returns:

    """
    # 为了加载最新的配置
    rate_limit = rate_limit if rate_limit is not None else setting.WARNING_INTERVAL
    url = url or setting.FEISHU_WARNING_URL
    user = user or setting.FEISHU_WARNING_USER

    if not all([url, message]):
        return

    if reach_freq_limit(rate_limit, url, user, message_prefix or message):
        log.info("报警时间间隔过短，此次报警忽略。 内容 {}".format(message))
        return

    if isinstance(user, dict):
        user = [user] if user else []

    at = ""
    if setting.FEISHU_WARNING_ALL:
        at = '<at user_id="all">所有人</at>'
    elif user:
        at = " ".join(
            [f'<at user_id="{u.get("open_id")}">{u.get("name")}</at>' for u in user]
        )

    data = {"msg_type": "text", "content": {"text": at + message}}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, headers=headers, data=json.dumps(data).encode("utf8")
        )
        result = response.json()
        response.close()
        if result.get("StatusCode") == 0:
            return True
        else:
            raise ValueError(result.get("msg"))
    except Exception as e:
        log.error("报警发送失败。 报警内容 {}, error: {}".format(message, e))
        return False


def send_msg(setting, msg, level="DEBUG", message_prefix=""):
    if setting.WARNING_LEVEL == "ERROR":
        if level.upper() != "ERROR":
            return

    if setting.DINGDING_WARNING_URL:
        keyword = "beapder报警系统\n"
        dingding_warning(setting, keyword + msg, message_prefix=message_prefix)

    if setting.EMAIL_RECEIVER:
        title = message_prefix or msg
        if len(title) > 50:
            title = title[:50] + "..."
        email_warning(setting, msg, message_prefix=message_prefix, title=title)

    if setting.WECHAT_WARNING_URL:
        keyword = "beapder报警系统\n"
        wechat_warning(setting, keyword + msg, message_prefix=message_prefix)

    if setting.FEISHU_WARNING_URL:
        keyword = "beapder报警系统\n"
        feishu_warning(setting, keyword + msg, message_prefix=message_prefix)
