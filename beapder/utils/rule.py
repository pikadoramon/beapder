# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     rule
   Description :
   Author :       hyrogen
   date：          2023/4/4
-------------------------------------------------
   Change Activity:
                   2023/4/4:
-------------------------------------------------
"""
from beapder.utils.generic import DEFAULT_FUNCTION

__author__ = 'hyrogen'

import json
import posixpath
import re
from urllib.parse import urlparse, urljoin

import jsonpath
from lxml.html import fromstring
from weakref import WeakKeyDictionary
from w3lib.url import canonicalize_url
import logging

logger = logging.getLogger(__name__)

_re_extract_extension = re.compile("\.(\w{2,3})$|\.(\w{2,3})\?")
_re_type = type(re.compile("", 0))
_re_url = re.compile("^(https?|ftp)://[^\s/$.?#<;:].[^\s#<;:]*$")

_ITERABLE_SINGLE_VALUES = dict, str, bytes
resp_doc_weakdict_ref = WeakKeyDictionary()
resp_json_weakdict_ref = WeakKeyDictionary()


def arg_to_iter(arg):
    """
    将数据转为可迭代对象, 例如args为dict, 或者 str, bytes 转为[arg]转换 return [arg]
    如果是list或者迭代器, 则直接返回arg
    """
    if arg is None:
        return []
    elif not isinstance(arg, _ITERABLE_SINGLE_VALUES) and hasattr(arg, '__iter__'):
        return arg
    else:
        return [arg]


# 忽视的扩展名称, 在做html全站提取时。下载资源链接可以忽略
IGNORED_EXTENSIONS = [
    # archives
    '.7z', '.7zip', '.bz2', '.rar', '.tar', '.tar.gz', '.xz', '.zip',

    # images
    '.mng', '.pct', '.bmp', '.gif', '.jpg', '.jpeg', '.png', '.pst', '.psp', '.tif',
    '.tiff', '.ai', '.drw', '.dxf', '.eps', '.ps', '.svg', '.cdr', '.ico',

    # audio
    '.mp3', '.wma', '.ogg', '.wav', '.ra', '.aac', '.mid', '.au', '.aiff',

    # video
    '.3gp', '.asf', '.asx', '.avi', '.mov', '.mp4', '.mpg', '.qt', '.rm', '.swf', '.wmv',
    '.m4a', '.m4v', '.flv', '.webm',

    # office suites
    '.xls', '.xlsx', '.ppt', '.pptx', '.pps', '.doc', '.docx', '.odt', '.ods', '.odg',
    '.odp',

    # other
    '.css', '.pdf', '.exe', '.bin', '.rss', '.dmg', '.iso', '.apk',

    '.c', '.js', '.cpp', '.py', '.java', '.cc'
]


def _matches(url, regexs):
    return any(r.search(url) for r in regexs)


def _is_valid_url(url):
    return url.split('://', 1)[0] in {'http', 'https', 'file', 'ftp'}


def url_is_from_any_domain(url, domains):
    """判断域名是否合规"""
    host = urlparse(url).netloc.lower()
    if not host:
        return False
    domains = [d.lower() for d in domains]
    return any((host == d) or (host.endswith(f'.{d}')) for d in domains)


def url_has_any_extension(url, extensions):
    return posixpath.splitext(urlparse(url).path)[1].lower() in extensions


def unique(list_, key=lambda x: x):
    seen = set()
    result = []
    for item in list_:
        seenkey = key(item)
        if seenkey in seen:
            continue
        seen.add(seenkey)
        result.append(item)
    return result


def _extract_links(extractor, response, values):
    links = []
    if extractor == 'text':
        for value in values:
            if isinstance(value, str):
                link = re.findall(value, response.text)
            else:
                link = value.findall(response.text)
            link = arg_to_iter(link)
            if len(link) > 0:
                links.extend(link)
    elif extractor == 'lxml':
        if response in resp_doc_weakdict_ref:
            doc = resp_doc_weakdict_ref[response]
        else:
            if response.text[:20].find("?xml") > -1:
                logger.warning("not support xml " + response.url)
                return links
            doc = fromstring(response.text)
            resp_doc_weakdict_ref[response] = doc
        for value in values:
            link = doc.xpath(value)
            link = arg_to_iter(link)
            if len(link) > 0:
                links.extend(link)
    elif extractor == 'json':
        if response in resp_json_weakdict_ref:
            doc = resp_json_weakdict_ref[response]
        else:
            doc = json.loads(response.text)
            resp_json_weakdict_ref[response] = doc
        for value in values:
            link = jsonpath.jsonpath(doc, value)
            link = arg_to_iter(link)
            if len(link) > 0:
                links.extend(link)
    return links


class LxmlLinkExtractor:

    def __init__(self,
                 allow=(),
                 deny=(),
                 allow_domains=(),
                 deny_domains=(),
                 deny_text=(),
                 extract_xpaths=(),
                 extract_jpaths=(),
                 extract_texts=None,
                 unique=True,
                 deny_extensions=None,
                 serializer=None,
                 canonicalize=True
                 ):
        """
        :param allow:
        :param deny:
        :param allow_domains:
        :param deny_domains:
        :param deny_text:
        :param extract_xpaths:
        :param extract_jpaths:
        :param extract_texts:
        :param unique:
        :param deny_extensions:
        :param serializer:
        """

        self.allow_res = [
            x if isinstance(x, _re_type) else re.compile(x) for x in arg_to_iter(allow)
        ]
        self.deny_res = [
            x if isinstance(x, _re_type) else re.compile(x) for x in arg_to_iter(deny)
        ]

        self.allow_domains = set(arg_to_iter(allow_domains))
        self.deny_domains = set(arg_to_iter(deny_domains))
        self.deny_text = [
            x if isinstance(x, _re_type) else re.compile(x) for x in arg_to_iter(deny_text)
        ]

        self.unique = bool(unique)
        # self.normalization = bool(normalization)
        if deny_extensions is None:
            deny_extensions = IGNORED_EXTENSIONS
        self.deny_extensions = set(arg_to_iter(deny_extensions))

        self.extract_xpaths = tuple(arg_to_iter(extract_xpaths))
        self.extract_jpaths = tuple(arg_to_iter(extract_jpaths))
        self.extract_texts = [x if isinstance(x, _re_type) else re.compile(x)
                              for x in arg_to_iter(extract_texts)]

        self.serializer = serializer
        self.canonicalize = canonicalize

    def _link_allow(self, url):
        if url is None:
            return False

        if not _is_valid_url(url):
            return False
        if self.allow_res and not _matches(url, self.allow_res):
            return False
        if self.deny_res and _matches(url, self.deny_res):
            return False

        if self.allow_domains and not url_is_from_any_domain(url, self.allow_domains):
            return False
        if self.deny_domains and url_is_from_any_domain(url, self.deny_domains):
            return False
        if self.deny_extensions and url_has_any_extension(url, self.deny_extensions):
            return False
        return True

    def _extract_links(self, response):
        links = []

        if self.extract_xpaths:
            links.extend(_extract_links('lxml', response, self.extract_xpaths))
        if self.extract_jpaths:
            links.extend(_extract_links('json', response, self.extract_jpaths))
        if self.extract_texts:
            links.extend(_extract_links('text', response, self.extract_texts))
        t = []
        for link in links:
            if link:
                link = self.serializer(link) if self.serializer and callable(self.serializer) else link
            if link is None or not (link.startswith("/") or link.startswith("http")):
                continue
            full_link = urljoin(response.url, link).strip("/")
            if full_link and self._link_allow(full_link) and _re_url.match(full_link):
                if self.canonicalize:
                    t.append(canonicalize_url(full_link))
                else:
                    t.append(full_link)
        return self._duplicate_if_need(t)

    def _duplicate_if_need(self, links):
        if self.unique:
            return unique(links)
        return links

    def extract_links(self, response):

        links = self._extract_links(response)

        return links

    def __repr__(self):
        return "[beapder.utils.rule.LxmlLinkExtractor at {}]".format(hex(id(self)))


DefaultLinkExtractor = LxmlLinkExtractor


class ItemSelector:

    def __init__(self,
                 name,
                 allow=(),
                 deny=(),
                 allow_domains=(),
                 deny_domains=(),
                 extract_xpaths=(),
                 extract_jpaths=(),
                 extract_texts=None,
                 serializer=None
                 ):
        """

        :param name:
        :param allow:
        :param deny:
        :param allow_domains:
        :param deny_domains:
        :param extract_xpaths:
        :param extract_jpaths:
        :param extract_texts:
        :param deny_extensions:
        :param serializer:
        """
        self.name = name
        self.allow_res = [
            x if isinstance(x, _re_type) else re.compile(x) for x in arg_to_iter(allow)
        ]
        self.deny_res = [
            x if isinstance(x, _re_type) else re.compile(x) for x in arg_to_iter(deny)
        ]

        self.allow_domains = set(arg_to_iter(allow_domains))
        self.deny_domains = set(arg_to_iter(deny_domains))

        self.extract_xpaths = tuple(arg_to_iter(extract_xpaths))
        self.extract_jpaths = tuple(arg_to_iter(extract_jpaths))
        self.extract_texts = [x if isinstance(x, _re_type) else re.compile(x)
                              for x in arg_to_iter(extract_texts)]

        self.serializer = serializer

    def _link_allow(self, url):
        if url is None:
            return False
        if not _is_valid_url(url):
            return False
        if self.allow_res and not _matches(url, self.allow_res):
            return False
        if self.deny_res and _matches(url, self.deny_res):
            return False

        if self.allow_domains and not url_is_from_any_domain(url, self.allow_domains):
            return False
        if self.deny_domains and url_is_from_any_domain(url, self.deny_domains):
            return False
        return True

    def _extract_items(self, response):
        links = []
        if self.extract_xpaths:
            links.extend(_extract_links('lxml', response, self.extract_xpaths))
        if self.extract_jpaths:
            links.extend(_extract_links('json', response, self.extract_jpaths))
        if self.extract_texts:
            links.extend(_extract_links('text', response, self.extract_texts))
        t = []
        for item in links:
            if self.serializer and callable(self.serializer):
                t.append(self.serializer(item))
            else:
                t.append(item)
        return self.name, t

    def extract_items(self, response):
        if not self._link_allow(response.url):
            return self.name, []
        tuple2 = self._extract_items(response)
        return tuple2

    def __repr__(self):
        return "[beapder.utils.rule.ItemSelector at {}]".format(hex(id(self)))


_default_link_extractor = LxmlLinkExtractor(
    extract_xpaths=("//a/@href", "//area/@href")
)

_default_process_links = DEFAULT_FUNCTION


def _identity_process_request(request, response):
    return request


def _get_method(method, spider):
    if callable(method):
        return method
    elif isinstance(method, str):
        return getattr(spider, method, None)


class Rule:

    def __init__(self,
                 extractor,
                 callback=None,
                 cb_kwargs=None,
                 errback=None,
                 follow=True,
                 process_request=None,
                 process_links=None
                 ):
        self.link_extractor = extractor or _default_link_extractor
        self.callback = callback
        self.errback = errback
        self.cb_kwargs = cb_kwargs or {}
        self.process_links = process_links or _default_process_links
        self.process_request = process_request or _identity_process_request
        self.follow = not callback if follow is None else bool(follow)

    def _compile(self, spider):
        self.callback = _get_method(self.callback, spider)
        self.errback = _get_method(self.errback, spider)
        self.process_links = _get_method(self.process_links, spider)
        self.process_request = _get_method(self.process_request, spider)
