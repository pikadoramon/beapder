# -*- coding: utf-8 -*-
import json
import queue
import random
import socket
import string
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter
from typing import Any

from influxdb import InfluxDBClient

from beapder.core.timer_schedule import add_schedule_obj
from beapder.utils import tools
from beapder.utils.log import log
from beapder.utils.tools import ensure_float, ensure_int, urlencode, get_current_timestamp


class MetricsEmitterABC(ABC):
    @abstractmethod
    def emit_any(self, measurement, tags, fields, timestamp):
        pass

    @abstractmethod
    def emit_counter(self, measurement, key, count, tags, timestamp):
        pass

    @abstractmethod
    def emit_store(self, measurement, key, value, tags, timestamp):
        pass

    @abstractmethod
    def emit_timer(self, measurement, key, duration, tags, timestamp):
        pass

    @classmethod
    @abstractmethod
    def init(cls, *args, **kwargs):
        pass

    @classmethod
    @abstractmethod
    def from_settings(cls, settings):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def flush(self):
        pass


class InfluxDBMetricsEmitter(MetricsEmitterABC):
    _instance = None

    def __init__(
            self,
            influxdb,
            *,
            batch_size=10,
            max_timer_seq=0,
            emit_interval=10,
            retention_policy=None,
            ratio=1.0,
            debug=False,
            add_hostname=False,
            max_points=10240,
            default_tags=None,
    ):
        """
        Args:
            influxdb: influxdb instance
            batch_size: 打点的批次大小
            max_timer_seq: 每个时间间隔内最多收集多少个 timer 类型点, 0 表示不限制
            emit_interval: 最多等待多长时间必须打点
            retention_policy: 对应的 retention policy
            ratio: store 和 timer 类型采样率，比如 0.1 表示只有 10% 的点会留下
            debug: 是否打印调试日志
            add_hostname: 是否添加 hostname 作为 tag
            max_points: 本地 buffer 最多累计多少个点
        """
        self.pending_points = queue.Queue()
        self.batch_size = batch_size
        self.influxdb: InfluxDBClient = influxdb
        self.tagkv = {}
        self.max_timer_seq = max_timer_seq
        self.lock = threading.Lock()
        self.hostname = socket.gethostname()
        self.last_emit_ts = time.time()  # 上次提交时间
        self.emit_interval = emit_interval  # 提交间隔
        self.max_points = max_points
        self.retention_policy = retention_policy  # 支持自定义保留策略
        self.debug = debug
        self.add_hostname = add_hostname
        self.ratio = ratio
        self.default_tags = default_tags or {}

    def define_tagkv(self, tagk, tagvs):
        self.tagkv[tagk] = set(tagvs)

    def _point_tagset(self, p):
        return f"{p['measurement']}-{sorted(p['tags'].items())}-{p['time']}"

    def _make_time_to_ns(self, _time):
        """
        将时间转换为 ns 级别的时间戳，补足长度 19 位
        Args:
            _time:

        Returns:

        """
        time_len = len(str(_time))
        random_str = "".join(random.sample(string.digits, 19 - time_len))
        return int(str(_time) + random_str)

    def _accumulate_points(self, points):
        """
        对于处于同一个 key 的点做聚合

          - 对于 counter 类型，同一个 key 的值(_count)可以累加
          - 对于 store 类型，不做任何操作，influxdb 会自行覆盖
          - 对于 timer 类型，通过添加一个 _seq 值来区分每个不同的点
        """
        counters = {}  # 临时保留 counter 类型的值
        timer_seqs = Counter()  # 记录不同 key 的 timer 序列号
        new_points = []

        for point in points:
            point_type = point["tags"].get("_type", None)
            tagset = self._point_tagset(point)

            # counter 类型全部聚合，不做丢弃
            if point_type == "counter":
                if tagset not in counters:
                    counters[tagset] = point
                else:
                    counters[tagset]["fields"]["_count"] += point["fields"]["_count"]
            elif point_type == "timer":
                if self.max_timer_seq and timer_seqs[tagset] > self.max_timer_seq:
                    continue
                # 掷一把骰子，如果足够幸运才打点
                if self.ratio < 1.0 and random.random() > self.ratio:
                    continue
                # 增加 _seq tag，以便区分不同的点
                point["tags"]["_seq"] = timer_seqs[tagset]
                point["time"] = self._make_time_to_ns(point["time"])
                timer_seqs[tagset] += 1
                new_points.append(point)
            else:
                if self.ratio < 1.0 and random.random() > self.ratio:
                    continue
                point["time"] = self._make_time_to_ns(point["time"])
                new_points.append(point)

        for point in counters.values():
            # 修改下counter类型的点的时间戳，补足19位, 伪装成纳秒级时间戳，防止influxdb对同一秒内的数据进行覆盖
            point["time"] = self._make_time_to_ns(point["time"])
            new_points.append(point)

            # 把拟合后的 counter 值添加进来
            new_points.append(point)
        return new_points

    def _get_ready_emit(self, force=False):
        """
        把当前 pending 的值做聚合并返回
        """
        if self.debug:
            log.info("got %s raw points", self.pending_points.qsize())

        # 从 pending 中读取点, 设定一个最大值，避免一直打点，一直获取
        points = []
        while len(points) < self.max_points or force:
            try:
                points.append(self.pending_points.get_nowait())
            except queue.Empty:
                break

        # 聚合点
        points = self._accumulate_points(points)

        if self.debug:
            log.info("got %s point", len(points))
            log.info(json.dumps(points, indent=4))

        return points

    def emit(self, point=None, force=False):
        """
        1. 添加新点到 pending
        2. 如果符合条件，尝试聚合并打点
        3. 更新打点时间

        :param point:
        :param force: 强制提交所有点 默认False
        :return:
        """
        if point:
            self.pending_points.put(point)

        # 判断是否需要提交点 1、数量 2、间隔 3、强力打点
        if not (
                force
                or self.pending_points.qsize() >= self.max_points  # noqa: W503
                or time.time() - self.last_emit_ts > self.emit_interval  # noqa: W503
        ):
            return

        # 需要打点，读取可以打点的值, 确保只有一个线程在做点的压缩
        with self.lock:
            points = self._get_ready_emit(force=force)

            if not points:
                return
            try:
                # h(hour) m(minutes), s(seconds), ms(milliseconds), u(microseconds), n(nanoseconds)
                self.influxdb.write_points(
                    points,
                    batch_size=self.batch_size,
                    time_precision="n",
                    retention_policy=self.retention_policy,
                )
            except Exception:
                log.exception("error writing points")

            self.last_emit_ts = time.time()

    def flush(self):
        if self.debug:
            log.info("start draining points %s", self.pending_points.qsize())
        self.emit(force=True)

    def close(self):
        self.flush()
        try:
            self.influxdb.close()
        except Exception as e:
            log.exception(e)

    def make_point(self, measurement, tags: dict, fields: dict, timestamp=None):
        """
        默认的时间戳是"秒"级别的
        """
        assert measurement, "measurement can't be null"
        tags = tags.copy() if tags else {}
        tags.update(self.default_tags)
        fields = fields.copy() if fields else {}
        if timestamp is None:
            timestamp = int(time.time())
        # 支持自定义hostname
        if self.add_hostname and "hostname" not in tags:
            tags["hostname"] = self.hostname
        point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
        if self.tagkv:
            for tagk, tagv in tags.items():
                if tagv not in self.tagkv[tagk]:
                    raise ValueError("tag value = %s not in %s", tagv, self.tagkv[tagk])
        return point

    def get_counter_point(
            self,
            measurement: str,
            key: str = None,
            count: int = 1,
            tags: dict = None,
            timestamp: int = None,
    ):
        """
        counter 不能被覆盖
        """
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "counter"
        count = ensure_int(count)
        fields = dict(_count=count)
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_store_point(
            self,
            measurement: str,
            key: str = None,
            value: Any = 0,
            tags: dict = None,
            timestamp=None,
    ):
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "store"
        fields = dict(_value=value)
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_timer_point(
            self,
            measurement: str,
            key: str = None,
            duration: float = 0,
            tags: dict = None,
            timestamp=None,
    ):
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "timer"
        fields = dict(_duration=ensure_float(duration))
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def emit_any(self, measurement, tags, fields, timestamp):
        point = self.make_point(measurement=measurement,
                                tags=tags,
                                fields=fields,
                                timestamp=timestamp)
        self.emit(point)

    def emit_counter(self, measurement, key, count, tags, timestamp):
        point = self.get_counter_point(measurement=measurement,
                                       key=key,
                                       count=count,
                                       tags=tags,
                                       timestamp=timestamp)
        self.emit(point)

    def emit_store(self, measurement, key, value, tags, timestamp):
        point = self.get_store_point(measurement=measurement,
                                     key=key,
                                     value=value,
                                     tags=tags,
                                     timestamp=timestamp)
        self.emit(point)

    def emit_timer(self, measurement, key, duration, tags, timestamp):
        point = self.get_timer_point(measurement=measurement,
                                     key=key,
                                     duration=duration,
                                     tags=tags,
                                     timestamp=timestamp)
        self.emit(point)

    @classmethod
    def init(cls, settings,
             influxdb_host=None,
             influxdb_port=None,
             influxdb_udp_port=None,
             influxdb_database=None,
             influxdb_user=None,
             influxdb_password=None,
             influxdb_measurement=None,
             retention_policy=None,
             retention_policy_duration="180d",
             emit_interval=60,
             batch_size=100,
             debug=False,
             use_udp=False,
             timeout=22,
             ssl=False,
             retention_policy_replication: str = "1",
             set_retention_policy_default=True,
             **kwargs):
        """
        打点监控初始化
        Args:
            :param settings: 全局的settings对象
            :param influxdb_host:
            :param influxdb_port:
            :param influxdb_udp_port:
            :param influxdb_database:
            :param influxdb_user:
            :param influxdb_password:
            :param influxdb_measurement: 存储的表，也可以在打点的时候指定
            :param retention_policy: 保留策略
            :param retention_policy_duration: 保留策略过期时间
            :param emit_interval: 打点最大间隔
            :param batch_size: 打点的批次大小
            :param debug: 是否开启调试
            :param use_udp: 是否使用udp协议打点
            :param timeout: 与influxdb建立连接时的超时时间
            :param ssl: 是否使用https协议
            :param retention_policy_replication: 保留策略的副本数, 确保数据的可靠性和高可用性。如果一个节点发生故障，其他节点可以继续提供服务，从而避免数据丢失和服务不可用的情况
            :param set_retention_policy_default: 是否设置为默认的保留策略，当retention_policy初次创建时有效
            :param **kwargs: 可传递MetricsEmitter类的参数

        Returns:
            MetricsEmitterABC

        """

        if cls._instance:
            return cls._instance

        influxdb_host = influxdb_host or settings.get("INFLUXDB_HOST")
        influxdb_port = influxdb_port or settings.getint("INFLUXDB_PORT")
        influxdb_udp_port = influxdb_udp_port or settings.getint("INFLUXDB_UDP_PORT")
        influxdb_database = influxdb_database or settings.get("INFLUXDB_DATABASE")
        influxdb_user = influxdb_user or settings.get("INFLUXDB_USER")
        influxdb_password = influxdb_password or settings.get("INFLUXDB_PASSWORD")
        _measurement = influxdb_measurement or settings.get("INFLUXDB_MEASUREMENT")
        retention_policy = (
                retention_policy or f"{influxdb_database}_{retention_policy_duration}"
        )

        if not all(
                [
                    influxdb_host,
                    influxdb_port,
                    influxdb_udp_port,
                    influxdb_database,
                    influxdb_user,
                    influxdb_password,
                ]
        ):
            return

        influxdb_client = InfluxDBClient(
            host=influxdb_host,
            port=influxdb_port,
            udp_port=influxdb_udp_port,
            database=influxdb_database,
            use_udp=use_udp,
            timeout=timeout,
            username=influxdb_user,
            password=influxdb_password,
            ssl=ssl,
        )
        # 创建数据库
        if influxdb_database:
            try:
                influxdb_client.create_database(influxdb_database)
                influxdb_client.create_retention_policy(
                    retention_policy,
                    retention_policy_duration,
                    replication=retention_policy_replication,
                    default=set_retention_policy_default,
                )
            except Exception as e:
                log.error("metrics init falied: {}".format(e))
                return

        cls._instance = InfluxDBMetricsEmitter(
            influxdb_client,
            debug=debug,
            batch_size=batch_size,
            retention_policy=retention_policy,
            emit_interval=emit_interval,
            **kwargs,
        )
        return cls._instance

    @classmethod
    def from_settings(cls, settings):
        if cls._instance:
            return cls._instance

        influxdb_host = settings.get("INFLUXDB_HOST")
        influxdb_port = settings.getint("INFLUXDB_PORT")
        influxdb_udp_port = settings.getint("INFLUXDB_UDP_PORT")
        influxdb_database = settings.get("INFLUXDB_DATABASE")
        influxdb_user = settings.get("INFLUXDB_USER")
        influxdb_password = settings.get("INFLUXDB_PASSWORD")
        _measurement = settings.get("INFLUXDB_MEASUREMENT")
        infuxdb_other_args = settings.getdict("INFLUXDB_OTHER_ARGS")
        retention_policy_duration = infuxdb_other_args.get("retention_policy_duration")
        retention_policy = (
                infuxdb_other_args.get("retention_policy") or
                "{influxdb_database}_{retention_policy_duration}".format(influxdb_database=influxdb_database,
                                                                         retention_policy_duration=retention_policy_duration)
        )

        if not all(
                [
                    influxdb_host,
                    influxdb_port,
                    influxdb_udp_port,
                    influxdb_database,
                    influxdb_user,
                    influxdb_password,
                ]
        ):
            return

        influxdb_client = InfluxDBClient(
            host=influxdb_host,
            port=influxdb_port,
            udp_port=influxdb_udp_port,
            database=influxdb_database,
            use_udp=infuxdb_other_args["use_udp"],
            timeout=infuxdb_other_args["timeout"],
            username=influxdb_user,
            password=influxdb_password,
            ssl=infuxdb_other_args["ssl"],
        )
        # 创建数据库
        if influxdb_database:
            try:
                influxdb_client.create_database(influxdb_database)
                influxdb_client.create_retention_policy(
                    retention_policy,
                    retention_policy_duration,
                    replication=infuxdb_other_args["retention_policy_replication"],
                    default=infuxdb_other_args["set_retention_policy_default"],
                )
            except Exception as e:
                log.error("metrics init falied: {}".format(e))
                return

        cls._instance = InfluxDBMetricsEmitter(
            influxdb_client,
            retention_policy=retention_policy,
            **infuxdb_other_args,
        )
        return cls._instance


class LogMetricsEmitter(MetricsEmitterABC):
    _instance = None

    def __init__(self, interval):
        self.interval = interval
        self.multiplier = 60.0 / self.interval
        self.lock = threading.Lock()
        self.stat_ = dict()  # 这里使用字典记录统计数据.
        self.pagesprev = 0
        self.succpagesprev = 0
        self.itemsprev = 0

        self.stat_["/spider/start"] = get_current_timestamp()
        self.stat_["/spider/close"] = None

    def incr(self, key, value):
        if key in self.stat_:
            self.stat_[key] += value
        else:
            self.stat_[key] = value

    def set(self, key, value):
        self.stat_[key] = value

    def log(self):
        msg = (
            "Crawl Page in Total %(succ_pages)d/%(pages)d pages (at %(pagerate)d total_pages/min and %(sprate)d "
            "succ_pages/min), "
            "scraped %(items)d items (at %(itemrate)d items/min)")
        pages = self.stat_.get("/spider/download_total", 0)
        succ_pages = self.stat_.get("/spider/download_success", 0)
        items = self.stat_.get("/spider/scraped_item", 0)
        prate = (pages - self.pagesprev) / self.multiplier
        irate = (items - self.itemsprev) / self.multiplier
        sprate = (succ_pages - self.succpagesprev) / self.multiplier
        self.succpagesprev, self.pagesprev, self.itemsprev = succ_pages, pages, items
        log_args = {'pages': pages, 'pagerate': prate,
                    'succ_pages': succ_pages, 'sprate': sprate,
                    'items': items, 'itemrate': irate, }
        log.info(
            msg % log_args
        )

    def __del__(self):
        self.close()

    def emit_any(self, measurement, tags, fields, timestamp):
        pass

    def emit_counter(self, measurement, key, count, tags, timestamp):

        _regular_key = "/{measurement}/{key}[{tags}]"
        colon = key.find(":")
        if key and colon > -1:
            self.incr(
                "/spider/" + key[colon + 1:],
                count
            )
        elif key == "total count":
            self.incr(
                "/spider/scraped_item",
                count
            )
        self.incr(
            _regular_key.format(measurement=measurement or "basic",
                                key=key,
                                tags=urlencode(tags) if isinstance(tags, dict) else "tag=default"),
            count
        )

    def emit_store(self, measurement, key, value, tags, timestamp):

        _regular_key = "/{measurement}/{key}[{tags}]"
        colon = key.find(":")
        if key and colon > -1:
            self.set(
                "/spider/" + key[colon + 1:],
                value
            )
        else:
            self.incr(
                _regular_key.format(measurement=measurement or "basic",
                                    key=key,
                                    tags=urlencode(tags) if isinstance(tags, dict) else "tag=default"),
                value
            )

    def emit_timer(self, measurement, key, duration, tags, timestamp):
        pass

    @classmethod
    def init(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(60)
        return cls._instance

    def close(self):
        self.stat_["/spider/close"] = get_current_timestamp()
        self.stat_["/spider/spend"] = self.stat_["/spider/close"] - self.stat_["/spider/start"]
        self.stat_["/spider/close_time"] = tools.timestamp_to_date(self.stat_["/spider/close"])
        self.stat_["/spider/start_time"] = tools.timestamp_to_date(self.stat_["/spider/start"])
        log.debug(
            "MetricsEmitter [beapder.extensions.metrics_emitter.LogMetricsEmitter] \n" + tools.dumps_json(self.stat_, 4,
                                                                                                          True))

    def flush(self):
        pass

    @classmethod
    def from_settings(cls, settings):
        if cls._instance is None:
            interval = settings.getint("LOG_METRICS_EMITTER_INTERVAL", 60)
            assert isinstance(interval, int) and interval > 1, \
                "LOG_METRICS_EMITTER_INTERVAL should be greater than 1 second, instead of `%s`" % interval
            cls._instance = cls(interval)
            add_schedule_obj(interval, cls._instance, "log")

        return cls._instance
