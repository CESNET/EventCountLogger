"""
Module for counting events per time interval using Redis, so it allows to count events across multiple processes.
"""
# Author: Vaclav Bartos <bartos@cesnet.cz>
# Copyright (c) 2021 CESNET, z.s.p.o.
# SPDX-License-Identifier: BSD-3-Clause

from typing import Union, Iterable, Dict, Set, Tuple
import re
import logging
import threading
import time
from datetime import datetime

# apscheduler loads quite slowly, so we import it later, only when its needed.
# Thanks to this, start-up of simple scripts (ecl_reader, ecl_log_event) is significantly faster.
#from apscheduler.schedulers.background import BackgroundScheduler
import redis

# TODO functions to validate configuration (at least existence and types of mandatory fields of group configs)
#    use in EventCountLogger and ecl_* binaries

DEFAULT_REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0  # Index of Redis DB used for the counters
}

class EventCountLogger:
    """
    Class wrapping functions to log counts of events per time interval in Redis.

    Usual usage is to instantiate one EventCountLogger per process and use its get_group() method to get individual
    groups of counters to use by different program components.
    """

    def __init__(self, cfg_groups: dict, cfg_redis: dict = None, log_level: Union[str, int] = 'WARNING'):
        """
        Initialize the class using given configuration.

        :param cfg_groups: dict containing specification of event groups
        :param cfg_redis: dict containing Redis connection parameters (keys are passed directly to Connection() class
                          from redis-py package, normally 'host', 'port' and 'db' are expected to be configured;
                          all are optional)
        :param log_level: log verbosity level (for 'logging' module)
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(log_level)
        self.log_level = log_level

        self.warning_printed = set() # group names requested but not found in config so warning was issued

        self.log.debug(f"Configured event groups: {cfg_groups}")

        # Load Redis config and create an instance of Redis connection wrapper
        self.redis_config = DEFAULT_REDIS_CONFIG.copy()
        if cfg_redis:
            self.redis_config.update(cfg_redis)
        self.log.info(f"Connecting to Redis using params: {self.redis_config}")
        # Use BlockingConnectionPool - it is thread-safe
        connection_pool = redis.BlockingConnectionPool(max_connections=50, timeout=10, **self.redis_config)
        self.redis = redis.Redis(connection_pool=connection_pool)

        # Test connection to Redis
        try:
            self.redis.ping()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            self.log.error(f"Can't connect to redis: {e}")
            raise

        self._all_groups = cfg_groups.copy()
        self._instantiated_groups = {}

    def dump_config(self) -> str:
        """Return string specifying loaded configuration (for debugging)."""
        return f"EventCountLogger configuration:\nredis: {self.redis_config}\ngroups: {self._all_groups}\n"

    def get_group(self, name:str, dummy:bool=False, warn:bool=True) -> 'Union[EventGroup,DummyEventGroup,None]':
        """
        Create EventGroup instance representing given event group (with parameters from configuration),
        or return reference to the existing one.
        :param name: Name of the group
        :param dummy: Return DummyEventGroup instance (instead of None) if the group is not in configuration
        :param warn: Print a warning log message if the group is not in configuration (only one message for each
                     group is printed during the lifetime of the EventCountLogger instance)
        :return: An instance of EventGroup if the group is configured, None or DummyEventGroup otherwise
        """
        # If there already is an instance representing this group, return reference to it
        group = self._instantiated_groups.get(name, None)
        if group:
            return group

        # Otherwise create a new instance according to loaded configuration and store it
        # If it isn't configured, return DummyEventGroup or None
        if name not in self._all_groups:
            if warn and name not in self.warning_printed:
                self.log.warning(f"No configuration for event group '{name}' found, events of this group will not be logged.")
                self.warning_printed.add(name)
            if dummy:
                return DUMMY_EVENT_GROUP
            return None

        grp_spec = self._all_groups[name]
        auto_declare = bool(grp_spec.get('auto_declare_events'))
        group = EventGroup(self.redis, name, grp_spec.get('events', []), grp_spec['intervals'], auto_declare,
                           grp_spec.get('sync_interval', None), grp_spec.get('sync_limit'), log_level=self.log_level)
        self._instantiated_groups[name] = group
        return group

    def __getitem__(self, item):
        """As get_group, but return DummyEventGroup when no group is configured."""
        return self.get_group(item, dummy=True, warn=True)


class EventGroup:
    """
    Class representing an event group and its configuration
    """

    def __init__(self, redis_conn: redis.Redis, name: str, event_ids: Iterable[str], intervals: Iterable[str],
                 auto_declare: bool = False, sync_interval: Union[int, float, None] = None, sync_limit: int = None,
                 log_level: Union[str, int] = 'WARNING'):
        """
        Class representing an event group and its configuration. There should always be only one instance of this class
        for given group. It should only be created internally by EventCountLogger.

        If sync_interval or sync_limit (or both) is set, local 'cache' counters are used and synchronized with
        the global one in Redis once per sync_interval or when counter reaches sync_limit events (this is faster,
        but counts in Redis are delayed)
        If none of these are set, each event is logged directly to global counter.

        :param redis_conn: Instance of Redis wrapper
        :param name: Name of the event group
        :param event_ids: Identifiers of events in this group
        :param auto_declare: Automatically declare unknown event IDs (when log is called)
        :param intervals: Specification of intervals (number + 's'/'m'/'h') in which to log current number of events.
        :param sync_interval: Number of seconds to synchronize local counters with the global ones (may by fractional).
        :param sync_limit: Maximum number of events in local counter before it's flushed to the global one.
        """
        self.logger = logging.getLogger(f"EventGroup({name})")
        self.logger.setLevel(log_level)

        assert sync_interval is None or sync_interval > 0
        assert sync_limit is None or sync_limit > 0

        # TODO: check "intervals" format (but don't convert it)

        self.redis = redis_conn
        self.group_name = name
        self.event_ids = set(event_ids)
        self.intervals = intervals
        self.auto_declare = auto_declare
        self.sync_interval = float(sync_interval) if sync_interval else None
        self.sync_limit = sync_limit

        self.use_local_counters = (self.sync_interval is not None or self.sync_limit is not None)

        # local counters structure: { "5m": { "eventX": <count>, "eventY": <count> }, "1h": { "eventX": <count> ..} ..}
        if self.use_local_counters:
            self.logger.info("Instance created. This group uses local counters")
            self.counters = {interval: {x: 0 for x in self.event_ids} for interval in self.intervals}
            self.counter_lock = threading.Lock()  # Lock to allow safe manipulations with local counters by multiple threads
        else:
            self.logger.info("Instance created.")
            self.counters = None

        if self.sync_interval is not None:
            self.logger.info("Starting a background scheduler for syncing local counters")
            # Import scheduler here, so start-up is faster when this is not needed
            from apscheduler.schedulers.background import BackgroundScheduler
            self.scheduler = BackgroundScheduler()
            self.scheduler.start()
            self.scheduler.add_job(self.sync, 'interval', seconds=self.sync_interval)

    # Main log method
    def log(self, event_id: str, count: int = 1):
        """
        Increment counter event_id in the group.
        :param event_id: Name of the event
        :param count: Increment counter by this number (default: 1)
        """
        self.logger.debug(f"logging event '{event_id}' (count={count})")
        # Check existence of the event_id
        if event_id not in self.event_ids:
            if self.auto_declare:
                self.declare_event_id(event_id)
            else:
                self.logger.warning(f"Event '{event_id}' is not declared!")
                return

        if self.use_local_counters:
            do_sync = False
            with self.counter_lock:
                for int_counters in self.counters.values():
                    int_counters[event_id] += count
                    if self.sync_limit is not None and int_counters[event_id] >= self.sync_limit:
                        do_sync = True
            if do_sync:
                self.sync()
        else:
            for interval in self.intervals:
                key = create_redis_key(self.group_name, interval, True, event_id)
                self._increment_redis_value(key, count)

    def sync(self):
        """
        Synchronize local counters with those in Redis
        """
        if not self.use_local_counters:
            return
        self.logger.debug("Syncing local counters to Redis")
        with self.counter_lock:
            # TODO: it would be better to sync all counters atomically in a transaction (MULTI/EXEC pair)
            for interval, counters in self.counters.items():
                for event_key, event_cnt in counters.items():
                    if event_cnt != 0:
                        key = create_redis_key(self.group_name, interval, True, event_key)
                        self._increment_redis_value(key, event_cnt)
                        counters[event_key] = 0

    def declare_event_id(self, event_id: str):
        """
        Create counter for event_id if it doesn't exist yet.
        Should be equivalent to listing the event ID in configuration file.
        """
        if event_id not in self.event_ids:
            self.event_ids.add(event_id)
            if self.use_local_counters:
                for int_counters in self.counters:
                    int_counters[event_id] = 0

    def declare_event_ids(self, event_ids: Iterable[str]):
        """
        The same as declare_event_id but for more values.
        """
        for event_id in event_ids:
            self.declare_event_id(event_id)

    def _get_redis_value(self, key: str) -> int:
        val = self.redis.get(key)
        return int(val) if val else 0

    def _increment_redis_value(self, key: str, value: int):
        self.redis.incr(key, amount=value)

    # TODO: is this even needed? maybe only for debugging
    def get_current_counts(self, event_id: str) -> Union[Dict[str, int], None]:
        """
        Return *current* state of counter event_id of *all* intervals.

        Fetch current count stored in Redis and add local one (if local counters are enabled).
        :param event_id: Name of the event
        :return: Dictionary { "interval1": counter_val, "interval2" : ...} or None if event_id is not declared
        """
        if event_id not in self.event_ids:
            self.logger.warning(f"Event '{event_id}' is not declared!")
            return None  # TODO raise exception? or return empty counters?

        # Note: there's a possible race condition. Assume the sync() method has pushed a value of a local counter to
        # Redis, but hasn't reset it, yet. If during this time this method reads the counter from Redis and adds the
        # local value, the local value is counted twice. However, this is almost impossible in practice, and the count
        # is only approximate anyway, since it doesn't include unsynced values from other processes.
        # (We could do everything within a "with self.counter_lock" block, but communication with Redis within a locked
        # section could block other threads too much.)

        ret_dict = {}
        # get counts from redis
        for interval in self.intervals:
            key = create_redis_key(self.group_name, interval, True, event_id)
            ret_dict[interval] = self._get_redis_value(key)

        # add local counts
        if self.use_local_counters:
            for interval, counters in self.counters.items():
                ret_dict[interval] += counters[event_id]

        return ret_dict

    # Methods to read data from Redis
    def get_last_count(self, event_id: str, interval: str) -> Union[int, None]:
        """
        Return *last* counter state in given reset interval of given event_id.

        :param event_id: Name of the event
        :param interval: Interval specification
        :return: int (None if given event_id or interval is not present in Redis)
        """
        key = create_redis_key(self.group_name, interval, False, event_id)
        return self._get_redis_value(key)

    def get_current_count(self, event_id: str, interval: str) -> Union[int, None]:
        """
        Return *current* counter state in given reset interval of given event_id.

        Note this method only read the shared state in Redis, so if local counters are enabled, the counts may be
        incomplete.

        :param event_id: Name of the event
        :param interval: Interval specification
        :return: int (None if given event_id or interval is not present in Redis)
        """
        key = create_redis_key(self.group_name, interval, True, event_id)
        return self._get_redis_value(key)

    def get_counts(self, interval: str, current: bool = False) -> Dict[str, Union[int, None]]:
        """
        Read state of counters in given reset interval from Redis (all event_ids in the group).

        Note this method only read the shared state in Redis, so if "current" interval isto be read and local counters
        are enabled, the counts may be incomplete.

        If auto_declare_events is enabled, the list of currently existing event IDs in Redis is fetched and joined with
        that from configuration before reading the counter values.

        :param interval: Intervals specification
        :param current: Whether to read "current" (True) or "last" (False) counters.
        :return: Dictionary { "event1": counter_val, "event2" : ...} or None if given interval is not defined
        """
        if interval not in self.intervals:
            self.logger.error(f"Interval '{interval}' is not defined for this group!")
            return {}  # TODO raise exception? or return empty counters?

        event_ids = self.event_ids
        if self.auto_declare:
            event_ids = event_ids | get_declared_events(self.redis, self.group_name, interval)[0 if current else 1] # don't use |=, we don't want to change the original object

        ret_dict = {}
        # get counts from redis
        for event_id in event_ids:
            key = create_redis_key(self.group_name, interval, current, event_id)
            ret_dict[event_id] = self._get_redis_value(key)

        return ret_dict


class DummyEventGroup:
    """
    Dummy class with an "empty" log() method, which does nothing.

    Can be used as a placeholder when needed EventGroup is not specified in configuration, to avoid a check before
    each attempt to log something.
    Example:
        ecl = EventCountLogger(...)
        evt_log = ecl.get_group("group1") or DummyEventGroup()
        ...
        evt_log.log("evt1") # works regardless the "group1" is defined or not
    """
    def log(self, event_id: str, count: int = 1):
        return

# A pre-created instance of DummyEventGroup
DUMMY_EVENT_GROUP = DummyEventGroup()


class EventCountLoggerMaster(EventCountLogger):
    """Class to be used as the master/controller process."""
    # TODO: support for callbacks? Functions to call whenever an interval of some group is finished

    def __init__(self, cfg_groups: dict, cfg_redis: dict = None, log_level: Union[str, int] = 'INFO'):
        """
        Class to be used as the master/controller process.

        It periodically renames the "cur" counters to the "last" ones and resets "cur".
        It should only be instantiated once, within the master process.

        :param cfg_groups: dict containing specification of event groups
        :param cfg_redis: dict containing Redis connection parameters (keys are passed directly to Connection() class
                          from redis-py package, normally 'host', 'port' and 'db' are expected to be configured;
                          all are optional)
        :param log_level: log verbosity level (for 'logging' module)
        """
        super().__init__(cfg_groups, cfg_redis, log_level)

        # Import scheduler here, so start-up is faster when this is not needed
        from apscheduler.schedulers.background import BackgroundScheduler
        self.scheduler = BackgroundScheduler()

    def run(self):
        """
        Start the infinite loop periodically managing data in Redis.

        Blocks until it is stopped by KeyboardInterrupt (Ctrl-C) or SystemExit exception.
        """
        self.log.info("ECL Master starting")
        self.scheduler.start()
        now = get_current_time()
        for group_name, group in self._all_groups.items():
            for interval in group["intervals"]:
                seconds = int2sec(interval)
                first_log_time = now + seconds - now % seconds
                self.log.info(f"Processing of group '{group_name}', interval '{interval}', will start in {seconds - now % seconds} s ...")

                self.scheduler.add_job(self._process, "interval", [group_name, interval], seconds=seconds,
                                       start_date=datetime.fromtimestamp(first_log_time))

        try:
            while True:
                time.sleep(500)
        except (KeyboardInterrupt, SystemExit):
            self.scheduler.shutdown()
            self.log.info("ECL Master stopped")
            return

    def _process(self, group_name: str, interval: str):
        """Move counters in the "current" interval to the "last" one."""
        self.log.info(f"Processing group '{group_name}' ({interval})")
        # Get lists of event IDs
        #   base - those pre-declared in configuration
        #   in_redis - those loaded from redis, i.e. even those declared dynamically
        base_events = set(self._all_groups[group_name]["events"])
        if self._all_groups[group_name].get("auto_declare_events", False): # if there can be dynamically declared event IDs
            events_in_redis_cur, events_in_redis_last = get_declared_events(self.redis, group_name, interval)
            # dynamic event IDs present only in the last interval must be deleted explicitly
            events_to_delete = events_in_redis_last - events_in_redis_cur - base_events
        else:
            events_to_delete = set()
            events_in_redis_cur = set()

        # Start transaction
        pipe = self.redis.pipeline()
        # Remove all dynamic keys present only in the last interval
        if events_to_delete:
            pipe.delete(*(create_redis_key(group_name, interval, False, eid) for eid in events_to_delete))
        # Move counters of dynamically defined events
        for event_id in events_in_redis_cur - base_events:
            curr_key = create_redis_key(group_name, interval, True, event_id)
            last_key = create_redis_key(group_name, interval, False, event_id)
            pipe.rename(curr_key, last_key) # rename current to last
        # Move and init counters of pre-defined events
        # (Counter of dynamic ones remain undefined until some event is logged. Thus, if no event is logged during whole
        #  interval, it's not copied into "last" next time and that event ID becomes undeclared.)
        for event_id in base_events:
            curr_key = create_redis_key(group_name, interval, True, event_id)
            last_key = create_redis_key(group_name, interval, False, event_id)
            pipe.setnx(curr_key, 0) # set current to 0 if not set yet
            pipe.rename(curr_key, last_key) # rename current to last
            pipe.set(curr_key, 0) # initialize current to 0
        # Set timestamp of the current interval
        time_key = create_redis_key(group_name, interval, True, "@ts")
        pipe.set(time_key, get_current_time())
        # Execute the transaction
        pipe.execute()


#######################
# Helper functions:

def create_redis_key(group_name: str, interval: str, is_current: bool, event_id: str) -> str:
    """Format a Redis key for counter.

    is_current is bool specifying whether it's for the value of the current or the last interval
    """
    return f"{group_name}:{interval}:{'cur' if is_current else 'last'}:{str(event_id)}"

def get_declared_events(redis_conn: redis.Redis, group_name: str, interval: str) -> Tuple[Set[str],Set[str]]:
    """
    Get list of event IDs in the given group by reading keys in Redis.

    The list can't be read from configuration, as event IDs can also be declared dynamically.

    :param redis_conn: Instance of Redis wrapper
    :param group_name: name of the event group
    :param interval: interval string
    :return: pair of two sets of event IDs (strings), first for 'current' interval, second for the 'last'.
    """
    cur_events = set()
    last_events = set()
    for key in redis_conn.scan_iter(f"{group_name}:{interval}:[^:]*:[^@]*"):
        _,_,cur_or_last,eventid = key.decode(encoding='utf-8').split(":", 3)
        if cur_or_last == 'cur':
            cur_events.add(eventid)
        elif cur_or_last == 'last':
            last_events.add(eventid)
    return cur_events, last_events

_interval_re = re.compile(r'^([-+]?\d*\.\d+|\d+)([hHmMsS])$')

def int2sec(interval_str: str) -> Union[int, float]:
    """
    Parse interval string (number + 's'/'m'/'h').

    Parse given string specifying a time interval and return it as a number of seconds. The format is a number
    followed by 's', 'm' or 'h' meaning seconds, minutes or hours, respectively (seconds by default).

    :param interval_str: string specifying the interval
    :return: Number of seconds (int or float)
    """
    res = _interval_re.match(interval_str)
    if res:
        number = float(res.group(1))
        char = str(res.group(2))
        if char in ["h", "H"]:
            return number * 3600
        if char in ["m", "M"]:
            return number * 60
        else:
            return number
    else:
        raise ValueError(f"Interval string '{interval_str}' does not match pattern '^[-+]?\\d*\\.\\d+|\\d+[hHmMsS]$'")

def get_current_time() -> int:
    """
    Return current Unix timestamp as int.
    """
    return int(time.time())


