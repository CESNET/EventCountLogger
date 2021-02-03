# Event Count Logger

Python library (and a daemon program) allowing to count the number of various events per time interval(s) in a distributed system using shared counters in Redis.



Simple example:
```python
ecl = EventCountLogger(cfg_groups)
event_logger = ecl.get_group('event_group_1')
...
event_logger.log('event1')  # increment shared counter of 'event1' by 1
```
```shell
# Read counts in the last 5-min interval
$ ecl_reader ecl_config.yml -g event_group_1 -i 5m
event1:10
event2:2
```

**Main features**
* Count any 'events' across multiple processes or machines
* Multiple counters, grouped into *event groups*
  * Pre-defined or dynamic list of event IDs in each group
* Multiple time intervals (count number of events in 5 minute and 1 hour intervals at the same time, for example)
* Local cache for high-rate events, synchronized with the shared counters at a lower rate


## Architecture

```
   System
 components                      Redis                    ecl_reader
+-----------+                +------------+           +---------------+
|   log()   | -------------> |   shared   | --------> |  get_counts() |
+-----------+      +-------> |  counters  |           +---------------+
                   |         +------------+
+-----------+      |               ^               ecl_master
|   log()   | -----+               |           +----------------+
+-----------+                      +---------> |  periodically  |
                                               | reset counters |
                                               +----------------+
```
Shared counters are stored in [Redis](https/redis.io) (in-memory key-value store).
They can be safely incremented by calls to `EventGroup.log(event_id)` from multiple processes.

A single instance of `ecl_master` script must be continuously running - it resets the counters in defined time intervals (and makes values of the last complete interval available for reading).

Counters can be read by `ecl_reader` script, or by using `EventCountLogger.get_*()` methods from any other program.


## How it works

Multiple types of event (identified by any string, *event ID*) can be logged. They are split into *groups*, each group is configured and handled separately.

For each group, a list of event ID is defined in a configuration file (or they can be defined dynamically, see later),
as well as the list of time intervals, in which the counters should be 'rotated'.

In Redis, there are two keys with integer value for each event ID and interval length - one with the number of
logged events in the current (incomplete) interval, one with the final value of the last finished interval.
The keys are named as `<group>:<interval>:<cur_or_last>:<event_id>`, e.g. `requests:5m:cur:req1`.

Logging an event means to increment the `cur` counters with given `<group>` and `<event_id>` and all `<intervals>`.

At the end of an interval, the `ecl_master` script renames all `cur` keys (with matching `<interval>`) to corresponding `last` ones and resets the `cur` ones to zero.

The expected usage is that `ecl_reader` is called periodically to fetch values of the `last` counters and
passes them to some monitoring system (e.g. Munin) or shows them in real-time in some GUI, for example.

There is also a `<group>:<interval>:cur:@ts` key for each group and interval containing the start time of the current interval (as UNIX timestamp).

### Local counters

Normally, when an event is logged, the counter in Redis in incremented immediately.
While Redis is quite fast, it is still a network communication which may be a problem with  too frequent events.
So, there is a possibility cache counts locally in a specific group.
In such case, only a local counter (i.e. a normal Python variable) is incremented when event is logged.
These local counters are synchronized with Redis only after some time elapses (`sync_interval` config param)
or when one of the counters in the group counter reaches a defined value (`sync_limit`).

## Usage example

Log event in a program:
```python
from event_count_logger import EventCountLogger
cfg_groups = {
  'requests': {
    'events': ['req1', 'req2', 'req3', 'error'],
    'intervals': ['5s', '5m'],
  }
}
ecl = EventCountLogger(cfg_groups)
req_logger = ecl.get_group('requests')

...

def handle_request(..., req_type, ...):
  try:
    if req_type == 'req1':
      req_logger.log('req1') # increment counter of 'req1' event by 1
      process_req1()
    ...
  except Exception as e:
    req_logger.log('error') # increment 'error' counter by 1
    handle_error(e)
```

Read the number of events in the last finished 5-minute interval:
```shell
$ ecl_reader ecl_config.yml -g requests -i 5m
req1:54
req2:11
req3:42
error:3
```

## Configuration

A YAML file is used to define event groups and specify Redis connection parameters.
See `example_config.yml` for an example with comments.

Event group specification keys:
* `events` (mandatory) - List of predefined event IDs (strings). Only those events can be logged in the group, unless `auto_declare_events=true`.
* `intervals` (mandatory) - List of interval lengths in which to 'rotate' the counters. Each interval length is a string in format `<number><s/m/h>` (seconds, minutes, hours), e.g. `5m` or `1h`.
* `auto_declare_events` - Enable automatic declaration of events not listed in `events`, i.e. a new counter is automatically created when a new event ID is logged. Such events are ephemeral, the counters are removed at the end of each interval. (`true/false`, default: `false`) 
* `sync_interval` - Synchronize local counters with Redis after this number of seconds (int or float).
* `sync_limit` - Synchronize local counters with Redis when a counter reach this number (int).

If none of `sync_interval` and `sync_limit` is specified, local counters are not used (i.e. shared counters in Redis are manipulated directly).

## Munin

Example [Munin](http://munin-monitoring.org/) plugins are included for easy logging and visualization
(they must be modified for a specific case, a separate plugin should be used for each event group).


---

# Notes

Written mainly for [NERD](https://github.com/CESNET/NERD) (and another internal project), but can be used in any Python system distributed to multiple processes.

If you want to use it and need more info, drop me a message (`bartos <at> cesnet.cz`).
If there are more users, I'll write a more complete documentation.
