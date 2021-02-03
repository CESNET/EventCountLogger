#!/usr/bin/env python3
"""Simple test of EventCountLogger"""

import event_count_logger
from time import sleep

# Note that normally there is a separate daemon script to "rotate" the counters at interval boundaries, when it's not
# running, all the counters are incrementing only.

config = {
    'groups': {
        'group1': {
            'events': ['event1', 'event2'],
            'intervals': ['10s', '1m'],
            'sync_interval': 2,
            'sync_limit': 5
        },
        'group2': {
            'events': ['e2-1', 'e2-2'],
            'intervals': ['30s']
        }
    }
}

ecl = event_count_logger.EventCountLogger(config['groups'], config.get('redis', None))
print(ecl.dump_config())

evt_grp1 = ecl.get_group('group1')
evt_grp2 = ecl.get_group('group2')

sleep(1)
print("logging group1/event1")
evt_grp1.log('event1')
print("logging group1/event2")
evt_grp1.log('event2')
sleep(1)
print("logging group1/event2")
evt_grp1.log('event2')
sleep(1)
print("logging group2/e2-1")
evt_grp2.log('e2-1')


sleep(2)
print("event1:", evt_grp1.get_current_counts('event1'))
print("event2:", evt_grp1.get_current_counts('event2'))
print("e2-1:", evt_grp2.get_current_counts('e2-1'))

