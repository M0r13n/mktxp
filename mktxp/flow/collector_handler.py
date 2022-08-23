# coding=utf8
## Copyright (c) 2020 Arseniy Kuznetsov
##
## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License
## as published by the Free Software Foundation; either version 2
## of the License, or (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

import concurrent.futures
from timeit import default_timer

MAX_CONCURRENT_THREADS = 5


class CollectorHandler:
    ''' MKTXP Collectors Handler
    '''
    def __init__(self, entries_handler, collector_registry):
        self.entries_handler = entries_handler
        self.collector_registry = collector_registry


    def _collect_single_router(self, router_entry):
        if not router_entry.api_connection.is_connected():
            # let's pick up on things in the next run
            router_entry.api_connection.connect()

        for collector_ID, collect_func in self.collector_registry.registered_collectors.items():                
            start = default_timer()
            yield from list(collect_func(router_entry))
            router_entry.time_spent[collector_ID] += default_timer() - start


    def collect(self):
        yield from self.collector_registry.bandwidthCollector.collect()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_THREADS) as executor:
            futures = set()
            for router_entry in self.entries_handler.router_entries:
                future = executor.submit(self._collect_single_router, router_entry)
                futures.add(future)

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                yield from result

