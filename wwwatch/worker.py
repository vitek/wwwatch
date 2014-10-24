import sys
import datetime
import socket
from contextlib import closing
from collections import defaultdict

from .accesslog import parseline, parse_accesslog_date, ParseError
from .taillog import Taillog


def comma_split(string):
    return [i.strip() for i in string.split(',')]


def handle_line(counter, line):
    counter['total'] += 1
    counter['method_' + line.method] += 1
    counter['version:' + line.http_version] += 1
    counter['status_' + line.status_code] += 1

    extra = line.extra
    # Cache status
    cache_status = extra.get('cs', None)
    if cache_status:
        counter['cache_' + cache_status] += 1

    # Response time
    if 'rt' in extra:
        response_time = float(extra['rt'])
        counter['response_time'] += response_time

    # Upstream time
    if 'ut' in extra:
        utimes = comma_split(extra['ut'])
        for upstream_time in utimes:
            try:
                upstream_time = float(upstream_time)
            except ValueError:
                continue
            counter['upstream_time'] += upstream_time
            counter['upstream'] += 1
        if len(utimes) > 1:
            counter['upstream_next'] += len(utimes) - 1


class WWWatchWorker(object):
    def __init__(self, storage, fname, flush_interval=15):
        self.storage = storage
        self.counter = defaultdict(int)
        self.fname = fname
        self.flush_interval = flush_interval

    def flush(self, taillog):
        if not self.counter:
            return
        path, position = taillog.get_position()
        self.storage.flush(self.counter, path, position)
        self.counter.clear()

    def run(self):
        path, position = self.storage.get_last_position()
        if path != self.fname:
            position = 0

        last_flush = None
        with closing(Taillog(self.fname, position=position,
                             idle_func=self.flush)) as taillog:
            for line in taillog:
                try:
                    line = parseline(line)
                except ParseError:
                    self.counter['errors'] += 1
                    continue
                timestamp = parse_accesslog_date(line.date)

                handle_line(self.counter, line)

                if last_flush is None:
                    last_flush = timestamp
                elif (timestamp - last_flush) > self.flush_interval:
                    self.flush(taillog)
                    last_flush = timestamp
