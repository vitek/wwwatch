import sys
import datetime
import socket
from contextlib import closing
from collections import defaultdict

import redis

from .accesslog import parseline, parse_accesslog_date, ParseError
from .taillog import Taillog


def comma_split(string):
    return [i.strip() for i in string.split(',')]


def flush_counter(pipe, name, counter):
    for key, value in counter.iteritems():
        if type(value) is int:
            pipe.hincrby(name, key, value)
        elif type(value) is float:
            pipe.hincrbyfloat(name, key, value)
        else:
            raise ValueError("Unsupported counter type for {}".format(key))


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
                counter['upstream_time'] += float(upstream_time)
            except ValueError:
                from ipdb import set_trace; set_trace()
            counter['upstream'] += 1
        if len(utimes) > 1:
            counter['upstream_next'] += len(utimes) - 1


class WWWatchWorker(object):
    def __init__(self, redis, fname, name, flush_interval=15):
        self.redis = redis
        self.fname = fname
        self.name = name
        self.flush_interval = flush_interval
        self.counter = defaultdict(int)
        self.hostname = socket.gethostname()
        self.key_path = '{}@{}:path'.format(self.name, self.hostname)
        self.key_position = '{}@{}:position'.format(self.name, self.hostname)

    def flush(self, taillog):
        if not self.counter:
            return
        path, position = taillog.get_position()
        with self.redis.pipeline() as pipe:
            flush_counter(pipe, self.name, self.counter)
            flush_counter(pipe, '{}@{}'.format(self.name, self.hostname),
                          self.counter)
            pipe.set(self.key_position, position)
            pipe.set(self.key_path, path)
            pipe.execute()
        self.counter.clear()

    def run(self):
        path, position = self.redis.mget(self.key_path, self.key_position)
        if path != self.fname:
            position = 0
        else:
            position = int(position)

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


def main():
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--redis-hostname', help='Redis server hostname',
                        default='localhost')
    parser.add_argument('--redis-port', help='Redis server port',
                        type=int, default=6379)

    parser.add_argument('access_log', help="Path to access log file",
                        metavar="access-log")
    parser.add_argument('name', help="Redis key name")

    args = parser.parse_args()

    redis_client = redis.StrictRedis(args.redis_hostname, args.redis_port)

    worker = WWWatchWorker(redis_client, args.access_log, args.name)
    worker.run()


if __name__ == "__main__":
    main()
