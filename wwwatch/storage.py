import socket
import json
from collections import defaultdict

import redis


class BasicStorage(object):
    def __init__(self):
        self.counters = {}

    def register_counter(self, name=''):
        if name not in self.counters:
            self.counters[name] = defaultdict(int)
        return self.counters[name]

    def flush(self, counter, path, position):
        raise NotImplementedError

    def get_last_position(self):
        raise NotImplementedError


class RedisStorage(BasicStorage):
    def __init__(self, hostname, port, prefix):
        super(RedisStorage, self).__init__()
        self.redis = redis.StrictRedis(hostname, port)
        self.prefix = prefix
        self.hostname = socket.gethostname()
        self.key_path = '{}@{}:path'.format(self.prefix, self.hostname)
        self.key_position = '{}@{}:position'.format(self.prefix, self.hostname)

    def flush_counter(self, counter, pipe, name):
        for key, value in counter.iteritems():
            if type(value) is int:
                pipe.hincrby(name, key, value)
            elif type(value) is float:
                pipe.hincrbyfloat(name, key, value)
            else:
                raise ValueError("Unsupported counter type for {}".format(key))

    def flush(self, path, position):
        with self.redis.pipeline() as pipe:
            for name, counter in self.counters.iteritems():
                if name:
                    key_name = '{}:{}'.format(self.prefix, name)
                else:
                    key_name = self.prefix
                self.flush_counter(counter, pipe, key_name)
                self.flush_counter(counter, pipe,
                                   '{}@{}'.format(key_name, self.hostname))
                pipe.set(self.key_position, position)
                pipe.set(self.key_path, path)
            pipe.execute()
        for counter in self.counters.itervalues():
            counter.clear()

    def get_last_position(self):
        path, position = self.redis.mget(self.key_path, self.key_position)
        if position is not None:
            position = int(position)
        return path, position


class JSONFileStorage(BasicStorage):
    def __init__(self, path):
        super(JSONFileStorage, self).__init__()
        self.path = path

    def read(self):
        try:
            with open(self.path) as fp:
                data = json.load(fp)
        except IOError:
            data = {}
        return data

    def write(self, data):
        data = json.dumps(data)
        with open(self.path, 'w') as fp:
            fp.write(data)

    def get_last_position(self):
        data = self.read()
        return data.get('path', None), data.get('position', 0)

    def flush(self, path, position):
        data = self.read()
        data['path'] = path
        data['position'] = position
        for name, counter in self.counters.iteritems():
            if not name:
                name = 'counters'
            json_counters = data.setdefault(name, {})
            for key, value in counter.iteritems():
                json_counters[key] = json_counters.get(key, 0) + value
        self.write(data)

        for counter in self.counters.itervalues():
            counter.clear()
