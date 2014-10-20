import socket
import json

import redis


class BasicStorage(object):
    def flush(self, counter, path, position):
        raise NotImplementedError

    def get_last_position(self):
        raise NotImplementedError


class RedisStorage(BasicStorage):
    def __init__(self, hostname, port, name):
        self.redis = redis.StrictRedis(hostname, port)
        self.name = name
        self.hostname = socket.gethostname()
        self.key_path = '{}@{}:path'.format(self.name, self.hostname)
        self.key_position = '{}@{}:position'.format(self.name, self.hostname)

    def flush_counter(self, counter, pipe, name):
        for key, value in counter.iteritems():
            if type(value) is int:
                pipe.hincrby(name, key, value)
            elif type(value) is float:
                pipe.hincrbyfloat(name, key, value)
            else:
                raise ValueError("Unsupported counter type for {}".format(key))

    def flush(self, counter, path, position):
        with self.redis.pipeline() as pipe:
            self.flush_counter(counter, pipe, self.name)
            self.flush_counter(counter, pipe,
                               '{}@{}'.format(self.name, self.hostname))
            pipe.set(self.key_position, position)
            pipe.set(self.key_path, path)
            pipe.execute()

    def get_last_position(self):
        path, position = self.redis.mget(self.key_path, self.key_position)
        if position is not None:
            position = int(position)
        return path, position


class JSONFileStorage(BasicStorage):
    def __init__(self, path):
        self.path = path

    def read(self):
        try:
            with open(self.path) as fp:
                data = json.load(fp)
        except IOError:
            data = {}
        if 'counters' not in data:
            data['counters'] = {}
        return data

    def write(self, data):
        data = json.dumps(data)
        with open(self.path, 'w') as fp:
            fp.write(data)

    def get_last_position(self):
        data = self.read()
        return data.get('path', None), data.get('position', 0)

    def flush(self, counter, path, position):
        data = self.read()
        data['path'] = path
        data['position'] = position
        json_counters = data['counters']
        for key, value in counter.iteritems():
            json_counters[key] = json_counters.get(key, 0) + value
        self.write(data)
