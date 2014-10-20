import socket

import redis


class BasicStorage(object):
    def flush(self, path, position):
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
        if not counter:
            return
        with self.redis.pipeline() as pipe:
            self.flush_counter(counter, pipe, self.name)
            self.flush_counter(counter, pipe,
                               '{}@{}'.format(self.name, self.hostname))
            pipe.set(self.key_position, position)
            pipe.set(self.key_path, path)
            pipe.execute()
            counter.clear()

    def get_last_position(self):
        path, position = self.redis.mget(self.key_path, self.key_position)
        if position is not None:
            position = int(position)
        return path, position
