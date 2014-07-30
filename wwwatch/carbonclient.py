import socket

class CarbonConnectionError(Exception):
    pass


class CarbonClient(object):
    def __init__(self, hostname='localhost', port=2003):
        self.hostname = hostname
        self.port = port
        self.socket = None

    def connect(self):
        if not self.socket:
            try:
                self.socket = socket.socket()
                self.socket.connect((self.hostname, self.port))
            except socket.error, e:
                raise CarbonConnectionError(str(e))

    def send(self, values):
        buf = ''
        for name, value, timestamp in values:
            buf += '{} {} {}\n'.format(name, value, timestamp)
        self.connect()
        try:
            self.socket.sendall(buf)
        except socket.error, e:
            self.socket.close()
            self.socket = None
            raise CarbonConnectionError(str(e))
