import time
import os


class Taillog(object):
    def __init__(self, path, position=0, delay=0.1, idle_func=None):
        self.delay = delay
        self.path = path
        self.fp = None
        self.fp_inode = None
        self.initial_position = position
        self.idle_func = idle_func

    def close(self):
        if self.fp:
            self.fp.close()
            self.fp = None
            self.position = None

    def __iter__(self):
        return self

    def get_position(self):
        if self.fp:
            return (self.fp.name, self.fp.tell())
        else:
            return (None, 0)

    def open(self):
        try:
            self.fp = open(self.path, 'rt')
        except IOError:
            return False
        st = os.fstat(self.fp.fileno())
        self.fp_inode = st.st_ino
        self.restore_initial_position()
        return True

    def restore_initial_position(self):
        if self.initial_position:
            st = os.fstat(self.fp.fileno())
            if self.initial_position <= st.st_size:
                self.fp.seek(self.initial_position - 1)
                if self.fp.read(1) != '\n':
                    self.fp.seek(0)
            self.initial_position = 0

    def sleep(self):
        if self.idle_func:
            self.idle_func(self)
        time.sleep(self.delay)

    def check_eof(self):
        try:
            st = os.stat(self.path)
        except OSError:
            return False
        return st.st_ino != self.fp_inode

    def next(self):
        line = ''
        while True:
            if self.fp is None:
                while not self.open():
                    self.sleep()
                line = ''
            line += self.fp.readline()
            if line[-1:] == '\n':
                return line
            # XXX: last line may be lost
            if self.check_eof():
                self.close()
                continue
            self.sleep()


def test_main():
    from contextlib import closing
    import sys
    with closing(Taillog(sys.argv[1], 12)) as taillog:
        for line in taillog:
            print 'GOT LINE:', line.strip(), taillog.get_position()


if __name__ == "__main__":
    test_main()
