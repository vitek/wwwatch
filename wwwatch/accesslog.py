import re
from calendar import timegm


accesslog_re = re.compile(
    r'^(?P<remote_addr>[^ ]+)'
    r' - '
    r'(.*?) '
    r'\[(?P<date>.*?)\] '
    r'"(?P<method>[^ ]+) (?P<path>.*?) HTTP/(?P<http_version>1.[01])" '
    r'(?P<status_code>[0-9]{1,3}) '
    r'([0-9]+) '  # bytes sent
    r'"([^"]+)" ' # referer
    r'"([^"]+)" ' # user-agent
    r'"([^"]+)" ' # x-forwarded-for
    r'(?P<extra>.*)$')


class ParseError(Exception):
    pass


def parse_extra(rest):
    """
    >>> parse_extra('')
    {}
    >>> parse_extra('foo=bar')
    {'foo': 'bar'}
    >>> sorted(parse_extra('a=x b=y').iteritems())
    [('a', 'x'), ('b', 'y')]
    >>> parse_extra('foo="hello world"')
    {'foo': 'hello world'}
    >>> sorted(parse_extra('a="hello world" b="happy new year"').iteritems())
    [('a', 'hello world'), ('b', 'happy new year')]
    """
    mapping = {}
    while rest:
        rest = rest.lstrip()
        try:
            idx = rest.index('=')
        except ValueError:
            break
        key, rest = rest[:idx], rest[idx + 1:]
        if rest[:1] == '"':
            try:
                idx = rest.index('"', 1)
            except ValueError:
                idx = len(rest)
            value, rest = rest[1:idx], rest[idx + 1:]
        else:
            try:
                idx = rest.index(' ')
            except ValueError:
                idx = len(rest)
            value, rest = rest[:idx], rest[idx + 1:]
        if value != '-':
            mapping[key] = value
    return mapping


class AccesslogLine(object):
    extra = None

    def __init__(self, date, method, path, http_version, status_code,
                 extra=None, **unused):
        self.date = date
        self.method = method
        self.path = path
        self.http_version = http_version
        self.status_code = status_code
        if extra:
            self.extra = parse_extra(extra)


def parseline(line):
    line = line.strip()
    m = accesslog_re.match(line)
    if m is None:
        raise ParseError("Cannot match line with a regular expression")
    fields = m.groupdict()
    extra = parse_extra(fields['extra'])
    return AccesslogLine(**fields)

MONTHNAMES = {
    name: no for no, name in enumerate(['jan', 'feb', 'mar',
                                        'apr', 'may', 'jun',
                                        'jul', 'aug', 'sep',
                                        'oct', 'nov', 'dec'], 1)
    }


def parse_accesslog_date(data):
    """
    Returns the time in seconds since the Epoch.

    >>> parse_accesslog_date('29/Jul/2014:13:07:06 +0000')
    1406639226
    >>> parse_accesslog_date('29/Jul/2014:17:07:06 +0400')
    1406639226
    >>> parse_accesslog_date('29/Jul/2014:10:07:06 -0300')
    1406639226
    """
    data, tz = data.split(' ')
    date, time = data.split(':', 1)
    day, month, year = date.split('/')
    day = int(day)
    year = int(year)
    month = MONTHNAMES[month.lower()]
    hh, mm, ss = [int(i) for i in time.split(':')]
    if tz[:1] == '-':
        offset = -60 * (int(tz[1:3]) * 60 + int(tz[3:5]))
    else:
        offset = +60 * (int(tz[1:3]) * 60 + int(tz[3:5]))
    return timegm((year, month, day, hh, mm, ss, 0, 0, 0)) - offset


def testme():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    testme()
