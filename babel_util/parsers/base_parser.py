import gzip


class AbstractParser(object):
    def __init__(self, stream, delimiter="\t"):
        self._close_stream = False

        if isinstance(stream, str):
            if stream.endswith(".gz"):
                self._stream = gzip.open(stream, mode="rt")
            else:
                self._stream = open(stream, "r")
            self._close_stream = True
        else:
            self._stream = stream

        self._delimiter = delimiter

    def __del__(self):
        if self._close_stream:
            self._stream.close()

    def parse_line(self, line):
        return list(map(str.strip, line.split(self.delimiter)))

    def __iter__(self):
        self._iter = iter(self._stream)
        return self

    def __next__(self):
        return self.next()

    def next(self):
        line = self._iter.__next__()
        while self.comment and line.startswith(self.comment):
            line = self._iter.__next__()

        return self.parse_line(line)
