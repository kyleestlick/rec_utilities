import gzip
import io
import os
import psutil
from datetime import datetime


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info()[0] / float(2 ** 20)


class Checkpoint:
    def __init__(self):
        self._start = datetime.now()
        self._last_event = self._start
        self.events = []

    def checkpoint(self, description=None):
        now = datetime.now()
        delta = now - self._last_event
        event = [now, delta, get_memory_usage(), description]
        self.events.append(event)
        self._last_event = now
        return self.event_to_string(event)

    def event_to_string(self, event):
        return "[{:%H:%M:%S} +{}s {:,}MB] {}".format(event[0], (event[0]-self._start).seconds, int(event[2]), event[3])

    def end(self, description="End Run"):
        self.checkpoint(description)
        self.summary()

    def summary(self):
        cum_time = (self._last_event - self._start).total_seconds()
        print("==== Summary ====")
        print("{} events over {} seconds".format(len(self.events), cum_time))
        for e in self.events:
            print("[{} seconds] {:.2%} total time - {}".format(e[1].seconds, e[1].total_seconds()/cum_time, e[3]))


def open_file(filename, mode="r", encoding=None):
    if isinstance(filename, io.TextIOWrapper):
        return filename

    if filename.endswith(".gz"):
        if mode == "r":
            f = gzip.open(filename, "rt", encoding=encoding)
        elif mode == "w":
            f = gzip.open(filename, "wt", encoding=encoding)
        else:
            raise ValueError("Unknown mode for .gz: " + mode)
    elif filename.endswith(".pickle"):
        if mode == "r":
            f = open(filename, "rb")
        elif mode == "w":
            f = open(filename, "wb")
        else:
            raise ValueError("Unknown mode for .pickle: " + mode)
    else:
        f = open(filename, mode)
    return f