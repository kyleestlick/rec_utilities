import gzip
import io
import os
import psutil
from datetime import datetime
import time
from json import JSONEncoder

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info()[0] / float(2 ** 20)

class Benchmark(object):
    def __init__(self, frequency=100000):
        self._start = time.time()
        self._last_time = self._start
        self.count = 0
        self._last_count = self.count
        self.frequency = frequency

    def increment(self, amount=1):
        self.count += amount
        if self.count % self.frequency == 0:
            self.print_freq()

    def print_freq(self):
        new_window = time.time()
        global_delta = new_window - self._start
        count_delta = self.count - self._last_count
        time_delta = new_window - self._last_time
        print("[+{:.2f}s {:,}]\twindow: {:.2f} e/s\ttotal: {:.2f} e/s".format(global_delta,
                                                                           self.count,
                                                                           count_delta/time_delta,
                                                                           self.count/global_delta))

        self._last_time = new_window
        self._last_count = self.count



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


def encode_datetime(obj):
    if isinstance(obj, datetime):
        return {'__datetime__': True, 'as_str': obj.isoformat()}
    return obj


class DatetimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {'__datetime__': True, 'as_str': obj.isoformat()}
        return JSONEncoder.default(self, obj)

#TODO: Implement, probably using dateparse since Python cant reade isoformat dates for some reason...
#def decode_datetime(obj):
#    if b'__datetime__' in obj:
#        obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
#    return obj