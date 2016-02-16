#!/usr/bin/env python3
from parsers.wos import WOSStream
from util.misc import open_file, Benchmark, encode_datetime
from multiprocessing import Process, JoinableQueue
import leveldb
import msgpack


def wos_parser(files, entries, wos_only, sample_rate, must_cite, batch_size, date_after, filter_set):
    log = logging.getLogger(__name__).getChild('parser')
    batch = []
    filtered_out = 0
    wrote = 0
    for filename in iter(files.get, 'STOP'):
        with open_file(filename) as f:
            p = WOSStream(f, wos_only=wos_only, sample_rate=sample_rate, must_cite=must_cite, date_after=date_after)
            for entry in p.parse():
                if filter_set and entry["id"] not in filter_set:
                    filtered_out += 1
                    continue
                batch.append(entry)
                if len(batch) >= batch_size:
                    entries.put(batch)
                    batch = []
                    wrote += batch_size
        if len(batch):
            entries.put(batch)
            wrote += len(batch)
            batch = []
        files.task_done()

    log.info("Wrote %s entries", wrote)
    if filter_set:
        log.info("Excluded %s entries", filtered_out)

    files.task_done()


def leveldb_writer(entries, db_path, batch_size, bench_freq):
    log = logging.getLogger(__name__).getChild('leveldb')
    log.info("Path - %s" % db_path)
    if batch_size:
        log.info("Batch Size - %s" % batch_size)
    log.info("Benchmark Freq - %s" % bench_freq)

    db = leveldb.LevelDB(db_path,
                         error_if_exists=True,
                         write_buffer_size=100 << 20,  # 100MB
                         block_cache_size=400 << 20)  # 400MB
    if batch_size:
        writer = leveldb.WriteBatch()
    else:
        writer = db

    b = Benchmark(bench_freq)
    for entry_list in iter(entries.get, 'STOP'):
        for entry in entry_list:
            db.Put(entry["id"].encode(), msgpack.dumps(entry, default=encode_datetime))
            b.increment()
            if batch_size and b.count % batch_size == 0:
                db.Write(writer)
        entries.task_done()

    if batch_size:
        db.Write(writer)

    b.print_freq()
    log.info(db.GetStats())
    entries.task_done()

if __name__ == "__main__":
    import argparse
    import datetime
    import logging
    from parsers.tree import TreeFile
    import pickle

    parser = argparse.ArgumentParser(description="Creates a LevelDB from WOS XML")
    parser.add_argument('leveldb_path')
    parser.add_argument('--filter', help="Only include IDs in this tree file")
    parser.add_argument('--wos-only', action="store_true", help="Only include nodes/edges in WOS")
    parser.add_argument('--sample-rate', help="Edge sample rate", type=float, default=None)
    parser.add_argument('--must-cite', action="store_true", help="Only include nodes that cite other nodes")
    parser.add_argument('-n', '--num-processes', help="Number of subprocesses to start", default=4, type=int)
    parser.add_argument('-bs', '--batch-size', help="Number of entries to batch prior to transmission", default=100, type=int)
    parser.add_argument('-a', '--after', help="Only include nodes published on or after this year")
    parser.add_argument('-lb', '--leveldb_batch', help="Use LevelDB batch writer", type=int)
    parser.add_argument('-bf', '--benchmark_freq', help="How often to emit benchmark info", type=int, default=1000000)
    parser.add_argument('--log', help="Logging level", default="WARNING", choices=["WARNING", "INFO", "DEBUG", "ERROR", "CRITICAL"])
    parser.add_argument('wosfiles', nargs='+')
    arguments = parser.parse_args()

    logging.basicConfig(level=arguments.log)

    file_queue = JoinableQueue()
    result_queue = JoinableQueue()
    log = logging.getLogger(__name__)

    filter_set = None
    if arguments.filter:
        filter_set = set()
        log.info("Building filter from " + arguments.filter)
        with open_file(arguments.filter) as f:
            if arguments.filter.endswith(".pickle"):
                log.info("Filter is a pickle, unpickling")
                filter_set = pickle.load(f)
            else:
                tf = TreeFile(f)
                filter_set = {e.pid for e in tf}
                pickle_path = arguments.filter+".pickle"
                log.info("Pickling filter to %s" % pickle_path)
                with open_file(pickle_path, "w") as pf:
                    pickle.dump(filter_set, pf, pickle.HIGHEST_PROTOCOL)
                tf = None
            log.info("Found %s ids to include" % len(filter_set))

    date_after = None
    if arguments.after:
        date_after = datetime.datetime.strptime(arguments.after, "%Y")

    for file in arguments.wosfiles:
        file_queue.put_nowait(file)

    for i in range(arguments.num_processes):
        file_queue.put_nowait('STOP')

    for i in range(arguments.num_processes):
        Process(target=wos_parser, args=(file_queue,
                                         result_queue,
                                         arguments.wos_only,
                                         arguments.sample_rate,
                                         arguments.must_cite,
                                         arguments.batch_size,
                                         date_after,
                                         filter_set)).start()

    Process(target=leveldb_writer, args=(result_queue,
                                         arguments.leveldb_path,
                                         arguments.leveldb_batch,
                                         arguments.benchmark_freq)).start()

    file_queue.join()
    result_queue.join()
    result_queue.put_nowait('STOP')
    result_queue.join()
