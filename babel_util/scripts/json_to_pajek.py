#!/usr/bin/env python3
from util.misc import open_file, Benchmark
from util.PajekFactory import PajekFactory
import ujson


def json_parser(files, entries, batch_size):
    batch = []
    for filename in iter(files.get, 'STOP'):
        with open_file(filename) as f:
            for line in f:
                entry = ujson.loads(line)
                batch.append(entry)
                if len(batch) >= batch_size:
                    entries.put(batch)
                    batch = []
        if len(batch):
            entries.put(batch)
            batch = []
        files.task_done()
    files.task_done()


def pjk_writer(entries, output_file):
    pjk = PajekFactory()
    b = Benchmark()
    for entry_list in iter(entries.get, 'STOP'):
        for entry in entry_list:
            for citation in entry["citations"]:
                pjk.add_edge(entry["id"], citation)
            b.increment()
        entries.task_done()

    b.print_freq()
    with open_file(output_file, "w") as f:
        pjk.write(f)
    entries.task_done()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from JSON")
    parser.add_argument('outfile')
    parser.add_argument('-n', '--num-processes', help="Number of subprocesses to run on. 0 means single process.", default=0, type=int)
    parser.add_argument('-b', '--batch-size', help="Number of entries to batch prior to transmission", default=100, type=int)
    parser.add_argument('infile', nargs='+')
    arguments = parser.parse_args()

    if arguments.num_processes == 0:
        b = Benchmark()
        pjk = PajekFactory()

        for filename in arguments.infile:
            with open_file(filename) as f:
                for line in f:
                    entry = ujson.loads(line)
                    for citation in entry["citations"]:
                        pjk.add_edge(entry["id"], citation)
                    b.increment()

        b.print_freq()
        with open_file(arguments.outfile, "w") as f:
            pjk.write(f)
    else:
        from multiprocessing import Process, JoinableQueue
        file_queue = JoinableQueue()
        result_queue = JoinableQueue()

        for file in arguments.infile:
            file_queue.put_nowait(file)

        for i in range(arguments.num_processes):
            file_queue.put_nowait('STOP')

        for i in range(arguments.num_processes):
            Process(target=json_parser, args=(file_queue,
                                              result_queue,
                                              arguments.batch_size)).start()

        Process(target=pjk_writer, args=(result_queue, arguments.outfile, arguments.benchmark_freq)).start()

        file_queue.join()
        result_queue.join()
        result_queue.put_nowait('STOP')
        result_queue.join()

