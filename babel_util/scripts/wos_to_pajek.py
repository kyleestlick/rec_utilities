#!/usr/bin/env python3
from parsers.wos import WOSStream
from util.PajekFactory import PajekFactory
from util.misc import open_file
from multiprocessing import Process, JoinableQueue
from datetime import datetime
from io import StringIO


def wos_parser(files, entries, wos_only, sample_rate, must_cite, batch_size):
    batch = []
    for filename in iter(files.get, 'STOP'):
        with open_file(filename) as f:
            p = WOSStream(f, wos_only=wos_only, sample_rate=sample_rate, must_cite=must_cite)
            for entry in p.parse():
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
    pjk = PajekFactory(edge_stream=StringIO(), node_stream=StringIO())
    count = 0
    start_time = datetime.now()
    for entry_list in iter(entries.get, 'STOP'):
        for entry in entry_list:
            for citation in entry["citations"]:
                pjk.add_edge(entry["id"], citation)
            count += 1
        entries.task_done()

    deltaT = datetime.now() - start_time
    print("{} entries processed: {:.2f} entries/s".format(count, 10**6*count/float(deltaT.microseconds)))
    print(pjk)
    #with open_file(arguments.outfile, "w") as f:
    #    pjk.write(f)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from WOS XML")
    parser.add_argument('outfile')
    parser.add_argument('--wos-only', action="store_true", help="Only include nodes/edges in WOS")
    parser.add_argument('--sample-rate', help="Edge sample rate", type=float, default=None)
    parser.add_argument('--must-cite', action="store_true", help="Only include nodes that cite other nodes")
    parser.add_argument('-n', '--num-processes', help="Number of subprocesses to start", default=4, type=int)
    parser.add_argument('-b', '--batch-size', help="Number of entries to batch prior to transmission", default=100, type=int)
    parser.add_argument('infile', nargs='+')
    arguments = parser.parse_args()

    file_queue = JoinableQueue()
    result_queue = JoinableQueue()

    for file in arguments.infile:
        file_queue.put_nowait(file)

    for i in range(arguments.num_processes):
        file_queue.put_nowait('STOP')

    for i in range(arguments.num_processes):
        Process(target=wos_parser, args=(file_queue,
                                         result_queue,
                                         arguments.wos_only,
                                         arguments.sample_rate,
                                         arguments.must_cite,
                                         arguments.batch_size)).start()

    Process(target=pjk_writer, args=(result_queue, arguments.outfile)).start()

    file_queue.join()
    result_queue.join()
    result_queue.put_nowait('STOP')
