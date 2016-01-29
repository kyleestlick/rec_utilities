#!/usr/bin/env python3
from parsers.wos import WOSStream
from util.PajekFactory import PajekFactory
from util.misc import open_file
from multiprocessing import Process, JoinableQueue
from datetime import datetime


def wos_parser(files, entries, wos_only, sample_rate, must_cite):
    for filename in iter(files.get, 'STOP'):
        with open_file(filename) as f:
            p = WOSStream(f, wos_only=wos_only, sample_rate=sample_rate, must_cite=must_cite)
            for entry in p.parse():
                entries.put(entry)
        files.task_done()
    files.task_done()


def pjk_writer(entries, pjk):
    count = 0
    last_time = datetime.now()
    for entry in iter(entries.get, 'STOP'):
        if "citations" in entry:
            for citation in entry["citations"]:
                pjk.add_edge(entry["id"], citation)
        entries.task_done()
        count += 1
        if count % 500 == 0:
            deltaT = datetime.now() - last_time
            if deltaT.seconds: #Sometimes we are just too fast
                print("{} entries process: {:.2f} entries/s".format(count, count/deltaT.seconds))
            last_time = datetime.now()
    print(pjk)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from WOS XML")
    parser.add_argument('outfile')
    parser.add_argument('--wos-only', action="store_true", help="Only include nodes/edges in WOS")
    parser.add_argument('--sample-rate', help="Edge sample rate", type=float, default=None)
    parser.add_argument('--must-cite', action="store_true", help="Only include nodes that cite other nodes")
    parser.add_argument('-n', '--num-processes', help="Number of subprocesses to start", default=4, type=int)
    parser.add_argument('infile', nargs='+')
    arguments = parser.parse_args()

    file_queue = JoinableQueue()
    result_queue = JoinableQueue()
    pjk = PajekFactory()

    for file in arguments.infile:
        file_queue.put_nowait(file)

    for i in range(arguments.num_processes):
        file_queue.put_nowait('STOP')

    for i in range(arguments.num_processes):
        Process(target=wos_parser, args=(file_queue, result_queue, arguments.wos_only, arguments.sample_rate, arguments.must_cite)).start()

    Process(target=pjk_writer, args=(result_queue, pjk)).start()

    file_queue.join()
    result_queue.join()
    result_queue.put_nowait('STOP')
    with open_file(arguments.outfile, "w") as f:
        pjk.write(f)
