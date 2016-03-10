#!/usr/bin/env python3
from util.misc import open_file, Benchmark
from util.PajekFactory import PajekFactory
import ujson

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from JSON")
    parser.add_argument('outfile')
    parser.add_argument('--wos-only', action="store_true", help="Only include nodes/edges in WOS")
    parser.add_argument('--sample-rate', help="Edge sample rate", type=float, default=None)
    parser.add_argument('--must-cite', action="store_true", help="Only include nodes that cite other nodes")
    parser.add_argument('-n', '--num-processes', help="Number of processes to run on. 1 means single process.", default=1, type=int)
    parser.add_argument('-b', '--batch-size', help="Number of entries to batch prior to transmission", default=100, type=int)
    parser.add_argument('-a', '--after', help="Only include nodes published on or after this year")
    parser.add_argument('-bf', '--benchmark_freq', help="How often to emit benchmark info", type=int, default=1000000)
    parser.add_argument('infile', nargs='+')
    arguments = parser.parse_args()

    b = Benchmark()
    pjk = PajekFactory()

    for filename in arguments.infile:
        with open_file(filename) as f:
            for line in f:
                entry = ujson.dumps(line)
                for citation in entry["citations"]:
                    pjk.add_edge(entry["id"], citation)
                b.increment()

    b.print_freq()
    with open_file(arguments.outfile, "w") as f:
        pjk.write(f)

