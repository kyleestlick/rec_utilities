#!/usr/bin/env python3
import pickle
import shelve
from parsers.tree import TreeFile
from recommenders.ef import make_classic_recs, make_expert_rec
from util.misc import Benchmark

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser(description="Creates EF recommendations and store them in a DBM")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('shelf')
    parser.add_argument('--benchmark-freq', default=10000, type=int)
    parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
    parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
    args = parser.parse_args()

    tf = TreeFile(args.infile)
    b = Benchmark(args.benchmark_freq)
    with shelve.open(args.shelf, flag='n', protocol=pickle.HIGHEST_PROTOCOL) as s:
        for recs in make_expert_rec(tf, args.limit):
            recd = [r.pid for r in recs]
            s['expert|'+recs[0].target_pid] = recd
            b.increment()

        args.infile.seek(0)
        tf = TreeFile(args.infile)
        for recs in make_classic_recs(tf, args.limit):
            recd = [r.pid for r in recs]
            s['classic|'+recs[0].target_pid] = recd
            b.increment()

    b.print_freq()
