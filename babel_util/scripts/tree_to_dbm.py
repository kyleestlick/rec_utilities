#!/usr/bin/env python3
import pickle
import shelve
from parsers.tree import TreeFile
from recommenders.ef import make_classic_recs, make_expert_rec

if __name__ == "__main__":
    import argparse
    import sys
    import time
    parser = argparse.ArgumentParser(description="Creates EF recommendations and store them in a DBM")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('shelf')
    parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
    parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
    args = parser.parse_args()

    tf = TreeFile(args.infile)
    count = 0
    start = time.time()
    with shelve.open(args.shelf, flag='n', protocol=pickle.HIGHEST_PROTOCOL) as s:
        for recs in make_expert_rec(tf, args.limit):
            recd = [r.pid for r in recs]
            s[recs[0].target_pid] = recd
            count += 1
            if count % 10000 == 0:
                print("%s entry/s" % round(count/(time.time() - start), 2))

        args.infile.seek(0)
        tf = TreeFile(args.infile)
        for recs in make_classic_recs(tf, args.limit):
            recd = [r.pid for r in recs]
            s[recs[0].target_pid] = recd
            count += 1
            if count % 10000 == 0:
                print("%s entry/s" % round(count/(time.time() - start), 2))

    print("%s entry/s" % round(count/(time.time() - start), 2))
    print("%s entries in %s seconds" % (count, round(time.time() - start, 2)))
