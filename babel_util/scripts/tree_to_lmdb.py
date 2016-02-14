#!/usr/bin/env python3
import lmdb
from recommenders.ef import make_expert_rec, make_classic_recs
from parsers.tree import TreeFile
import msgpack
from util.misc import Benchmark

if __name__ == "__main__":
    import argparse
    import sys
    import platform
    parser = argparse.ArgumentParser(description="Creates LMDB (.lmdb) from a tree file (.tree)")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('db_prefix')
    parser.add_argument('--benchmark-freq', default=10000, type=int)
    parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
    parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
    args = parser.parse_args()

    if platform.system() == 'Linux':
        map_size = 500 * 1024 * 1024 * 1024  # 500GB
    else:
        map_size = 10 * 1024 * 1024  # 10MB on OSX. Sparse files don't work well on ancient FS.

    env = lmdb.Environment(args.db_prefix, map_async=True, writemap=True, max_dbs=2, map_size=map_size)
    classic = env.open_db(key=b'classic')
    expert = env.open_db(key=b'expert')
    tf = TreeFile(args.infile)
    b = Benchmark(args.benchmark_freq)
    with env.begin(db=expert, write=True) as txn:
        for recs in make_expert_rec(tf, args.limit):
            recd = [r.pid for r in recs]
            txn.put(recs[0].target_pid.encode(), msgpack.packb(recd))
            b.increment()

    args.infile.seek(0)
    tf = TreeFile(args.infile)
    with env.begin(db=classic, write=True) as txn:
        for recs in make_classic_recs(tf, args.limit):
            recd = [r.pid for r in recs]
            txn.put(recs[0].target_pid.encode(), msgpack.packb(recd))
            b.increment()

    print(env.stat())
    b.print_freq()
