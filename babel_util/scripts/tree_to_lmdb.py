#!/usr/bin/env python3
import lmdb
from recommenders.ef import make_expert_rec, make_classic_recs
from parsers.tree import TreeFile
import msgpack

if __name__ == "__main__":
    import argparse
    import sys
    import platform
    import time
    parser = argparse.ArgumentParser(description="Creates LMDB (.lmdb) from a tree file (.tree)")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('db_prefix')
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
    count = 0
    start = time.time()
    with env.begin(db=expert, write=True) as txn:
        for recs in make_expert_rec(tf, args.limit):
            recd = [r.pid for r in recs]
            txn.put(recs[0].target_pid.encode(), msgpack.packb(recd))
            count += 1
            if count % 10000 == 0:
                print("%s entry/s" % round(count/(time.time() - start), 2))

    args.infile.seek(0)
    tf = TreeFile(args.infile)
    with env.begin(db=classic, write=True) as txn:
        for recs in make_classic_recs(tf, args.limit):
            recd = [r.pid for r in recs]
            txn.put(recs[0].target_pid.encode(), msgpack.packb(recd))
            count += 1
            if count % 10000 == 0:
                print("%s entry/s" % round(count/(time.time() - start), 2))

    print(env.stat())
    print("%s entry/s" % round(count/(time.time() - start), 2))
    print("%s entries in %s seconds" % (count, round(time.time() - start, 2)))
