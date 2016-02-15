#!/usr/bin/env python3
import leveldb
from recommenders.ef import make_expert_rec, make_classic_recs
from parsers.tree import TreeFile
import msgpack
from util.misc import Benchmark

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser(description="Creates recommendations and stores in an LevelDB from a tree file (.tree)")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('db_path')
    parser.add_argument('--batch-size', default=None, type=int)
    parser.add_argument('--benchmark-freq', default=10000, type=int)
    parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
    parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
    args = parser.parse_args()

    db = leveldb.LevelDB(args.db_path,
                         write_buffer_size=100 << 20,  # 100MB
                         block_cache_size=400 << 20)  # 400MB
    b = Benchmark(args.benchmark_freq)
    tf = TreeFile(args.infile)

    if args.batch_size:
        writer = leveldb.WriteBatch()
    else:
        writer = db

    for recs in make_expert_rec(tf, args.limit):
        recd = [r.pid for r in recs]
        key = recs[0].target_pid+"|expert"
        writer.Put(key.encode(), msgpack.packb(recd))
        b.increment()
        if args.batch_size and b.count % args.batch_size == 0:
            db.Write(writer)

    args.infile.seek(0)
    tf = TreeFile(args.infile)
    for recs in make_classic_recs(tf, args.limit):
        recd = [r.pid for r in recs]
        key = recs[0].target_pid+"|classic"
        writer.Put(key.encode(), msgpack.packb(recd))
        b.increment()
        if args.batch_size and b.count % args.batch_size == 0:
            db.Write(writer)

    if args.batch_size:
        db.Write(writer, sync=True)

    b.print_freq()
    print(db.GetStats())
