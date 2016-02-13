#!/usr/bin/env python3

from __future__ import print_function
from storage.dynamo import Table, TABLE_DEFINITION, DATASETS
from contextlib import closing
import itertools
from decimal import Decimal
import boto3
import time
import logging
from parsers.tree import TreeFile
from recommenders.ef import make_expert_rec, make_classic_recs

def make_key(e):
    return "|".join((e.target_pid, e.rec_type, str(e.score)))


def process_record_stream(stream):
    for group in stream:
        hashkey_stream = itertools.groupby(group, make_key)
        for (key, stream) in hashkey_stream:
            # Boto doesn't support lists, ony sets
            recs = set([s.pid for s in stream])
            yield debucketer(key, recs)


def debucketer(key, value):
    (hash_key, ef) = key.rsplit("|", 1)
    # Boto's handling of float's is poor. Decimals, however, work fine.
    # See https://github.com/boto/boto/issues/2413
    return {TABLE_DEFINITION["hash_key"]: hash_key,
            TABLE_DEFINITION["range_key"]: Decimal(ef),
            TABLE_DEFINITION["rec_attribute"]: value}

if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Transform recommender output to DynamoDB")
    parser.add_argument("dataset", help="Dataset", choices=DATASETS)
    parser.add_argument("tree", help="file to transform", type=argparse.FileType('r'))
    parser.add_argument("--region", help="Region to connect to", default="us-east-1")
    parser.add_argument("-c", "--create", help="create table in database", action="store_true")
    parser.add_argument("-f", "--flush", help="flush database.", action="store_true")
    parser.add_argument("-d", "--dryrun", help="Process data, but don't insert into DB", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    if args.region == "localhost":
        client = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    else:
        client = boto3.resource('dynamodb')

    t = Table(client, args.dataset)

    if args.flush:
        logging.info("Deleting table: " + t.table_name)
        if not args.dryrun:
            t.delete()

    if args.create:
        logging.info("Creating table: " + t.table_name)
        if not args.dryrun:
            t.create(write=2000)

    entries = 0
    start = time.time()

    parser = TreeFile(args.tree)

    with t.get_batch_put_context() as batch:
        print("Generating expert recommendations...")
        for expert_rec in process_record_stream(make_expert_rec(parser)):
            if args.verbose:
                print(expert_rec)
            if not args.dryrun:
                batch.put_item(expert_rec)
            entries += 1
            if entries % 50000 == 0:
                current_time = time.time()
                current_rate = entries/(current_time - start)
                print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, time.time()-start, entries/(time.time()-start)))
                sys.stdout.flush()

        # Reset for the second pass
        print("Generating classic recommendations...")
        args.tree.seek(0)
        parser = TreeFile(args.tree)
        for classic_rec in process_record_stream(make_classic_recs(parser)):
            if args.verbose:
                print(classic_rec)
            if not args.dryrun:
                batch.put_item(classic_rec)
            entries += 1
            if entries % 50000 == 0:
                current_time = time.time()
                current_rate = entries/(current_time - start)
                print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, time.time()-start, entries/(time.time()-start)))
                sys.stdout.flush()
    end = time.time()
    print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, end-start, entries/(end-start)))

    if not args.dryrun:
        t.update_throughput()

