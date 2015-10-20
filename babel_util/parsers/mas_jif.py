#!/usr/bin/env python
import mas
from collections import defaultdict

def papers_to_jid(stream):
    papers = dict()
    for line in stream:
        entry = mas.parse_line_tuple(line)
        pid = entry[mas.FIELDS["pid"]]
        journal_id = entry[mas.FIELDS["journal_id"]]
        if journal_id:
            papers[pid] = journal_id
    return papers


if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('papers', help="Papers.txt", type=argparse.FileType('r'))
    parser.add_argument('links',  help="PaperReferences.txt", type=argparse.FileType('r'))
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-d', '--delimiter', default='\t')

    args = parser.parse_args()
    jid = papers_to_jid(args.papers)

    journal_links = defaultdict(int)
    for line in args.links:
        fid, tid = list(map(str.strip, line.split(args.delimiter)))
        try:
            key = (jid[fid], jid[tid])
            journal_links[key] += 1
        except KeyError:
            pass

    for key, score in journal_links.items():
        args.outfile.write("{}\t{}\t{}\n".format(key[0], key[1], score))
