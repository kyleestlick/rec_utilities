#!/usr/bin/env python
#NOTE: On the MAS dataset this appears to take ~15 minutes and 4GB of memory
from collections import defaultdict


class Centrality:
    def __init__(self):
        self.nodes = defaultdict(int)
        self.edge_count = 0.0

    def parse_file(self, stream, delimiter='\t'):
        for line in stream:
            _, to_id = map(str.strip, line.split(delimiter))
            self.nodes[to_id] += 1
            self.edge_count += 1

        for node_id, count in self.nodes.iteritems():
            yield (node_id, count/self.edge_count)

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-d', '--delimiter', default='\t')

    args = parser.parse_args()
    centrality = Centrality()

    for node_id, score in centrality.parse_file(args.infile):
        args.outfile.write("{0}\t{1}\n".format(node_id, score))
