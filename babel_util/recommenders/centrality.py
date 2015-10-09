#!/usr/bin/env python
import networkx as nx

def centrality(G):
    total_in_degree = 0.0
    for _, in_degree in G.in_degree_iter():
        total_in_degree += in_degree

    for node_id, in_degree in G.in_degree_iter():
        yield (node_id, in_degree/total_in_degree)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('infile')
    parser.add_argument('-d', '--delimiter', default='/t')
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)

    args = parser.parse_args()
    G = nx.read_edgelist(args.infile, args.delimiter, create_using=nx.DiGraph)

    for node_id, score in centrality(G):
        args.outfile.write("{0}\t{1}\n".format(node_id, score))
    print(nx.info(G))
