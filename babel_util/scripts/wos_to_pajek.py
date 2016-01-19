#!/usr/bin/env python3
from parsers.wos import WOSStream
from util.PajekFactory import PajekFactory
from util.misc import open_file, Checkpoint

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from WOS XML")
    parser.add_argument('outfile')
    parser.add_argument('infile', nargs='+')
    args = parser.parse_args()

    chk = Checkpoint()

    nodes = args.outfile + ".nodes"
    edges = args.outfile + ".edges"

    nodes_f = open_file(nodes, "w")
    edges_f = open_file(edges, "w")

    parsed = 1
    total_files = len(args.infile)
    pjk = PajekFactory(node_stream=nodes_f, edge_stream=edges_f)
    for file in args.infile:
        print(chk.checkpoint("Parsing {}: {}/{}: {:.2%}".format(file, parsed, total_files, parsed/float(total_files))))
        f = open_file(file)
        parser = WOSStream(f)
        for entry in parser.parse():
            for citation in entry["citations"]:
                pjk.add_edge(entry["id"], citation)
        print(chk.checkpoint(" Done: "+str(pjk)))
        parsed += 1

    with open_file(args.outfile, "w") as f:
        f.writelines(pjk.make_header())

    chk.end("Done")
