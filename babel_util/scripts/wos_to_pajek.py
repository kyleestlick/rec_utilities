#!/usr/bin/env python3
from parsers.wos import WOS
from util.PajekFactory import PajekFactory
from util.misc import open_file, Checkpoint

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from WOS XML")
    parser.add_argument('outfile')
    parser.add_argument('infile', nargs='+')
    args = parser.parse_args()

    chk = Checkpoint()

    parsed = 1
    total_files = len(args.infile)
    pjk = PajekFactory()
    for file in args.infile:
        print(chk.checkpoint("Parsing {}: {}/{}: {:.2%}".format(file, parsed, total_files, parsed/float(total_files))))
        f = open_file(file)
        parser = WOS(f)
        for entry in parser.parse():
            for citation in entry["citations"]:
                pjk.add_edge(entry["id"], citation)
        print(chk.checkpoint(" Done: "+str(pjk)))
        parsed += 1

    chk.checkpoint("PJK->Disk")
    with open_file(args.outfile, "w") as f:
        pjk.write(f)

    chk.end("Done")
