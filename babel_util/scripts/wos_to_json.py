#!/usr/bin/env python3
from parsers.wos import WOSStream

if __name__ == "__main__":
    import argparse
    import datetime
    import os
    from ujson import dump
    from util.misc import Benchmark, open_file
    parser = argparse.ArgumentParser(description="Creates Pajek (.net) files from WOS XML")
    parser.add_argument('--outdir', help="Directory to write JSON files to")
    parser.add_argument('--wos-only', action="store_true", help="Only include nodes/edges in WOS")
    parser.add_argument('--sample-rate', help="Edge sample rate", type=float, default=None)
    parser.add_argument('--must-cite', action="store_true", help="Only include nodes that cite other nodes")
    parser.add_argument('-a', '--after', help="Only include nodes published on or after this year")
    parser.add_argument('-bf', '--benchmark_freq', help="How often to emit benchmark info", type=int, default=1000000)
    parser.add_argument('infile', nargs='+')
    arguments = parser.parse_args()

    date_after = None
    if arguments.after:
        date_after = datetime.datetime.strptime(arguments.after, "%Y")

    b = Benchmark()

    for file_name in arguments.infile:
        with open_file(file_name, "r") as f:
            p = WOSStream(f, arguments.wos_only, arguments.sample_rate, arguments.must_cite, date_after)
            output_file = "%s.json" % os.path.basename(f.name).split(".", maxsplit=1)[0]
            if arguments.outdir:
                output_file = os.path.join(arguments.outdir, output_file)
            with open(output_file, "w") as g:
                for entry in p.parse():
                    dump(entry, g)
                    g.write("\n")
                    b.increment()
    b.print_freq()


