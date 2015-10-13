#!/usr/bin/env python

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-d', '--delimiter', default='\t')
    parser.add_argument('-o', '--old', help="Rank older papers higher", action="store_true")

    args = parser.parse_args()

    for line in args.infile:
        pid, title_raw, title_normalized, year, date, doi, venue_raw, venue_normalized, jid_to_venue, cid_to_venue, rank = map(str.strip, line.split(args.delimiter))
        if year:
            try:
                if args.old:
                    score = 1.0/float(year)*1000.0
                else:
                    score = 1.0-1.0/float(year)*1000.0
                args.outfile.write("{}\t{}\n".format(pid, score))
            except ValueError:
                pass
