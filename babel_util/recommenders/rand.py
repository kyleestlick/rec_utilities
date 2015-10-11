import random

if __name__ == "__main__":
    import argparse

    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-d', '--delimiter', default='\t')

    args = parser.parse_args()
    for line in args.infile:
        fid, tid = map(str.strip, line.split(args.delimiter))
        value = random.uniform(0, 1)
        args.outfile.write("{}\t{}\n".format(fid, value))
