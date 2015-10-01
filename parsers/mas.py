from collections import defaultdict

current_idx = 0

def accum():
    global current_idx
    current_idx += 1
    return current_idx

def invert_dict(d):
    return dict(zip(d.values(), d.keys()))

class MASParser:
    def __init__(self):
        self.ids = defaultdict(accum)
        self.edges = defaultdict(list)
        self.edge_count = 0

    def parse_papers(self, stream):
        for line in stream:
            from_id, to_id = map(str.strip, line.split('\t'))
            self.edges[self.ids[from_id]].append(self.ids[to_id])
            self.edge_count += 1

    def to_pajek(self, stream):
        invert_ids = invert_dict(self.ids)
        stream.write("*Vertices {}\n".format(len(self.ids)))
        for x in range(1, len(self.ids) + 1):
            stream.write("{} \"{}\"\n".format(x, invert_ids[x]))

        stream.write("*Edges {}\n".format(self.edge_count))
        for k, vlist in self.edges.items():
            for v in vlist:
                stream.write("{} {}\n".format(k, v))


if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()

    p = MASParser()
    p.parse_papers(args.infile)
    p.to_pajek(args.outfile)
