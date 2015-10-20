from collections import defaultdict

current_idx = 0

FIELDS = {"pid": 0,
          "title_raw": 1,
          "title_normalized": 2,
          "year": 3,
          "date": 4,
          "doi": 5,
          "venue_raw": 6,
          "venue_normalized": 7,
          "journal_id": 8,
          "conference_id": 9}


def accum():
    global current_idx
    current_idx += 1
    return current_idx


def invert_dict(d):
    return dict(zip(d.values(), d.keys()))


def parse_line_tuple(line, delimiter='\t'):
    return map(str.strip, line.split(delimiter))


class MASAbstract(object):
    def __init__(self, stream, delimiter="\t"):
        self._stream = stream
        self._delimiter = delimiter

    def parse_line(self, line):
        return list(map(str.strip, line.split(self.delimiter)))

    def __iter__(self):
        self._iter = iter(self._stream)
        return self

    def __next__(self):
        return self.next()

    def next(self):
        line = self._iter.__next__()
        while self.comment and line.startswith(self.comment):
            line = self._iter.__next__()

        return self.parse_line(line)


class Papers(MASAbstract):
    FIELDS = {"pid": 0,
              "title_raw": 1,
              "title_normalized": 2,
              "year": 3,
              "date": 4,
              "doi": 5,
              "venue_raw": 6,
              "venue_normalized": 7,
              "journal_id": 8,
              "conference_id": 9}

    def __init__(self, stream, delimiter="\t"):
        super(MASAbstract, self).__init__(stream, delimiter)

    def get_journal_count(self):
        journals = defaultdict(int)
        for entry in self:
            journals[entry[Papers.FIELDS["journal_id"]]] += 1

        return dict(journals)


class PaperReferences:
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

    p = Papers(args.infile)
    journal_counts = p.get_journal_count()
    for jid, count in journal_counts.items():
        args.outfile.write("{}\t{}\n".format(jid, count))
