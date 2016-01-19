#!/usr/bin/env python
from .AutoID import AutoID, invert_dict

class PajekFactory(object):
    """Factory to build a Pajek file"""

    def __init__(self, edge_stream=None, node_stream=None):
        """Build a Pajek Writer.

        Args:
            edge_stream: If provided, a stream that edges will be written to instead of storing in memory.
            node_stream: If provided, a stream that nodes will be written to instead of storing in memory.
        """
        self.ids = AutoID(first_id=0)
        self.edge_stream = edge_stream
        self.node_stream = node_stream
        self.edges = list()

        self.edge_count = 0

    def add_edge(self, source, dest):
        """Add an edge from source to dest.

        Names, **not** ids should be used here.

        Args:
            source: Name of the source node
            dest: Name of the destination node

        Returns:
            None
        """

        write_source = source not in self.ids
        sid = self.ids[source]

        write_dest = dest not in self.ids
        did = self.ids[dest]

        if self.node_stream:
            if write_source:
                self.node_stream.write('%s "%s"\n' % (sid, source))
            if write_dest:
                self.node_stream.write('%s "%s"\n' % (did, dest))

        if self.edge_stream:
            self.edge_stream.write("%s %s\n" % (sid, did))
            self.edge_count += 1
        else:
            self.edges.append(sid, did)

    def make_header(self):
        vtx = "*vertices {}\n".format(len(self.ids))
        edge = "*edges {}\n".format(self.edge_count)

        return vtx, edge

    def write(self, output):
        output.write("*vertices {}\n".format(len(self.ids)))
        invert = invert_dict(self.ids)
        for n in sorted(invert.keys()):
            output.write("{} \"{}\"\n".format(n, invert[n]))

        output.write("*edges {}\n".format(len(self.edges)))
        for edge in self.edges:
            output.write("{} {}\n".format(*edge))

    def __str__(self):
        return "<PJK {} vertices, {} edges>".format(len(self.ids), self.edge_count)
