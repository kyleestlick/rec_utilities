#!/usr/bin/env python
from .AutoID import AutoID
from shutil import copyfileobj
from tempfile import TemporaryFile


class PajekFactory(object):
    """Factory to build a Pajek file"""

    def __init__(self, edge_stream=None, node_stream=None, temp_dir=None):
        """Build a Pajek Writer.

        Args:
            edge_stream: If provided, a stream that edges will be written to. Otherwise a temporary file is used.
            node_stream: If provided, a stream that nodes will be written to. Otherwise a temporary file is used.
            temp_dir: If provided, the directory to write temporary files to.
        """
        self.ids = AutoID(first_id=1)
        self.edge_stream = edge_stream
        self.node_stream = node_stream

        if not self.edge_stream:
            self.edge_stream = TemporaryFile("w+", dir=temp_dir)
        if not self.node_stream:
            self.node_stream = TemporaryFile("w+", dir=temp_dir)

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

        #TODO: Make a version of AutoID that just writes out nodes for us
        if write_source:
            self.node_stream.write('%s "%s"\n' % (sid, source))
        if write_dest:
            self.node_stream.write('%s "%s"\n' % (did, dest))

        self.edge_stream.write("%s %s\n" % (sid, did))
        self.edge_count += 1

    def write(self, output):
        """Write pajek file to output"""
        self.edge_stream.seek(0)
        self.node_stream.seek(0)

        output.write("*vertices {}\n".format(len(self.ids)))
        copyfileobj(self.node_stream, output)
        output.write("*edges {}\n".format(self.edge_count))
        copyfileobj(self.edge_stream, output)

    def __str__(self):
        return "<PJK {} vertices, {} edges>".format(len(self.ids), self.edge_count)
