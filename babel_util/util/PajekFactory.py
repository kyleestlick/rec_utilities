#!/usr/bin/env python
from .AutoID import AutoID, invert_dict

class PajekFactory(object):
    """Factory to build a Pajek file"""

    def __init__(self):
        self.ids = AutoID(first_id=1)
        self.edges = list()

    def add_edge(self, source, dest):
        """Add an edge from source to dest.

        Names, **not** ids should be used here.

        Args:
            source: Name of the source node
            dest: Name of the destination node

        Returns:
            None
        """
        self.edges.append((self.ids[source], self.ids[dest]))

    def write(self, output):
        output.write("*vertices {}\n".format(len(self.ids)))
        invert = invert_dict(self.ids)
        for n in sorted(invert.keys()):
            output.write("{} \"{}\"\n".format(n, invert[n]))

        output.write("*edges {}\n".format(len(self.edges)))
        for edge in self.edges:
            output.write("{} {}\n".format(*edge))

    def __str__(self):
        return "<PJK {} verticies, {} edges>".format(len(self.ids), len(self.edges))
