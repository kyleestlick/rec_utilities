#!/usr/bin/env python
import logging
import networkx as nx

class TreeFile(object):
    """Handling functions for tree files, as produced by the EigenFactor
    Recommends algorithm.

    The file should be a plain text file with the following format:
    <cluster_id> <score> <paper_id>
    1:1:1:1 0.000021 "123456"
    1:1:1:2 0.023122 "8675309"
    """
    def __init__(self, stream, delimiter=' ', comment='#'):
        """Initializes a TreeFile for reading.

        Args:
            source: An iterable providing a line of input for each iteration.
            delimiter: Character tree file is delimited by.
            comment: Lines starting with this character should be skipped
        """
        self.delimiter = delimiter
        self.stream = stream
        self.comment = comment

    def to_dict(self, on_collide="error", transform=None):
        """Converts a TreeFile to a dictionary. Consumes all of stream.

        This might consume all available memory if the input stream is large.

        Args:
            on_collide: If a value already exists in the dictionary what should
                happen. Options are:
                    error - raise an exception
                    warn - log a warning
                    info - log an info
            transform: If provided a function that will be applied to the
                values prior to storing them. This function should accept
                a tuple of (cluster_id, score, paper_id):
                ("1:2:3:4", 0.12345, "A paper title"). If this function returns
                None the paper will not be stored.

        Returns:
            Returns a dictionary using paper_id as the key and
            (cluster_id, score, paper_id) as the value.

        Raises:
            KeyError: If on_collide="error" this signals a duplicate paper_id
            in the tree file.
        """
        results = dict()
        for cid, score, pid in self:
            if pid in results:
                if on_collide == "error":
                    raise KeyError("Duplicate paper_id: {0}".format(pid))
                elif on_collide == "warn":
                    logging.warn("Duplicate paper_id: {0}".format(pid))
                elif on_collide == "info":
                    logging.info("Duplicate paper_id: {0}".format(pid))

            if transform:
                value = transform((cid, score, pid))
                if value is not None:
                    results[pid] = value
            else:
                results[pid] = (cid, score)

        return results

    def to_graph(self):
        """Converts a TreeFile to a networkx DiGraph. Consumes all of the stream.

        This might consume all available memory if the input stream is large.

        Returns:
            A networkx DiGraph. It will likely contain things.
        """
        G = nx.DiGraph()

        for cid, score, pid in self:
            pid = "paper-" + str(pid)
            edges = list()
            clusters = cid.split(':')

            clusters.pop() #Remove final cluster order

            # This means we found an orphan
            if len(clusters) == 0:
                G.add_node(pid, {"score":score})
                break

            i = ":".join(clusters)
            edges.append((i, pid))
            clusters.pop()

            while len(clusters):
                j = ":".join(clusters)
                edges.append((j, i))
                i = j
                clusters.pop()

            G.add_edges_from(edges)
            G.node[pid]['score'] = score

        return G

    def __iter__(self):
        self._iter = iter(self.stream)
        return self

    def next(self):
        line = self._iter.next()
        while self.comment and line.startswith(self.comment):
            line = self._iter.next()
        return self.parse_line(line)

    def parse_line(self, line):
        try:
            cid, score, pid = line.split(self.delimiter)
            pid = pid.strip().strip('"')
            return cid, float(score), pid
        except ValueError:
            print line
            raise
        except AttributeError:
            print line
            raise
