#!/usr/bin/env python
import logging


class TreeFile(object):
    """Handling functions for tree files, as produced by Infomap.

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
                    logging.warning("Duplicate paper_id: {0}".format(pid))
                elif on_collide == "info":
                    logging.info("Duplicate paper_id: {0}".format(pid))

            if transform:
                value = transform((cid, score, pid))
                if value is not None:
                    results[pid] = value
            else:
                results[pid] = (cid, score)

        return results

    def __iter__(self):
        self._iter = iter(self.stream)
        return self

    def __next__(self):
        line = next(self._iter)
        while self.comment and line.startswith(self.comment):
            line = next(self._iter)
        return self.parse_line(line)

    def parse_line(self, line):
        try:
            v = line.split(self.delimiter)
            v[2] = v[2].strip().strip('"')
            return TreeRecord(v[0], v[2], v[1])
        except ValueError:
            print(line)
            raise
        except AttributeError:
            print(line)
            raise
        except IndexError:
            print(line)
            raise


class TreeRecord(object):
    __slots__ = ("pid", "local", "score", "parent")

    def __init__(self, cluster, pid, score, delimiter=':'):
        if not pid or pid == "":
            raise ValueError("Invalid pid")

        if score is None:
            raise ValueError("Invalid score")

        if cluster is None:
            raise ValueError("Invalid cluster")

        cluster = cluster.split(delimiter)

        try:
            cluster.pop()  # Remove local order
            self.local = delimiter.join(cluster)
            if not self.local:
                raise ValueError("Invalid cluster")
        except IndexError:
            self.local = None
        try:
            cluster.pop()  # Remove local-cluster id
            if len(cluster):
                self.parent = delimiter.join(cluster)
            else:
                self.parent = None
        except IndexError:
            self.parent = None

        score = float(score)
        if score == 0:
            score = -1.0  #Dynamo doesn't understand inf

        # Strip whitespace and any quotes
        self.pid = pid.strip().strip('"')
        self.score = score

    def __eq__(self, other):
        if not isinstance(other, TreeRecord):
            return False

        return self.pid == other.pid and self.local == other.local and self.parent == other.parent

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "<TreeRecord: %s %s %s>" % (self.local, self.pid, self.score)

    def __repr__(self):
        return "<TreeRecord: %s %s %s>" % (self.local, self.pid, self.score)
