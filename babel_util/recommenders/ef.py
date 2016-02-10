#!/usr/bin/env python
"""Implements classic and expert EigenFactor recommendations"""
import itertools
from parsers.tree import TreeFile


class Recommendation(object):
    rec_type = None
    __slots__ = ("target_pid", "pid", "score")

    def __init__(self, target_pid, pid, score):
        self.target_pid = target_pid
        self.score = score
        self.pid = pid

    def to_flat(self):
        return "%s %s %s\n" % (self.target_pid, self.pid, self.score)

    def __str__(self):
        return "%s %s %s %s" % (self.target_pid, self.rec_type, self.pid, self.score)

    def __repr__(self):
        return "<%s, %s, %s, %s>" % (self.target_pid, self.rec_type, self.pid, self.score)


class ClassicRec(Recommendation):
    rec_type = "classic"


class ExpertRec(Recommendation):
    rec_type = "expert"




def require_local(e):
    return e.local is not None


def get_local(e):
    return e.local


def get_parent(e):
    return e.parent


def get_score(e):
    return e.score


def make_expert_rec(stream, rec_limit=10):
    """Given a stream of TreeRecord, generate ExpertRecs.

    Args:
        stream: A stream of TreeRecords, sorted by cluster_id (1:1:1, 1:1:2)
        rec_limit: The number of recommendations to generate per-paper. Default 10.
        
    Returns:
        A generator returning a sorted list of ExpertRecs."""
    filtered_stream = filter(require_local, stream)  # TODO: I haven't convinced myself that this isn't necessary
    expert_stream = itertools.groupby(stream, get_local)

    for (_, stream) in expert_stream:
        # These are already sorted
        candidates = [e for e in stream]
        for paper in candidates:
            # A paper shouldn't recommend itself
            topn = list(filter(lambda e: e.pid != paper.pid, candidates[:rec_limit+1]))
            yield list(map(lambda r: ExpertRec(paper.pid, r.pid, r.score), topn[:rec_limit]))


def make_classic_recs(stream, rec_limit=10):
    """Given a stream of TreeRecord, generate ClassicRecs.

    Args:
        stream: A stream of TreeRecords, sorted by cluster_id (1:1:1, 1:1:2)
        rec_limit: The number of recommendations to generate per-paper. Default 10.
        
    Returns:
        A generator returning a sorted list of ClassicRecs."""
    filtered_stream = filter(require_local, stream)
    classic_stream = itertools.groupby(stream, get_parent)

    for (_, stream) in classic_stream:
        candidates = [e for e in stream]
        candidates.sort(key=get_score, reverse=True)
        for paper in candidates:
            # A paper shouldn't recommend itself
            topn = list(filter(lambda e: e.pid != paper.pid, candidates[:rec_limit+1]))
            yield list(map(lambda r: ClassicRec(paper.pid, r.pid, r.score), topn[:rec_limit]))

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('classic', type=argparse.FileType('w'))
    parser.add_argument('expert', type=argparse.FileType('w'))
    parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
    parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
    args = parser.parse_args()

    tf = TreeFile(args.infile)
    for recs in make_expert_rec(tf, args.limit, args.toint):
        score = len(recs)
        for rec in recs:
            if args.toint:
                rec.score = score
                score -= 1
            args.expert.write(rec.to_flat())

    args.infile.seek(0)
    tf = TreeFile(args.infile)
    for recs in make_classic_recs(tf, args.limit):
        score = len(recs)
        for rec in recs:
            if args.toint:
                rec.score = score
                score -= 1
            args.classic.write(rec.to_flat())
