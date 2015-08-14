from collections import defaultdict

def parse_stream(stream, delimiter=' '):
    """Given a stream of cocitations produce a dictionary of results.

    Note that the cociations must be sorted in the stream. Or sort them after.

    Input stream should look like:
    #<source> <target> <score>
    2092356 2092356 8
    2092356 1994676 3
    2092356 1945751 3

    Args:
        stream: An iterable containing a source, target and score, seperated by delim.
        delimiter: The seperater for each field. Defaults to a space ' '.

    Returns:
        A dictionary whose keys are source papers and values are list of tuples (target, sscore).
        Note that if the rankings in stream wasn't sorted these lists won't be in order.
    
    """
    result = defaultdict(list)
    for line in stream:
        source, target, rank = map(str.strip, line.split(delimiter))
        if source != target:
            result[source].append((target, rank))        
    
    return dict(result)
