"""
    parkme.utils.misc
    ~~~~~~~~~~~~~~~~~
    Miscellaneous utility methods

    Copyright (C) 2015 ParkMe, Inc. All Rights Reserved.
"""
import itertools


def pairwise(iterable):
    """Return all possible pairs of items from the given iterable.

    Lifted from https://docs.python.org/2/library/itertools.html
    s -> (s0, s1), (s1, s2), (s2, s3)

    :param iterable: An iterable
    :type iterable: iterable
    :rtype: iterable
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)
