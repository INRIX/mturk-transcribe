# -*- coding: utf-8 -*-
"""
    parkme.utils.misc
    ~~~~~~~~~~~~~~~~~
    Miscellaneous utility methods

    Copyright (C) 2015 ParkMe, Inc. All Rights Reserved.
"""
import datetime
import itertools
import time


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


def datetime_to_microtime(dt_value):
    """Convert the given datetime value to a timestamp that includes
    microseconds.

    :param dt_value: A datetime
    :type dt_value: datetime.datetime
    :rtype: float
    """
    return (
        float(time.mktime(dt_value.timetuple())) + dt_value.microsecond / 1E6)


def microtime_to_datetime(microtime):
    """Convert the given microsecond timestamp to datetime.

    :param microtime: A microsecond timestamp
    :type microtime: float
    :rtype: datetime.datetime
    """
    return datetime.datetime.utcfromtimestamp(microtime)
