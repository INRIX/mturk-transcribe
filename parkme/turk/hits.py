"""
    parkme.turk.hits
    ~~~~~~~~~~~~~~~~
    Utilities related to processing Amazon Mechanical Turk HITs.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import functools
import itertools


def in_batch(hit, batch_id):
    """Indicates whether or not the given HIT is in the batch with the given
    ID.

    :param hit: A HIT
    :type hit: mturk.connection.HIT
    :param batch_id: A batch ID
    :type batch_id: int or str or unicode
    :rtype: bool
    """
    return str(batch_id) in hit.RequesterAnnotation


def has_pending_assignments(hit):
    """Indicates whether or not the given HIT has pending assignments.

    :param hit: A HIT
    :type hit: mturk.connection.HIT
    :rtype: bool
    """
    return int(hit.NumberOfAssignmentsPending) > 0


def filter_by_batch_id(hits, batch_id):
    """Generator returning all HITs from the iterable of HITs with the given
    batch ID.

    :param hits: An iterable of HITs
    :type hits: iterable of mturk.connection.HIT
    :param batch_id: The batch ID to filter on
    :type batch_id: int or str or unicode
    :return: Generator yielding each HIT with the matching batch ID
    :rtype: iterable of mturk.connection.HIT
    """
    in_batch_with_id = functools.partial(in_batch, batch_id=batch_id)
    return itertools.ifilter(in_batch_with_id, hits)
