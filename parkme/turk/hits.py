"""
    parkme.turk.hits
    ~~~~~~~~~~~~~~~~
    Utilities related to processing Amazon Mechanical Turk HITs.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import datetime
import functools
import itertools

from boto.mturk import layoutparam
from boto.mturk import price


def in_batch(hit, batch_id):
    """Indicates whether or not the given HIT is in the batch with the given
    ID.

    :param hit: A HIT
    :type hit: mturk.connection.HIT
    :param batch_id: A batch ID
    :type batch_id: int or str or unicode
    :rtype: bool
    """
    try:
        return str(batch_id) in hit.RequesterAnnotation
    except AttributeError:
        return False


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


def dict_to_layout_parameters(dict_to_convert):
    """Mechanical turk layout parameters are really just an overly formalized
    dictionary.

    :param dict_to_convert: A dictionary to convert
    :type dict_to_convert: dict
    :rtype: boto.mturk.layoutparam.LayoutParameters
    """
    params = []
    for key, value in dict_to_convert.iteritems():
        params.append(layoutparam.LayoutParameter(key, value))
    return layoutparam.LayoutParameters(params)


class HITTemplate(object):
    """Represents a Mechanical Turk HIT template that can be used to create
    HITs."""

    def __init__(self,
                 mturk_connection,
                 hit_layout_id,
                 reward_per_assignment,
                 assignments_per_hit=1,
                 hit_expires_in=datetime.timedelta(days=7),
                 time_per_assignment=datetime.timedelta(hours=1),
                 auto_approval_delay=datetime.timedelta(hours=8),
                 annotation=None):
        """Initialize the HIT template with some basic information.

        :param mturk_connection: The mechanical turk connection to use
        :type mturk_connection: boto.mturk.connection.MTurkConnection
        :param hit_layout_id: The HIT layout id
        :type hit_layout_id: str or unicode
        :param reward_per_assignment: The reward for each assignment (eg. 0.00)
        :type reward_per_assignment: float
        :param assignments_per_hit: The number of assignments for each HIT
        :type assignments_per_hit: int
        :param hit_expires_in: The time delay before the hit expires
        :type hit_expires_in: datetime.timedelta
        :param time_per_assignment: How much time Turkers have to complete
        :type time_per_assignment: datetime.timedelta
        :param auto_approval_delay: The delay before assignment is auto-approved
        :type auto_approval_delay: datetime.timedelta
        """
        self.mturk_connection = mturk_connection
        self.hit_layout_id = hit_layout_id
        self.reward_per_assignment = reward_per_assignment
        self.assignments_per_hit = assignments_per_hit
        self.hit_expires_in = hit_expires_in
        self.time_per_assignment = time_per_assignment
        self.auto_approval_delay = auto_approval_delay

    def create_hit(self, params, batch_id=None):
        """Create a new HIT using this template with the given params.

        :param params: A dictionary of template params
        :type params: dict
        :param batch_id: (Optional) Batch ID to be associated with HIT
        :type batch_id: str or unicode or None
        :rtype: boto.mturk.HIT
        """
        boto_params = dict_to_layout_parameters(params)
        reward_price = price.Price(
            amount=self.reward_per_assignment, currency_code='USD')
        return self.mturk_connection.create_hit(
            hit_layout=self.hit_layout_id,
            reward=reward_price,
            max_assignments=self.assignments_per_hit,
            lifetime=self.hit_expires_in,
            duration=self.time_per_assignment,
            approval_delay=self.auto_approval_delay,
            annotation=batch_id,
            layout_params=boto_params)
