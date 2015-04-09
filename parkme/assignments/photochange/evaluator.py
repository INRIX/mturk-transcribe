# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.evaluator
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Interactors for evaluating photo change results from Mechanical Turk.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
import collections

from parkme.assignments import utils
from parkme.assignments.photochange import models
from parkme.turk import assignments


# The minimum percentage of matches to consider the results a consensus
CONSENSUS_THRESHOLD = 0.51


def has_consensus(num_agree, total_num_items):
    """Indicates whether or not the given numbers can be considered to have
    consensus.

    :param num_agree: The number of items that agree
    :type num_agree: int
    :param total_num_items: The total number of items
    :type total_num_items: int
    :rtype: bool
    """
    return (float(num_agree) / float(total_num_items)) >= CONSENSUS_THRESHOLD


def needs_manual_update(assignment):
    """Whether or not this assignment indicates the new photo needs a manual
    update.

    :param assignment: An assignment
    :type assignment: photochanged.models.PhotoChangedAssignment
    :rtype: bool
    """
    return any([
        not assignment.same_sign,
        assignment.new_photo_has_extra_rates,
        assignment.old_photo_has_extra_rates])


def should_automatically_update(assignment):
    """Whether or not this assignment indicates the new photo can be
    automatically updated.

    :param assignment: An assignment
    :type assignment: photochanged.models.PhotoChangedAssignment
    :rtype: bool
    """
    return not needs_manual_update(assignment) and assignment.same_prices


def should_send_for_rate_pricing(assignment):
    """Whether or not this assignment indicates the new photo should be sent
    for rate pricing.

    :param assignment: An assignment
    :type assignment: photochanged.models.PhotoChangedAssignment
    :rtype: bool
    """
    return not needs_manual_update(assignment) and not assignment.same_prices


def get_consensus_result(list_of_assignments):
    """Get the consensus result for a list of assignments if available.

    :param list_of_assignments: A list of assignments
    :type list_of_assignments: list
    :return: List of answers to each question (list index is question number)
    :rtype: list or None
    """
    if not list_of_assignments:
        return None

    results = []
    for each in list_of_assignments:
        results.append((needs_manual_update(each),
                        should_automatically_update(each),
                        should_send_for_rate_pricing(each)))

    counted = collections.Counter(results)
    most_common = counted.most_common(1)

    most_common, num_agree_on_most_common = most_common[0]
    if has_consensus(num_agree_on_most_common, len(list_of_assignments)):
        return most_common

    return None


def has_consensus_for_assignments(list_of_assignments):
    """Indicates whether or not there is consensus on a given result for a set
    of assignments.

    :param list_of_assignments: A list of assignments
    :type list_of_assignments: list
    :rtype: bool
    """
    return get_consensus_result(list_of_assignments) is not None


def get_all_photo_change_assignments(mturk_connection, batch_id):
    """Get all photo change assignment results from Mechanical Turk.

    :param mturk_connection: A mechanical turk connection
    :type mturk_connection: boto.mturk.connection.Connection
    :param batch_id: A batch id
    :type batch_id: str or unicode
    :rtype: iterable
    """
    assignment_gateway = assignments.AssignmentGateway(mturk_connection)
    return assignment_gateway.get_by_batch_id(
        batch_id, models.PhotoChangeAssignment)


def evaluate_all_photo_change_assignments(mturk_connection, batch_id):
    """Evaluate all of the photo change assignments in the given batch.

    :param mturk_connection: A mechanical turk connection
    :type mturk_connection: boto.mturk.connection.Connection
    :param batch_id: A batch id
    :type batch_id: str or unicode
    """
    all_assignments = get_all_photo_change_assignments(
        mturk_connection, batch_id)
    asset_id_to_assignments = utils.group_by_attribute(
        all_assignments, 'new_asset_id')
    for asset_id, all_assignments in asset_id_to_assignments.iteritems():
        print asset_id, '->', len(all_assignments)
        for each in all_assignments:
            print (
                each.same_sign,
                each.new_photo_has_extra_rates,
                each.old_photo_has_extra_rates,
                each.same_prices)
        if not has_consensus_for_assignments(all_assignments):
            print 'No consensus result.'
            print
            continue
        result = get_consensus_result(all_assignments)
        print result
        print
