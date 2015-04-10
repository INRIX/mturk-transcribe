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


def get_consensus_result_among_items(items):
    """Get the consensus result among a set of boolean items.

    :param items: A list of items
    :type items: list
    :rtype: bool
    """
    true_items = [each for each in items if each]
    return has_consensus(len(true_items), len(items))


def should_reject(assignment):
    """Indicates whether or not the given assignment should be rejected.

    :param assignment: An assignment
    :type assignment: photochanged.models.PhotoChangedAssignment
    :rtype: bool
    """
    all_blank = all([
        assignment.same_sign is None,
        assignment.new_photo_has_extra_rates is None,
        assignment.old_photo_has_extra_rates is None,
        assignment.same_prices is None])
    questions_2_through_4_blank = all([
        assignment.new_photo_has_extra_rates is None,
        assignment.old_photo_has_extra_rates is None,
        assignment.same_prices is None])
    if all_blank:
        return True
    if not assignment.same_sign and not questions_2_through_4_blank:
        return True
    if (assignment.same_sign and
            (assignment.new_photo_has_extra_rates or
             assignment.old_photo_has_extra_rates) and
            assignment.same_prices is not None):
        return True
    if (assignment.same_sign and
            not assignment.new_photo_has_extra_rates and
            not assignment.old_photo_has_extra_rates and
            assignment.same_prices is None):
        return True
    return False


def get_consensus_result(list_of_assignments):
    """Get the consensus result for a list of assignments if available.

    :param list_of_assignments: A list of assignments
    :type list_of_assignments: list
    :return: List of answers to each question (list index is question number)
    :rtype: list or None
    """
    if not list_of_assignments:
        return None

    same_sign = get_consensus_result_among_items(
        [each.same_sign for each in list_of_assignments])
    new_photo_has_extra_rates = get_consensus_result_among_items(
        [each.new_photo_has_extra_rates for each in list_of_assignments])
    old_photo_has_extra_rates = get_consensus_result_among_items(
        [each.old_photo_has_extra_rates for each in list_of_assignments])
    same_prices = get_consensus_result_among_items(
        [each.same_prices for each in list_of_assignments])

    return (same_sign,
            new_photo_has_extra_rates,
            old_photo_has_extra_rates,
            same_prices)


def has_consensus_for_assignments(list_of_assignments):
    """Indicates whether or not there is consensus on a given result for a set
    of assignments.

    :param list_of_assignments: A list of assignments
    :type list_of_assignments: list
    :rtype: bool
    """
    return get_consensus_result(list_of_assignments) is not None


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
    new_asset_id_to_assignments = utils.group_by_attribute(
        all_assignments, 'new_asset_id')
    for new_asset_id, new_assns in new_asset_id_to_assignments.iteritems():
        # Each old asset will have 3 assignments. We want to get a consensus
        # from those.
        old_asset_id_to_assignments = utils.group_by_attribute(
            new_assns, 'old_asset_id')
        print '[{}]'.format(new_asset_id)

        same_sign_results = []
        for old_asset_id, old_assns in old_asset_id_to_assignments.iteritems():
            print old_asset_id, new_asset_id, '->', len(old_assns)

            for each in old_assns:
                print (
                    each.same_sign,
                    each.new_photo_has_extra_rates,
                    each.old_photo_has_extra_rates,
                    each.same_prices)

            if not has_consensus_for_assignments(old_assns):
                print
                print 'NO CONSENSUS.'
                print
                continue

            result = get_consensus_result(old_assns)
            print
            print 'CONSENSUS RESULTS', result
            print

            if result[0]:
                same_sign_results.append(result)

        if same_sign_results:
            print
            print 'SAME SIGN RESULTS {}'.format(new_asset_id)
            for each in same_sign_results:
                print each
