# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.evaluator
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Interactors for evaluating photo change results from Mechanical Turk.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
from parkme.assignments import utils
from parkme.assignments.photochange import models
from parkme.turk import assignments


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
