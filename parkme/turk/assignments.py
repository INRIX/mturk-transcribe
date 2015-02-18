"""
    parkme.turk.assignments
    ~~~~~~~~~~~~~~~~~~~~~~~
    Utilities related to processing Amazon Mechanical Turk assignments.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import itertools


def map_hits_to_assignments(hits, mturk_connection):
    """Generator that converts the given HITs into Assignments.

    :param hits: An iterable of HITs
    :type hits: mturk.connection.HIT
    :param mturk_connection: The Mechanical Turk connection
    :type mturk_connection: mturk.connection.MTurkConnection
    :rtype: iterable of mturk.connection.Assignment
    """
    hit_ids = itertools.imap(lambda x: x.HITId, hits)
    assignments_for_hits = itertools.imap(
        mturk_connection.get_assignments, hit_ids)
    return itertools.chain(*assignments_for_hits)


def get_answer_to_question(assignment, question_id):
    """Get the answer to the question with the given ID.

    :param assignment: An assignment
    :type assignment: mturk.connection.Assignment
    :param question_id: The qid associated with the question
    :type question_id: str or unicode
    :return: All answers to the given question
    :rtype: str or unicode or None
    """
    for each in assignment.answers[0]:
        if each.qid == question_id:
            return each
