# -*- coding: utf-8 -*-
# pylint: disable=W0142
"""
    parkme.turk.assignments
    ~~~~~~~~~~~~~~~~~~~~~~~
    Utilities related to processing Amazon Mechanical Turk assignments.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import itertools

from boto.mturk import connection

from parkme.turk import hits


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
    return itertools.imap(
        lambda x: Assignment(x), itertools.chain(*assignments_for_hits))


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


class Assignment(object):
    """Entity representing a Mechanical Turk assignment."""

    _EMPTY = object()

    def __init__(self, assignment):
        """Initialize assignment entity.

        :param assignment: An assignment
        :type assignment: boto.mturk.Assignment
        """
        self.assignment = assignment
        self._rates = self._EMPTY

    @property
    def rates(self):
        """Return the rates associated with this assignment.

        :rtype: str or unicode or None
        """
        if self._rates is self._EMPTY:
            rate_answers = get_answer_to_question(self.assignment, 'Rates')
            self._rates = (rate_answers.fields[0].lower()
                           if rate_answers and rate_answers.fields
                           else None)
        return self._rates

    @property
    def assignment_id(self):
        """Assignment ID for this assignment"""
        return self.assignment.AssignmentId

    @property
    def hit_id(self):
        """HIT ID for this assignment"""
        return self.assignment.HITId

    @property
    def worker_id(self):
        """Worker ID for this assignment"""
        return self.assignment.WorkerId


class AssignmentGateway(object):
    """Gateway using a MTurk connection to get at assignments."""

    _DEFAULT = object()

    def __init__(self, mturk_connection):
        """Initialize a new gateway.

        :param mturk_connection: A Mechanical Turk connection
        :type mturk_connection: boto.mturk.Connection
        """
        self.mturk_connection = mturk_connection

    @classmethod
    def get(cls, mturk_connection):
        """Simple MVC-like method for returning a new gateway.

        :param mturk_connection: A Mechanical Turk connection
        :type mturk_connection: boto.mturk.Connection
        """
        return cls(mturk_connection)

    def get_by_batch_id(self, batch_id):
        """Return all the assignments in the given batch.

        :param batch_id: A batch id
        :type batch_id: int or str or unicode
        :rtype: iterable of boto.mturk.Assignment
        """
        all_hits = list(self.mturk_connection.get_all_hits())
        hits_in_batch = hits.filter_by_batch_id(all_hits, batch_id)
        return map_hits_to_assignments(hits_in_batch, self.mturk_connection)

    def accept(self, assignment, feedback=_DEFAULT):
        """Accept the given assignment. Ignores exception thrown when
        assignment has already been accepted.

        :param assignment: An assignment
        :type assignment: Assignment
        :param feedback: Feedback message (Defaults to generic message)
        :type feedback: str or unicode or _DEFAULT
        """
        feedback = ('Great work! Automatically approved by rate parser.'
                    if feedback is self._DEFAULT
                    else feedback)
        try:
            self.mturk_connection.accept_assignment(
                assignment.assignment_id, feedback=feedback)
        except connection.MTurkRequestError as mtre:
            if mtre.status != 200:
                raise mtre

    def reject(self, assignment, feedback=_DEFAULT):
        """Reject the given assignment. Ignores exception thrown when
        assignment has already been rejected.

        :param assignment: An assignment
        :type assignment: Assignment
        :param feedback: Feedback message (Defaults to generic message)
        :type feedback: str or unicode or _DEFAULT
        """
        feedback = (
            "We're sorry, this HIT was flagged as malformed by our rate parser"
            if feedback is self_DEFAULT
            else feedback)
        try:
            self.mturk_connection.reject_assignment(
                assignment.assignment_id, feedback=feedback)
        except connection.MTurkRequestError as mtre:
            if mtre.status != 200:
                raise mtre
