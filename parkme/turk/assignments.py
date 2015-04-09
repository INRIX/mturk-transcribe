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


def map_hits_to_assignments(hits, mturk_connection, assignment_cls):
    """Generator that converts the given HITs into Assignments.

    :param hits: An iterable of HITs
    :type hits: mturk.connection.HIT
    :param mturk_connection: The Mechanical Turk connection
    :type mturk_connection: mturk.connection.MTurkConnection
    :param assignment_cls: The assignment class
    :type assignment_cls: parkme.assignments.BaseAssignment
    :rtype: iterable of mturk.connection.Assignment
    """
    hit_ids = itertools.imap(lambda x: x.HITId, hits)
    assignments_for_hits = itertools.imap(
        mturk_connection.get_assignments, hit_ids)
    return itertools.imap(
        lambda x: assignment_cls(x), itertools.chain(*assignments_for_hits))


def get_answer_to_question(assignment, question_id):
    """Get the answer to the question with the given ID.

    :param assignment: An assignment
    :type assignment: mturk.connection.Assignment
    :param question_id: The qid associated with the question
    :type question_id: str or unicode
    :return: All answers to the given question
    :rtype: str or unicode or None
    """
    if not assignment.answers:
        return None

    for each in assignment.answers[0]:
        if each.qid == question_id:
            return each

    return None


class BaseAssignment(object):
    """Entity representing a Mechanical Turk assignment."""

    # Constants
    _EMPTY = object()

    def __init__(self, assignment):
        """Initialize assignment entity.
        
        :param assignment: An assignment
        :type assignment: boto.mturk.Assignment
        """
        self.assignment = assignment

    def __hash__(self):
        return hash(self.assignment_id)

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

    def get_answer_to_question(self, question_name):
        """Return the answer for the question with the given name.

        :param question_name: The question name
        :type question_name: str or unicode
        :rtype: str or unicode or None
        """
        answers = get_answer_to_question(self.assignment, question_name)
        return answers.fields[0] if answers and answers.fields else None

    def get_bool_answer(self, question_name):
        """Return the boolean answer to the given question. The answer is
        assumed to be an integer (0=False, 1=True).

        :param question_name: The question name
        :type question_name: str or unicode
        :return: Boolean answer or None if blank.
        :rtype: bool or None
        """
        result = self.get_answer_to_question(question_name)
        return bool(int(result)) if result else None

    def get_multichoice_answers_to_question(self, question_name):
        """Return a list containing all the answers to the question with the
        given name.

        :param question_name: The question name
        :type question_name: str or unicode
        :rtype: list
        """
        answer = self.get_answer_to_question(question_name)
        return answer.split('|') if answer else None


class ImageCategorizationAssignment(BaseAssignment):
    """Represents an image categorization assignment"""

    _CATEGORIES_QUESTION_NAME = 'Answer'
    _ASSET_ID_QUESTION_NAME = 'AssetId'
    _LOT_ID_QUESTION_NAME = 'LotId'
    _DOES_NOT_MATCH_QUESTION_NAME = 'DoesNotMatch'

    def __init__(self, assignment):
        """Initialize assignment entity.

        :param assignment: An assignment
        :type assignment: boto.mturk.Assignment
        """
        super(ImageCategorizationAssignment, self).__init__(assignment)
        self._categories = self._EMPTY
        self._asset_id = self._EMPTY
        self._lot_id = self._EMPTY
        self._does_not_match = self._EMPTY

    @property
    def categories(self):
        """Return all the categories on this assignment.

        :rtype: list or str or unicode
        """
        if self._categories is self._EMPTY:
            self._categories = self.get_multichoice_answers_to_question(
                self._CATEGORIES_QUESTION_NAME)
        return self._categories

    @property
    def asset_id(self):
        """Return the asset id associated with this assignment.

        :rtype: str or unicode
        """
        if self._asset_id is self._EMPTY:
            self._asset_id = self.get_answer_to_question(
                self._ASSET_ID_QUESTION_NAME)
        return self._asset_id

    @property
    def lot_id(self):
        """Return the lot id associated with this assignment.

        :rtype: str or unicode
        """
        if self._lot_id is self._EMPTY:
            self._lot_id = self.get_answer_to_question(
                self._LOT_ID_QUESTION_NAME)
        return self._lot_id

    @property
    def does_not_match(self):
        """Indicates whether or not this assignment does not match available
        categories.

        :rtype: bool
        """
        if self._does_not_match is self._EMPTY:
            self._does_not_match = bool(self.get_answer_to_question(
                self._DOES_NOT_MATCH_QUESTION_NAME))
        return self._does_not_match

    
class RateTranscriptionAssignment(BaseAssignment):
    """Represents a rate transcription assignment
    TODO: Break this out into a file for application-specific models
    """
    
    _RATES_QUESTION_NAME = 'Rates'
    _LOTID_QUESTION_NAME = 'LotId'
    _ASSET_ID_QUESTION_NAME = 'AssetId'
    _NOT_RATES_QUESTION_NAME = 'NotRates'

    def __init__(self, assignment):
        """Initialize assignment entity.

        :param assignment: An assignment
        :type assignment: boto.mturk.Assignment
        """
        super(RateTranscriptionAssignment, self).__init__(assignment)
        self._rates = self._EMPTY
        self._lot_id = self._EMPTY
        self._asset_id = self._EMPTY
        self._does_not_contain_rates = self._EMPTY

    @property
    def rates(self):
        """Return the rates associated with this assignment.

        :rtype: str or unicode or None
        """
        if self._rates is self._EMPTY:
            self._rates = self.get_answer_to_question(
                self._RATES_QUESTION_NAME)
            self._rates = self._rates.lower() if self._rates else None
        return self._rates

    @property
    def lot_id(self):
        """Return the lot id associated with this assignment.

        :rtype: str or unicode or None
        """
        if self._lot_id is self._EMPTY:
            self._lot_id = self.get_answer_to_question(
                self._LOTID_QUESTION_NAME)
        return self._lot_id

    @property
    def asset_id(self):
        """Return the asset id associated with this assignment.

        :rtype: str or unicode or None
        """
        if self._asset_id is self._EMPTY:
            self._asset_id = self.get_answer_to_question(
                self._ASSET_ID_QUESTION_NAME)
        return self._asset_id

    @property
    def does_not_contain_rates(self):
        """Indicates whether or not 'Image Does Not Contain Rates' checkbox
        selected.

        :rtype: bool
        """
        if self._does_not_contain_rates is self._EMPTY:
            self._does_not_contain_rates = bool(self.get_answer_to_question(
                self._NOT_RATES_QUESTION_NAME))
        return self._does_not_contain_rates
    

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

    def get_by_batch_id(self, batch_id, assignment_cls):
        """Return all the assignments in the given batch.

        :param batch_id: A batch id
        :type batch_id: int or str or unicode
        :param assignment_cls: The assignment class to encapsulate results with
        :type assignment_cls: BaseAssignment
        :rtype: iterable of boto.mturk.Assignment
        """
        all_hits = self.mturk_connection.get_all_hits()
        hits_in_batch = hits.filter_by_batch_id(all_hits, batch_id)
        return map_hits_to_assignments(
            hits_in_batch, self.mturk_connection, assignment_cls)

    def accept(self, assignment, feedback=_DEFAULT):
        """Accept the given assignment. Ignores exception thrown when
        assignment has already been accepted.

        :param assignment: An assignment
        :type assignment: Assignment
        :param feedback: Feedback message (Defaults to generic message)
        :type feedback: str or unicode or _DEFAULT
        """
        feedback = ('Great work! Automatically approved.'
                    if feedback is self._DEFAULT
                    else feedback)
        try:
            self.mturk_connection.approve_assignment(
                assignment.assignment_id, feedback=feedback)
        except connection.MTurkRequestError as mtre:
            if mtre.status != 200:
                raise mtre

    def accept_rejected(self, assignment, feedback=_DEFAULT):
        """Approve an assignment that was previously rejected.

        :param assignment: An assignment
        :type assignment: Assignment
        :param feedback: The feedback
        :type feedback: str or unicode or _DEFAULT
        """
        feedback = (
            "We have reviewed and approved this hit. We apologize for any inconvenience."
            if feedback is self._DEFAULT
            else feedback)
        try:
            self.mturk_connection.approve_rejected_assignment(
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
            "We're sorry, this HIT was not approved."
            if feedback is self._DEFAULT
            else feedback)
        try:
            self.mturk_connection.reject_assignment(
                assignment.assignment_id, feedback=feedback)
        except connection.MTurkRequestError as mtre:
            if mtre.status != 200:
                raise mtre
