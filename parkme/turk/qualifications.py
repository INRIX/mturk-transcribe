# -*- coding: utf-8 -*-
from boto.mturk import question


class QualificationAssignment(object):
    """Helper object to simplify creating qualification assignments"""

    def __init__(self, overview, questions, expected_answers):
        """Takes a list of questions and the expected answers for the assignment.

        :param overview: The questions overview
        :type overview: dict
        :param questions: A dict containing questions
        :type questions: dict
        :param expected_answers: Dict of question id to expected answer
        :type expected_answers: dict
        """
        self.questions = questions
        self.expected_answers = expected_answers
