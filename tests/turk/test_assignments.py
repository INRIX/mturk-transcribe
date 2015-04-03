# -*- coding: utf-8 -*-
import unittest
import uuid

import mock

from parkme.turk import assignments


class GetAnswerToQuestionTest(unittest.TestCase):

    def setUp(self):
        super(GetAnswerToQuestionTest, self).setUp()

        self.MOCK_QUESTION_ID = str(uuid.uuid4())

        self.mock_assignment = mock.Mock()
        self.mock_question = mock.Mock()
        self.mock_question.qid = self.MOCK_QUESTION_ID
        self.mock_assignment.answers = [[self.mock_question]]

    def test_should_return_none_if_no_answers_in_assignment(self):
        """Should return None if no answers in assignment"""
        self.mock_assignment.answers = []
        self.assertIsNone(
            assignments.get_answer_to_question(self.mock_assignment, 'herp'))

    def test_should_return_none_if_question_not_in_assignment(self):
        """Should return None if question not in assignment"""
        self.assertIsNone(
            assignments.get_answer_to_question(self.mock_assignment, 'herp'))

    def test_should_return_question_for_given_question_id(self):
        """Should return question for given question id"""
        self.assertEqual(
            self.mock_question,
            assignments.get_answer_to_question(
                self.mock_assignment, self.MOCK_QUESTION_ID))


class BaseAssignmentTest(unittest.TestCase):

    def setUp(self):
        super(BaseAssignmentTest, self).setUp()

        self.MOCK_ASSIGNMENT_ID = str(uuid.uuid4())
        self.MOCK_HIT_ID = str(uuid.uuid4())
        self.MOCK_WORKER_ID = str(uuid.uuid4())

        self._mock_assignment = mock.Mock()
        self._mock_assignment.AssignmentId = self.MOCK_ASSIGNMENT_ID
        self._mock_assignment.HITId = self.MOCK_HIT_ID
        self._mock_assignment.WorkerId = self.MOCK_WORKER_ID

        self.base_assignment = assignments.BaseAssignment(
            self._mock_assignment)

    def test_should_extract_correct_assignment_id(self):
        """Should return the assignment id"""
        self.assertEqual(
            self.MOCK_ASSIGNMENT_ID, self.base_assignment.assignment_id)

    def test_should_extract_correct_hit_id(self):
        """Should return the HIT id"""
        self.assertEqual(
            self.MOCK_HIT_ID, self.base_assignment.hit_id)

    def test_should_extract_correct_worker_id(self):
        """Should return the Worker id"""
        self.assertEqual(
            self.MOCK_WORKER_ID, self.base_assignment.worker_id)
