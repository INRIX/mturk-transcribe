# -*- coding: utf-8 -*-
import unittest
import uuid

import mock

from parkme.turk import assignments


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
