# -*- coding: utf-8 -*-
import unittest
import uuid

import mock

from parkme import exceptions
from parkme.turk import hits


class InBatchTest(unittest.TestCase):

    def setUp(self):
        super(InBatchTest, self).setUp()
        self.MOCK_BATCH_ID = str(uuid.uuid4())

        self.mock_hit = mock.Mock()
        self.mock_hit.RequesterAnnotation = self.MOCK_BATCH_ID

    def test_should_return_false_if_hit_is_none(self):
        """Should return False if HIT is None"""
        self.assertFalse(hits.in_batch(None, 'herp'))

    def test_should_return_false_if_hit_missing_attribute(self):
        """Should return False if HIT is missing attribute"""
        del self.mock_hit.RequesterAnnotation
        self.assertFalse(hits.in_batch(self.mock_hit, 'herp'))

    def test_should_return_false_if_batch_id_is_none(self):
        """Should return False if batch id is None"""
        self.assertFalse(hits.in_batch(self.mock_hit, None))

    def test_should_return_false_if_batch_id_not_in_annotation(self):
        """Should return False if batch id not in annotation"""
        self.assertFalse(hits.in_batch(self.mock_hit, 'herp'))

    def test_should_return_true_if_hit_matches_batch_id(self):
        """Should return True if HIT matches batch id"""
        self.assertTrue(hits.in_batch(self.mock_hit, self.MOCK_BATCH_ID))


class HasPendingAssignmentsTest(unittest.TestCase):

    def setUp(self):
        super(HasPendingAssignmentsTest, self).setUp()
        self.mock_hit = mock.Mock()
        self.mock_hit.NumberOfAssignmentsPending = 12

    def test_should_return_false_if_hit_is_none(self):
        """Should return False if HIT is None"""
        self.assertFalse(hits.has_pending_assignments(None))

    def test_should_return_false_if_hit_is_missing_property(self):
        """Should return False if HIT is missing property"""
        del self.mock_hit.NumberOfAssignmentsPending
        self.assertFalse(hits.has_pending_assignments(self.mock_hit))

    def test_should_return_false_if_hit_has_no_pending_assignments(self):
        """Should return False if HIT has no pending assignments"""
        self.mock_hit.NumberOfAssignmentsPending = 0
        self.assertFalse(hits.has_pending_assignments(self.mock_hit))

    def test_should_return_true_if_hit_has_pending_assignments(self):
        """Should return True if HIT has pending assignments"""
        self.assertTrue(hits.has_pending_assignments(self.mock_hit))


class DictToLayoutParametersTest(unittest.TestCase):

    def test_should_return_empty_layout_parameters_if_none_given(self):
        """Should return empty layout parameters if None is given"""
        result = hits.dict_to_layout_parameters(None)
        self.assertEquals({}, result.get_as_params())

    def test_should_return_empty_layout_parameters_if_empty_dict_given(self):
        """Should return empty layout paramters if empty dict given"""
        result = hits.dict_to_layout_parameters({})
        self.assertEquals({}, result.get_as_params())

    def test_should_return_expected_result(self):
        """Should return expected result"""
        fixture = {'a': 1, 'b': 2}
        result = hits.dict_to_layout_parameters(fixture)
        found = {each.name: each.value for each in result.layoutParameters}
        self.assertEquals(fixture, found)
