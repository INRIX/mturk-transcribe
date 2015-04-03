# -*- coding: utf-8 -*-
import unittest
import uuid

import mock

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
