# -*- coding: utf-8 -*-
import unittest

from parkme.utils import misc


class PairwiseTest(unittest.TestCase):

    def test_should_return_empty_list_if_given_empty_list(self):
        """Should return empty list if empty"""
        self.assertEqual([], list(misc.pairwise([])))

    def test_should_return_empty_list_if_only_one_item(self):
        """Should return empty list if only one item"""
        self.assertEqual([], list(misc.pairwise([1])))

    def test_should_return_only_one_pair_if_two_items(self):
        """Should return only one pair if two items"""
        self.assertEqual([(1, 2)], list(misc.pairwise([1, 2])))

    def test_should_return_all_pairs_of_items(self):
        """Should return all pairs of items given"""
        self.assertEqual([(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)],
                         list(misc.pairwise(range(1, 7))))
