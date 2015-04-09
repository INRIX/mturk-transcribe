# -*- coding: utf-8 -*-
import unittest

import mock

from parkme.assignments.photochange import evaluator


class BaseAssignmentTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseAssignmentTestCase, self).setUp()
        self.mock_assignment = mock.Mock()


class NeedsManualUpdateTest(BaseAssignmentTestCase):
    
    def test_should_return_true_if_not_same_sign(self):
        """Should return True if sign is not the same."""
        self.mock_assignment.same_sign = False
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.assertTrue(
            evaluator.needs_manual_update(self.mock_assignment))

    def test_should_return_true_if_same_sign_but_new_has_more_rates(self):
        """Should return True if same sign, but new photo has more rates"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = True
        self.mock_assignment.old_photo_has_extra_rates = False
        self.assertTrue(
            evaluator.needs_manual_update(self.mock_assignment))

    def test_should_return_true_if_same_sign_but_old_has_more_rates(self):
        """Should return True if same sign, but old photo has more rates"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = True
        self.assertTrue(
            evaluator.needs_manual_update(self.mock_assignment))

    def test_should_return_false_if_same_sign_and_no_rates_changes(self):
        """Should return False if same sign, but no rates changes"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.assertFalse(
            evaluator.needs_manual_update(self.mock_assignment))

    def test_should_return_false_even_if_prices_have_changed(self):
        """Should return False if same sign, no rate changes, and prices changed"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.mock_assignment.same_prices = False
        self.assertFalse(
            evaluator.needs_manual_update(self.mock_assignment))

    def test_should_return_false_even_if_prices_have_not_changed(self):
        """Should return False even in the case where prices have not changed"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.mock_assignment.same_prices = True
        self.assertFalse(
            evaluator.needs_manual_update(self.mock_assignment))


class ShouldAutomaticallyUpdateTest(BaseAssignmentTestCase):

    def test_should_return_false_not_same_sign(self):
        """Should return False if not same sign"""
        self.mock_assignment.same_sign = False
        self.assertFalse(
            evaluator.should_automatically_update(self.mock_assignment))

    def test_should_return_false_if_extra_fees_in_new_photo(self):
        """Should return False if extra fees in new photo"""
        self.mock_assignment.same_sign = False
        self.mock_assignment.new_photo_has_extra_rates = True
        self.assertFalse(
            evaluator.should_automatically_update(self.mock_assignment))

    def test_should_return_false_if_extra_fees_in_old_photo(self):
        """Should return False if extra fees in old photo"""
        self.mock_assignment.same_sign = False
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = True
        self.assertFalse(
            evaluator.should_automatically_update(self.mock_assignment))

    def test_should_return_true_if_same_sign_same_rates_same_prices(self):
        """Should return True if same sign, same rates, same prices"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.mock_assignment.same_prices = True
        self.assertTrue(
            evaluator.should_automatically_update(self.mock_assignment))


class ShouldSendForRatePricingTest(BaseAssignmentTestCase):
    
    def test_should_return_false_if_not_same_sign(self):
        """Should return False if not same sign"""
        self.mock_assignment.same_sign = False
        self.assertFalse(
            evaluator.should_send_for_rate_pricing(self.mock_assignment))

    def test_should_return_false_if_new_rates_changed(self):
        """Should return False if new rates changed"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = True
        self.assertFalse(
            evaluator.should_send_for_rate_pricing(self.mock_assignment))

    def test_should_return_false_if_old_rates_changed(self):
        """Shoul dreturn False if old rates changed"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = True
        self.assertFalse(
            evaluator.should_send_for_rate_pricing(self.mock_assignment))

    def test_should_return_false_if_same_price(self):
        """Should return False if same price"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.mock_assignment.same_prices = True
        self.assertFalse(
            evaluator.should_send_for_rate_pricing(self.mock_assignment))

    def test_should_return_true_if_not_same_price(self):
        """Should return True if not same price"""
        self.mock_assignment.same_sign = True
        self.mock_assignment.new_photo_has_extra_rates = False
        self.mock_assignment.old_photo_has_extra_rates = False
        self.mock_assignment.same_prices = False
        self.assertTrue(
            evaluator.should_send_for_rate_pricing(self.mock_assignment))
