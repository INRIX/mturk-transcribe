# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.models
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Models for yes/no photo change assignment.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
import collections
import datetime

from parkme.turk import assignments
from parkme.turk import hits


ComparableAsset = collections.namedtuple(
    'ComparableAsset',
    ['asset_id', 'lot_id', 'str_bucket', 'str_path', 'dt_photo'])


class PhotoChangeTemplate(hits.HITTemplate):
    """MTurk HIT to check for photo changes"""

    HIT_LAYOUT_ID = '3KSK2GNGRZO4QEWXMSLBUYNCET8H3X'
    PRICE_PER_ASSIGNMENT_DOLLARS = 0.05
    ASSIGNMENTS_PER_HIT = 3

    def __init__(self, mturk_connection):
        """Initialize photo change HIT template"""
        super(PhotoChangeTemplate, self).__init__(
            mturk_connection=mturk_connection,
            title='New Photo vs Old Photo',
            description='You will be shown 2 photos and asked various questions about them.',
            hit_layout_id=self.HIT_LAYOUT_ID,
            reward_per_assignment=self.PRICE_PER_ASSIGNMENT_DOLLARS,
            assignments_per_hit=self.ASSIGNMENTS_PER_HIT,
            hit_expires_in=datetime.timedelta(days=7),
            time_per_assignment=datetime.timedelta(minutes=10),
            auto_approval_delay=datetime.timedelta(hours=8))


class PhotoChangeAssignment(assignments.BaseAssignment):
    """Model representing a photo change assignment"""

    _NEW_ASSET_ID_QUESTION_NAME = 'NewAssetId'
    _OLD_ASSET_ID_QUESTION_NAME = 'OldAssetId'
    _LOT_ID_QUESTION_NAME = 'LotId'
    _SAME_SIGN_QUESTION_NAME = 'SameSign'
    _NEW_PHOTO_HAS_EXTRA_RATES_QUESTION_NAME = 'NewPhotoHasExtraRates'
    _OLD_PHOTO_HAS_EXTRA_RATES_QUESTION_NAME = 'OldPhotoHasExtraRates'
    _SAME_PRICES_QUESTION_NAME = 'SamePrices'

    def __init__(self, assignment):
        """Initialize assignment

        :param assignment: An assignment
        :type assignment: boto.mturk.assignment
        """
        super(PhotoChangeAssignment, self).__init__(assignment)
        self._new_asset_id = self._EMPTY
        self._old_asset_id = self._EMPTY
        self._lot_id = self._EMPTY
        self._same_sign = self._EMPTY
        self._same_rates = self._EMPTY
        self._same_prices = self._EMPTY

    @property
    def new_asset_id(self):
        """The asset ID of the new photo"""
        return self.get_answer_to_question(
            self._NEW_ASSET_ID_QUESTION_NAME)

    @property
    def old_asset_id(self):
        """The asset ID of the old photo"""
        return self.get_answer_to_question(
            self._OLD_ASSET_ID_QUESTION_NAME)

    @property
    def lot_id(self):
        """The lot ID"""
        return self.get_answer_to_question(
            self._LOT_ID_QUESTION_NAME)

    @property
    def same_sign(self):
        """Whether or not the images are of the same sign"""
        return self.get_bool_answer(self._SAME_SIGN_QUESTION_NAME)

    @property
    def new_photo_has_extra_rates(self):
        """Whether or not the new photo has extra rates"""
        return self.get_bool_answer(
            self._NEW_PHOTO_HAS_EXTRA_RATES_QUESTION_NAME)

    @property
    def old_photo_has_extra_rates(self):
        """Whether or not the old photo has extra rates"""
        return self.get_bool_answer(
            self._OLD_PHOTO_HAS_EXTRA_RATES_QUESTION_NAME)

    @property
    def same_prices(self):
        """Whether or not the photos share the same prices"""
        return self.get_bool_answer(
            self._SAME_PRICES_QUESTION_NAME)
