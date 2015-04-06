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
    """MTurk assignment to check for photo changes"""

    HIT_LAYOUT_ID = '3NUKNKVODMJSU6BNU5ZCS9YSFU8A54'

    def __init__(self, mturk_connection):
        """Initialize photo change HIT template"""
        super(PhotoChangeTemplate, self).__init__(
            mturk_connection=mturk_connection,
            hit_layout_id=self.HIT_LAYOUT_ID,
            reward_per_assignment=0.02,
            assignments_per_hit=3,
            hit_expires_in=datetime.timedelta(days=7),
            time_per_assignment=datetime.timedelta(minutes=3),
            auto_approval_delay=datetime.timedelta(hours=8))


class PhotoChangeAssignment(assignments.BaseAssignment):
    """Model representing a photo change assignment"""

    _NEW_ASSET_ID_QUESTION_NAME = 'NewAssetId'
    _OLD_ASSET_ID_QUESTION_NAME = 'OldAssetId'
    _LOT_ID_QUESTION_NAME = 'LotId'
    _SAME_SIGN_QUESTION_NAME = 'SameSign'
    _SAME_RATES_QUESTION_NAME = 'SameRates'
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
        return bool(
            int(self.get_answer_to_question(self._SAME_SIGN_QUESTION_NAME)))

    @property
    def same_rates(self):
        """Whether or not these photos contain the same rates"""
        return bool(
            int(self.get_answer_to_question(self._SAME_RATES_QUESTION_NAME)))

    @property
    def same_prices(self):
        """Whether or not these photos contain the same prices"""
        return bool(
            int(self.get_answer_to_question(self._SAME_PRICES_QUESTION_NAME)))

