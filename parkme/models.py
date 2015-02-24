# -*- coding: utf-8 -*-
"""
    parkme.models
    ~~~~~~~~~~~~~
    SQLite models and their respective data gateways.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections
import sqlite3


# Base entity objects
TranscribedRate = collections.namedtuple(
    'TranscribedRate', ['hit_id', 'batch_id', 'lot_id', 'rates', 'user_notes'])
ManualReview = collections.namedtuple(
    'ManualReview', ['hit_id', 'batch_id'])


class BaseDataGateway(object):
    """Represents the base class for data gateways"""

    def __init__(self, dbfile):
        """Create a new rates table instance with the given dbfile.

        :param dbfile: A database file
        :type dbfile: str or unicode
        """
        self.dbconn = sqlite3.connect(dbfile)

    def __del__(self):
        """Cleanup resources"""
        self.dbconn.close()


class TranscribedRateDataGateway(BaseDataGateway):
    """Represents the transcribed rates stored in SQLite"""

    def create_table(self):
        """Create the table if it doesn't already exist."""
        cursor = self.dbconn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcribed_rates
        (hit_id TEXT PRIMARY KEY, batch_id INTEGER, lot_id TEXT, result TEXT,
        user_notes TEXT)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS transcribed_rates_hit_id_idx
        ON transcribed_rates (hit_id);
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS transcribed_rates_batch_id_idx
        ON transcribed_rates (batch_id);
        ''')
        self.dbconn.commit()

    def save(self, rate):
        """Insert a new rate into the database

        :param rate: A transcribed rate
        :type rate: parkme.models.TranscribedRate
        :rtype: parkme.models.TranscribedRate
        """
        cursor = self.dbconn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO transcribed_rates VALUES (?, ?, ?, ?, ?)",
            (rate.hit_id, rate.batch_id,
             rate.lot_id, rate.rates, rate.user_notes))
        self.dbconn.commit()
        return rate


class ManualReviewDataGateway(BaseDataGateway):
    """Table listing all HITs in need of manual review"""

    def create_table(self):
        """Create the table if does not already exist"""
        cursor = self.dbconn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS manual_review
        (hit_id TEXT PRIMARY KEY, batch_id INTEGER)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS manual_review_batch_id_idx
        ON manual_review (batch_id);
        ''')

    def save(self, manual_review):
        """Insert a new HIT for manual review into the database.

        :param manual_review: A manual review
        :type manual_review: parkme.models.ManualReview
        :rtype: parkme.models.ManualReview
        """
        cursor = self.dbconn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO manual_review VALUES (?, ?)",
            (manual_review.hit_id, manual_review.batch_id))
        self.dbconn.commit()
        return manual_review
