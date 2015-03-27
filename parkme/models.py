# -*- coding: utf-8 -*-
"""
    parkme.models
    ~~~~~~~~~~~~~
    SQLite models and their respective data gateways.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections
import sqlite3
import time


# Base entity objects
TranscribedRate = collections.namedtuple(
    'TranscribedRate', ['hit_id', 'batch_id', 'lot_id', 'rates', 'user_notes'])
ManualReview = collections.namedtuple(
    'ManualReview', ['hit_id', 'batch_id'])
CategorizationBatch = collections.namedtuple(
    'CategorizationBatch',
    ['categorization_batch_id',
     'newest_photo_timestamp',
     'created_at',
     'is_finished'])


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


class CategorizationBatchDataGateway(BaseDataGateway):
    """Gateway to table containing data categorization batches"""

    def create_table(self):
        """Create the table if it does not already exist"""
        cursor = self.dbconn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS categorization_batch
        (categorization_batch_id TEXT PRIMARY KEY,
        newest_photo_timestamp NUMERIC,
        created_at NUMERIC,
        is_finished NUMERIC)
        ''')

    def save(self, categorization_batch):
        """Insert a new categorization batch into the database.

        :param categorization_batch: A categorization batch
        :type categorization_batch: parkme.models.CategorizationBatch
        :rtype: parkme.models.CategorizationBatch
        """
        cursor = self.dbconn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO categorization_batch VALUES (?, ?, ?, ?)",
            (categorization_batch.categorization_batch_id,
             time.mktime(categorization_batch.newest_photo_timestamp.timetuple()),
             time.mktime(categorization_batch.created_at.timetuple()),
             1 if categorization_batch.is_finished else 0))
        self.dbconn.commit()
        return categorization_batch
