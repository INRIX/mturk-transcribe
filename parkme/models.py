# -*- coding: utf-8 -*-
"""
    parkme.models
    ~~~~~~~~~~~~~
    SQLite models and their respective data gateways.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections
import sqlite3

import pytz

from parkme.utils import misc


# Base entity objects
CategorizationBatch = collections.namedtuple(
    'CategorizationBatch',
    ['categorization_batch_id',
     'newest_photo_timestamp',
     'created_at',
     'num_photos',
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
        num_photos INTEGER,
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
            """
            INSERT OR REPLACE INTO categorization_batch VALUES (?, ?, ?, ?, ?)
            """,
            (categorization_batch.categorization_batch_id,
             misc.datetime_to_microtime(
                 categorization_batch.newest_photo_timestamp),
             misc.datetime_to_microtime(categorization_batch.created_at),
             categorization_batch.num_photos,
             1 if categorization_batch.is_finished else 0))
        self.dbconn.commit()
        return categorization_batch

    def get_most_recent_batch(self):
        """Returns the most recent categorization batch.

        :rtype: parkme.models.CategorizationBatch
        """
        cursor = self.dbconn.cursor()
        cursor.execute(
            """
            SELECT * FROM categorization_batch
            ORDER BY created_at DESC LIMIT 1
            """)
        result = cursor.fetchone()
        if result:
            return self._raw_result_to_categorization_batch_obj(result)
        return None

    def get_all_unfinished(self):
        """Return a list containing the information for all unfinished batches.

        :rtype: list of parkme.models.CategorizationBatch
        """
        cursor = self.dbconn.cursor()
        cursor.execute(
            """SELECT * FROM categorization_batch WHERE is_finished=0
             ORDER BY created_at DESC;""")
        results = []
        for result in cursor:
            results.append(
                self._raw_result_to_categorization_batch_obj(result))
        return results

    def _raw_result_to_categorization_batch_obj(self, raw_result):
        """Convert a raw result from the database into a categorization batch
        object.

        :param raw_result: A raw result
        :type raw_result: tuple
        :rtype: parkme.models.CategorizationBatch
        """
        localized_newest_photo_timestamp = (
            pytz.utc.localize(misc.microtime_to_datetime(raw_result[1])))
        localized_created_at = (
            pytz.utc.localize(misc.microtime_to_datetime(raw_result[2])))
        return CategorizationBatch(
            categorization_batch_id=raw_result[0],
            newest_photo_timestamp=localized_newest_photo_timestamp,
            created_at=localized_created_at,
            num_photos=raw_result[3],
            is_finished=bool(raw_result[4]))
