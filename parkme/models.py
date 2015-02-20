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
    'TranscribedRate', ['hit_id', 'batch_id', 'rates', 'user_notes'])


class TranscribedRateDataGateway(object):
    """Represents the transcribed rates stored in SQLite"""

    def __init__(self, dbfile):
        """Create a new rates table instance with the given dbfile.

        :param dbfile: A database file
        :type dbfile: str or unicode
        """
        self.dbconn = sqlite3.connect(dbfile)

    def __del__(self):
        """Cleanup resources"""
        self.dbconn.close()

    def create_table(self):
        """Create the table if it doesn't already exist."""
        cursor = self.dbconn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcribed_rates
        (hit_id TEXT PRIMARY KEY, batch_id INTEGER, result TEXT,
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
            "INSERT OR REPLACE INTO transcribed_rates VALUES (?, ?, ?, ?)",
            (rate.hit_id, rate.batch_id, rate.rates, rate.user_notes))
        self.dbconn.commit()
        return rate
