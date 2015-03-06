"""
    parkme.db
    ~~~~~~~~~
    Utilities and basic interfaces for interacting with the database.

    Copyright (C) 2015 ParkMe, Inc. All Rights Reserved.
"""
import contextlib

import psycopg2


@contextlib.contextmanager
def cursor(connection_params=None):
    """Simple context manager for creating a database cursor for user without
    having to do the manual cleanup.

    :param connection_params: Any psycopg2 connection params
    :type connection_params: str or unicode
    :rtype: psycopg2.Connection
    """
    connection_params = (
        connection_params if connection_params else "dbname=pim user=pim")
    db_connection = psycopg2.connect(connection_params)
    db_cursor = db_connection.cursor()
    yield db_cursor, db_connection
    db_connection.commit()
    db_cursor.close()
    db_connection.close()
