# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.interactors
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Interactors containing functionality for photo change assignment.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
import datetime

from parkme.assignments.photochange import models


#TODO(etscrivner): Remove these temporary notes
# Getting available assets:
#
# - For each lot in the system
#   - For each rates image in the system
#     - Sort and retrieve only the newest rates


def get_assets_for_comparison(db_connection, lot_id):
    """Get all assets ready for comparison.

    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    :param lot_id: A lot's string (not integer) id.
    :type lot_id: str or unicode
    :rtype: psycopg2.Cursor
    """
    cursor = db_connection.cursor()
    params = [
        lot_id,
        datetime.datetime.utcnow().year
    ]
    query = '''
    SELECT * FROM asset
    LEFT JOIN asset_lot_asset_type_xref USING(pk_asset) WHERE
    pk_lot_asset_type=4 AND
    pk_asset_category=2 AND
    pk_asset_status IN (1, 2) AND
    pk_lot=%s AND
    extract(year from dt_photo) < %s
    ORDER BY asset.dt_create_date DESC
    '''
    cursor.execute(query, params)
    return cursor
