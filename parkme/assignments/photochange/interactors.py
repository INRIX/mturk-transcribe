# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.interactors
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Interactors containing functionality for photo change assignment.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
import datetime
import functools

from parkme import exceptions
from parkme.assignments.photochange import models


#TODO(etscrivner): Remove these temporary notes
# Getting available assets:
#
# - For each lot in the system
#   - For each rates image in the system
#     - Sort and retrieve only the newest rates
#
# Lots can be in any of these situations:
#
# - Newest photo not yet marked as show quality
# - Newest photo marked as show quality
#
# No matter what the scenario is we'd like to compare the newest photo against
# all the other images.


def has_enough_assets_to_compare(asset_group):
    """Simple predicate to determine whether the given asset group contains
    enough assets to run a comparison.

    :param asset_group: A list of asset
    :type asset_group: list
    :rtype: bool
    """
    return asset_group and len(asset_group) > 1


def get_newest_asset(asset_group):
    """Return the newest asset in an asset group"""
    return asset_group[0]


def get_remaining_assets(asset_group):
    """Return the remaining assets in an asset group"""
    return asset_group[1:]


def get_comparable_assets_for_lot(db_connection, lot_id):
    """Get all assets ready for comparison.

    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    :param lot_id: A lot's string (not integer) id.
    :type lot_id: str or unicode
    :rtype: iterable of parkme.assignments.photochange.models.ComparableAsset
    """
    if not db_connection:
        raise exceptions.Error('Invalid db_connection given')

    if not lot_id:
        raise exceptions.Error('Invalid lot_id given')

    cursor = db_connection.cursor()
    params = [
        lot_id,
        datetime.datetime.utcnow().year
    ]
    query = '''
    SELECT pk_asset, pk_lot, str_bucket, str_path, dt_photo FROM asset
    LEFT JOIN asset_lot_asset_type_xref USING(pk_asset) WHERE
    pk_lot_asset_type=4 AND
    pk_asset_category=2 AND
    pk_asset_status IN (1, 2) AND
    pk_lot=%s AND
    extract(year from dt_photo) < %s
    ORDER BY asset.dt_photo DESC
    '''
    cursor.execute(query, params)
    return functools.imap(models.ComparableAsset, cursor)


def get_all_lots(db_connection):
    """Returns an iterable containing all the lots in the database.

    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    :rtype: psycopg2.Cursor
    """
    if not db_connection:
        raise exceptions.Error('Invalid db_connection given')

    cursor = db_connection.cursor()
    query = '''
    SELECT pk_lot FROM lot WHERE
    pk_lot_status != 7 AND
    str_rates IS NOT NULL AND
    str_rates != ''
    '''
    cursor.execute(query)
    return cursor


def get_comparable_assets(db_connection):
    """Yields groups of comparable assets one lot at a time.

    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    :rtype: iterable
    """
    for lot_id in get_all_lots(db_connection):
        yield list(get_comparable_assets_for_lot(db_connection, lot_id))


def upload_tasks_to_turk(mturk_connection, db_connection):
    """Upload tasks to Mechanical Turk.

    :param mturk_connection: A Mechanical Turk connection
    :type mturk_connection: boto.mturk.Connection
    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    """
    for asset_group in get_comparable_assets(db_connection):
        if not has_enough_assets_to_compare(asset_group):
            continue

        newest_asset = get_newest_asset(asset_group)
        remaining_assets = get_remaining_assets(asset_group)
