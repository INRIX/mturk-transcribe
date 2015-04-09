# -*- coding: utf-8 -*-
"""
    parkme.assignments.photochange.uploader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Interactors containing functionality to upload photo change assignments to
    Mechanical Turk.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
import itertools

from parkme import exceptions
from parkme.assignments import utils
from parkme.assignments.photochange import models


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


def get_assignment_data(new_asset, old_asset):
    """Convert the given comparable assets into format for image comparison
    task.

    :param new_asset: The new asset
    :type new_asset: models.ComparableAsset
    :param old_asset: The old asset
    :type old_asset: models.ComparableAsset
    :rtype: dict
    """
    new_image_url = utils.asset_to_image_url(
        new_asset.str_bucket, new_asset.str_path)
    old_image_url = utils.asset_to_image_url(
        old_asset.str_bucket, old_asset.str_path)
    return {
        'new_image_url': new_image_url,
        'new_asset_id': new_asset.asset_id,
        'old_image_url': old_image_url,
        'old_asset_id': old_asset.asset_id,
        'lot_id': new_asset.lot_id
    }


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
    params = [lot_id]
    query = '''
    SELECT pk_asset, pk_lot, str_bucket, str_path, dt_photo FROM asset
    LEFT JOIN asset_lot_asset_type_xref USING(pk_asset) WHERE
    pk_lot_asset_type=4 AND
    pk_asset_category=2 AND
    pk_asset_status IN (1, 2) AND
    pk_lot=%s
    ORDER BY asset.dt_photo DESC
    '''
    cursor.execute(query, params)
    return itertools.imap(lambda args: models.ComparableAsset(*args), cursor)


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
    for (lot_id,) in get_all_lots(db_connection):
        print lot_id
        yield list(get_comparable_assets_for_lot(db_connection, lot_id))


def upload_assignments_to_turk(
        mturk_connection, batch_id, new_asset, older_assets):
    """Given the newest asset and a list of older assets this task will create an
    assignment to match the new asset with each of the older assets.

    :param mturk_connection: A Mechanical Turk connection
    :type mturk_connection: boto.mturk.connection.Connection
    :param batch_id: The ID to be associated wtih this batch of tasks
    :type batch_id: str or unicode
    :param new_asset: The newest asset
    :type new_asset: parkme.assignments.photochange.models.ComparableAsset
    :param older_assets: The older assets
    :type older_assets: list
    """
    hit_template = models.PhotoChangeTemplate(mturk_connection)
    for index, old_asset in enumerate(older_assets):
        assignment_data = get_assignment_data(new_asset, old_asset)
        if not index:
            print assignment_data['new_image_url']
        print assignment_data['old_image_url']
        hit_template.create_hit(assignment_data, batch_id)


def upload_all_tasks_to_turk(mturk_connection, batch_id, db_connection):
    """Upload all tasks to Mechanical Turk.

    :param mturk_connection: A Mechanical Turk connection
    :type mturk_connection: boto.mturk.Connection
    :param batch_id: The ID to be associated wtih this batch of tasks
    :type batch_id: str or unicode
    :param db_connection: A database connection
    :type db_connection: psycopg2.Connection
    :return: Number of items uploaded
    :rtype: int
    """
    num_items_uploaded = 0
    for asset_group in get_comparable_assets(db_connection):
        if not has_enough_assets_to_compare(asset_group):
            continue

        newest_asset = get_newest_asset(asset_group)
        remaining_assets = get_remaining_assets(asset_group)
        upload_assignments_to_turk(
            mturk_connection, batch_id, newest_asset, remaining_assets)
        num_items_uploaded += len(remaining_assets)
    return num_items_uploaded
