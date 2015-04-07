# -*- coding: utf-8 -*-
"""
    upload_photo_change_assignments
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Upload photo change assignments

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections
import sys
import uuid

sys.path.append('')

from boto.mturk import connection
import psycopg2

from parkme.assignments.photochange import interactors
from parkme import settings


#INFO(etscrivner): Lot IDs provided by the data team for testing
LOT_IDS_FIXTURE = [
    78393, 17580, 77747, 32963, 17203,
    77465, 33055, 17185, 77759, 99482,
    16874, 16932, 16389, 17931, 78269,
    17171, 112761, 16416, 17637, 13752
]


def get_lot_id(psql_connection, lot_id):
    """Return the string id of the lot with the given integer id.

    :param psql_connection: A SQL connection
    :type psql_connection: psycopg2.Connection
    :param lot_id: A lot id
    :type lot_id: int or long
    :rtype: str or None
    """
    cursor = psql_connection.cursor()
    cursor.execute(
        'SELECT pk_lot FROM lot WHERE pk_lot_id=%s', [lot_id])
    results = list(cursor)
    return results[0][0] if results else None


if __name__ == '__main__':
    pgsql_connection = psycopg2.connect("dbname=pim user=pim")
    hit_id = str(uuid.uuid4())
    try:
        print 'HIT ID: {}'.format(hit_id)
        mturk_connection = connection.MTurkConnection(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        lot_ids = [
            get_lot_id(pgsql_connection, lot_id) for lot_id in LOT_IDS_FIXTURE]
        for lot_id in lot_ids:
            print lot_id
            comparable_assets = list(interactors.get_comparable_assets_for_lot(
                pgsql_connection, lot_id))

            if not interactors.has_enough_assets_to_compare(comparable_assets):
                print 'Not enough assets.'
                print
                continue
            newest_asset = interactors.get_newest_asset(comparable_assets)
            older_assets = interactors.get_remaining_assets(comparable_assets)
            interactors.upload_assignments_to_turk(
                mturk_connection, newest_asset, older_assets)
            print
    finally:
        pgsql_connection.close()
