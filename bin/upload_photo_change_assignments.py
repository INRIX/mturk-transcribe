# -*- coding: utf-8 -*-
"""
    upload_photo_change_assignments
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Upload photo change assignments

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections
import csv
import sys
import uuid

sys.path.append('')

from boto.mturk import connection
import psycopg2

from parkme.assignments.photochange import uploader
from parkme.assignments.photochange import models
from parkme import settings


#INFO(etscrivner): Lot IDs provided by the data team for testing
LOT_IDS_FIXTURE = [
    78393, 17580, 77747, 32963, 17203,
    77465, 33055, 17185, 77759, 99482,
    16874, 16932, 16389, 17931, 78269,
    17171, 112761, 16416, 17637, 13752
]

def get_total_cost_estimate(num_assignments):
    amazons_fee = 1.10
    return (
        num_assignments *
        models.PhotoChangeTemplate.ASSIGNMENTS_PER_HIT *
        models.PhotoChangeTemplate.PRICE_PER_ASSIGNMENT_DOLLARS *
        amazons_fee)


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


def get_lot_ids_from_csv_file(csv_file_path):
    """Get lot ids from the given CSV file.

    :param csv_file_path: The path to the file
    :type csv_file_path: str or unicode
    :rtype: list
    """
    lot_ids = []
    with open(csv_file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            lot_ids.append(row['pk_lot'])
    return lot_ids


if __name__ == '__main__':
    pgsql_connection = psycopg2.connect("dbname=pim user=pim")
    batch_id = str(uuid.uuid4())
    lot_ids = []

    try:
        if len(sys.argv) == 2:
            lot_ids = get_lot_ids_from_csv_file(sys.argv[1])
        else:
            lot_ids = [
                get_lot_id(psql_connection, each) for each in LOT_IDS_FIXTURE]

        print 'HIT ID: {}'.format(batch_id)
        mturk_connection = connection.MTurkConnection(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        num_assignments = 0
        for lot_id in lot_ids:
            print lot_id
            comparable_assets = list(uploader.get_comparable_assets_for_lot(
                pgsql_connection, lot_id))

            if not uploader.has_enough_assets_to_compare(comparable_assets):
                print 'Not enough assets.'
                print
                continue
            newest_asset = uploader.get_newest_asset(comparable_assets)
            older_assets = uploader.get_remaining_assets(comparable_assets)
            uploader.upload_assignments_to_turk(
                mturk_connection, batch_id, newest_asset, older_assets)
            num_assignments += len(older_assets)
            print

        print "Uploaded {} Assignments.".format(num_assignments)
        print "Estimated Cost ${:0.02f}".format(get_total_cost_estimate(num_assignments))
    finally:
        pgsql_connection.close()
