# -*- coding: utf-8 -*-
import sys
sys.path.append('')

import collections
import csv
import datetime

import psycopg2


def convert_row_to_csv_row(row):
    """Convert a row using our query into a row for output in CSV.

    :param row: A row
    :type row: tuple
    :rtype: tuple
    """
    # Format is image_url, asset_id
    return 'http://{}/{}'.format(row[1], row[2]), row[0]


def get_most_recent_assets(assets):
    """Return only the most recent assets from the list of assets.

    :param assets: A list of assets
    :type assets: list
    :rtype: list
    """
    def get_photo_timestamp(asset):
        return asset[3]

    if not assets:
        return []

    sorted_assets = sorted(assets, key=get_photo_timestamp, reversed=True)
    newest_date = get_photo_timestamp(sorted_assets[0])
    one_week_before_newest = newest_date - datetime.timedelta(days=8)

    most_recent_assets = []
    for each in sorted_assets:
        if get_photo_timestamp(each) >= one_week_before_newest:
            most_recent_assets.append(each)

    return most_recent_assets


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: turkable_assets_to_csv.py [CSVFILE]"
        print "Persist all turkable assets to a CSV file for batching"
        print "processing."
        exit(1)

    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    cur.execute('''
    SELECT assetct.pk_asset, str_bucket, str_path, dt_photo, pk_lot FROM (
       SELECT
       asset.*, COUNT(asset_lot_asset_type_xref.*) AS category_count
       FROM asset LEFT OUTER JOIN asset_lot_asset_type_xref
       USING (pk_asset) GROUP BY pk_asset
    ) AS assetct LEFT JOIN lot USING(pk_lot)
    WHERE pk_asset_status=1
    AND (str_rates IS NULL OR str_rates='')
    AND pk_lot_status != 7
    AND extract(year from dt_photo) < %s
    AND assetct.category_count=0;
    ''', (datetime.datetime.utcnow().year,))
    output_field_names = ['image_url', 'asset_id']

    # Group assets by lot
    lots_to_assets = collections.defaultdict(list)
    for each in cur:
        lots_to_assets[each[4]] = each

    # Find the most recent set of assets
    all_most_recent_assets = []
    for _, assets in lots_to_assets.iteritems():
        all_most_recent_assets += get_most_recent_assets(assets)

    with open(sys.argv[1], 'wb+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_field_names)
        for each in all_most_recent_assets:
            csvwriter.writerow(convert_row_to_csv_row(each))
    conn.close()
