# -*- coding: utf-8 -*-
import sys
sys.path.append('')

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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: turkable_assets_to_csv.py [CSVFILE]"
        print "Persist all turkable assets to a CSV file for batching"
        print "processing."
        exit(1)

    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    cur.execute('''
    SELECT assetct.pk_asset, str_bucket, str_path FROM (
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

    with open(sys.argv[1], 'wb+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_field_names)
        for each in cur:
            csvwriter.writerow(convert_row_to_csv_row(each))
    conn.close()
