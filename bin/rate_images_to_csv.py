# -*- coding: utf-8 -*-
import sys
sys.path.append('')

import csv

from parkme import db


def convert_row_to_csv_row(asset):
    """Convert the given asset row into a CSV file row.

    :param asset: An asset row from the database
    :type asset: tuple
    :rtype: tuple
    """
    return asset[0], asset[1], 'http://{}/{}'.format(asset[2], asset[3])


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: rate_images_to_csv.py [OUTPUTCSV]'
        print 'Export images ready to be transcribed to CSV file.'
        exit(1)

    with db.cursor() as (cur, _):
        cur.execute('''
        SELECT pk_asset, pk_lot, str_bucket, str_path FROM (
          SELECT asset.* FROM ASSET LEFT JOIN
          asset_lot_asset_type_xref USING(pk_asset)
          WHERE pk_lot_asset_type=4
        ) AS assetct LEFT OUTER JOIN lot USING(pk_lot)
        WHERE (str_rates IS NULL OR str_rates='') AND pk_lot_status != 7
        ''')

        output_field_names = ['asset_id', 'lot_id', 'image_url']

        with open(sys.argv[1], 'wb+') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(output_field_names)
            for row in cur:
                csvwriter.writerow(convert_row_to_csv_row(row))
