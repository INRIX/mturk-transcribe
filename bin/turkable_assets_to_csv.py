# -*- coding: utf-8 -*-
import sys
sys.path.append('')

import csv

import psycopg2


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: turkable_assets_to_csv.py [CSVFILE]"
        print "Persist all turkable assets to a CSV file for batching"
        print "processing."
        exit(1)

    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    field_names = ['lot_id', 'asset_id', 'str_bucket', 'str_path', 'dt_create_date']
    cur.execute('''
    SELECT asset.pk_lot, pk_asset, str_bucket, str_path, asset.dt_create_date
    FROM asset
    LEFT JOIN lot USING (pk_lot)
    WHERE
    pk_asset_status = 1
    AND (str_rates IS NULL OR str_rates = '')
    AND pk_lot_status != 7 ORDER BY asset.dt_create_date DESC;
    ''')
    with open(sys.argv[1], 'wb+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(field_names)
        for each in cur:
            csvwriter.writerow(each)
    conn.close()
