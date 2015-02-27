# -*- coding: utf-8 -*-
import sys
sys.path.append('')

import collections
import itertools
import pprint

import psycopg2


def partition_assets(assets):
    """Partition the given group of assets into those that are show quality and
    those that are not show quality.

    :param assets: The assets for a given lot
    :type assets: list of tuples
    :rtype: tuple of tuples - (show quality, not show quality)
    """
    def as_new_as_pred(new_as_date):
        return lambda x: x[-1].date() >= new_as_date
    sorted_assets = sorted(assets, key=lambda x: x[-1].date(), reverse=True)
    newest_date = sorted_assets[0][-1].date()
    show_quality = filter(as_new_as_pred(newest_date), assets)
    not_show_quality = itertools.ifilterfalse(
        as_new_as_pred(newest_date), assets)
    return show_quality, list(not_show_quality)


if __name__ == '__main__':
    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    cur.execute('''
    SELECT asset.pk_lot, pk_asset, str_bucket, str_path, asset.dt_create_date
    FROM asset
    LEFT JOIN lot USING (pk_lot)
    WHERE
    pk_asset_status = 1
    AND (str_rates IS NULL OR str_rates = '')
    AND pk_lot_status != 7 ORDER BY asset.pk_lot LIMIT 50;
    ''')
    lot_to_assets = collections.defaultdict(list)
    for each in cur:
        lot_to_assets[each[0]].append(each)
    for lot_id, assets in lot_to_assets.iteritems():
        print "[{}]".format(lot_id)
        pprint.pprint(assets)
        show_quality, not_show_quality = partition_assets(assets)
        print "[SHOW QUALITY]"
        pprint.pprint(show_quality)
        print "[NOT SHOW QUALITY]"
        pprint.pprint(not_show_quality)
    conn.close()
