# -*- coding: utf-8 -*-
import sys
sys.path.append('')

import collections

import psycopg2


Asset = collections.namedtuple(
    'Asset',
    ['lot_id', 'asset_id', 'asset_category_id',
     'asset_status_id' 'category_id' 'str_bucket',
     'str_path', 'dt_create_date'])


def mark_asset_as_show_quality(conn, asset_id):
    """Mark the given asset as show quality.

    :param conn: A postgresql connection
    :type conn: psycopg2.Connection
    :param asset_id: An asset id
    :type asset_id: str or unicode
    """
    cur = conn.cursor()
    cur.execute(
        'UPDATE asset SET b_show_quality=TRUE WHERE pk_asset=%s;',
        (asset_id,))


def mark_asset_as_approved(conn, asset_id):
    """Mark the given asset as approved.

    :param conn: A postgresql connection
    :type conn: psycopg2.Connection
    :param asset_id: An asset id
    :type asset_id: str or unicode
    """
    cur = conn.cursor()
    cur.execute(
        'UPDATE asset SET pk_asset_status=2 WHERE pk_asset=%s;',
        (asset_id,))


def has_associated_lot_asset_types(conn, asset_id):
    """Indicates whether or not the lot with the given ID has any associated
    asset types.

    :param asset_id: An asset id
    :type asset_id: str or unicode
    :rtype: bool
    """
    cur = conn.cursor()
    cur.execute(
        'SELECT COUNT(*) FROM asset_lot_asset_type_xref WHERE pk_asset=%s;',
        (asset_id,))
    result = cur.fetchone()
    return result[0] > 0L


if __name__ == '__main__':
    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    cur.execute('''
    SELECT asset.pk_lot, pk_asset, pk_asset_category,
    str_bucket, str_path, asset.dt_create_date
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
        assets = [Asset(*each) for each in assets]
        for each in assets:
            if has_associated_lot_asset_types(conn, each.asset_id):
                mark_asset_as_show_quality(conn, each.asset_id)
            mark_asset_as_approved(conn, each.asset_id)
    conn.close()
