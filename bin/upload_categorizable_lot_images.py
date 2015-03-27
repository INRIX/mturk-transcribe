"""
    upload_categorizable_lot_images
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Upload the categorizable lot images to Amazon Mechanical Turk using their
    API.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import sys
sys.path.append('')

import datetime
import optparse
import uuid

from boto.mturk import connection
import psycopg2

from parkme import models
from parkme import settings
from parkme.turk import hits


class CategorizeLotPhotoTemplate(hits.HITTemplate):
    """MTurk assignment to categorize lot photo templates"""

    # (TEST) The ID for [TEST] Categorize Parking Lot Photo
    HIT_LAYOUT_ID = '33LDDHGJVTUH37V05I853CFD5GV3Q6'
    # (PRODUCTION) The ID for Categorize Parking Lot Photo
    # HIT_LAYOUT_ID = '3MCDHXBQ4Z7SJ2ZT2XZACNE142JWKX'

    def __init__(self, mturk_connection):
        """Initialize categorization HIT template."""
        super(CategorizeLotPhotoTemplate, self).__init__(
            mturk_connection=mturk_connection,
            hit_layout_id=self.HIT_LAYOUT_ID,
            reward_per_assignment=0.02,
            assignments_per_hit=3,
            hit_expires_in=datetime.timedelta(days=7),
            time_per_assignment=datetime.timedelta(minutes=3),
            auto_approval_delay=datetime.timedelta(hours=8))


def get_uncategorized_assets(dbconn):
    """Returns the uncategorized assets found in the database.

    :param dbconn: A database connection
    :type dbconn: psycopg2.Connection
    :rtype: psycopg2.Cursor
    """
    cur = dbconn.cursor()
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
    return cur


def row_to_hit_data(row):
    """Convert the given database row to hit data.

    :param row: A database row
    :type row: tuple
    :rtype: dict
    """
    return {
        'asset_id': row[0],
        'lot_id': row[4],
        'image_url': 'http://{}/{}'.format(row[1], row[2])
    }


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option(
        '-d', '--dry-run', action='store_true', dest='dry_run', default=False)
    options, _ = parser.parse_args()

    if options.dry_run:
        print '[DRY RUN]'

    mturk_connection = connection.MTurkConnection(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    dbconn = psycopg2.connect("dbname=pim user=pim")

    data_gateway = models.CategorizationBatchDataGateway('db.sqlite3')
    data_gateway.create_table()

    hit_template = CategorizeLotPhotoTemplate(mturk_connection)
    most_recent_batch = data_gateway.get_most_recent_batch()

    batch_id = str(uuid.uuid4())
    num_photos = 0
    newest_photo_dt = (
        most_recent_batch.newest_photo_timestamp
        if most_recent_batch else
        datetime.datetime(year=datetime.MINYEAR, month=1, day=1))

    print 'BatchID:', batch_id

    for each in get_uncategorized_assets(dbconn):
        if each[3] >= newest_photo_dt:
            newest_photo_dt = each[3]
        data = row_to_hit_data(each)
        print data
        if not options.dry_run:
            hit_template.create_hit(data, batch_id=batch_id)
        num_photos += 1

    num_assignments = num_photos * hit_template.assignments_per_hit
    print
    print '{} Photos, {} Assignments'.format(num_photos, num_assignments)
    print 'Estimated Payout: ${:0.02f}'.format(
        num_assignments * hit_template.reward_per_assignment)

    cat_batch = models.CategorizationBatch(
        categorization_batch_id=batch_id,
        newest_photo_timestamp=newest_photo_dt,
        created_at=datetime.datetime.utcnow(),
        is_finished=False)
    data_gateway.save(cat_batch)

    dbconn.close()
