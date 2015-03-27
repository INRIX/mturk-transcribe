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
import pytz

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


def get_uncategorized_assets(dbconn, exclude_before_dt=None):
    """Returns the uncategorized assets found in the database.

    :param dbconn: A database connection
    :type dbconn: psycopg2.Connection
    :param exclude_before_dt: (Optional) Exclude items before the given time
    :type exclude_before_dt: datetime.datetime
    :rtype: psycopg2.Cursor
    """
    cur = dbconn.cursor()
    params = [datetime.datetime.utcnow().year]
    query = '''
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
    AND assetct.category_count=0
    '''

    # Add clause to exclude photos before timestamp
    if exclude_before_dt:
        query += 'AND dt_photo > %s'
        params.append(exclude_before_dt)

    query += ';'
    cur.execute(query, params)
    return cur


def get_row_timestamp(row):
    """Return the photo timestamp from the given row

    :param row: A database row
    :type row: tuple
    :rtype: datetime.datetime
    """
    return row[3]


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


def get_last_categorized_dt(data_gateway):
    """Get the timestamp of the oldest categorized photo.

    :param data_gateway: A categorization batch data gateway
    :type data_gateway: parkme.models.CategorizationBatchDataGateway
    :rtype: datetime.datetime
    """
    most_recent_batch = data_gateway.get_most_recent_batch()
    if most_recent_batch:
        return most_recent_batch.newest_photo_timestamp
    return datetime.datetime(
        year=datetime.MINYEAR, month=1, day=1, tzinfo=pytz.utc)


def print_summary(num_photos, hit_template):
    """Print a summary containing information about num photos uploaded and
    estimated pricing.

    :param num_photos: The number of photos
    :type num_photos: int
    :param hit_template: A HIT template
    :type hit_template: parkme.turk.hits.HITTemplate
    """
    num_assignments = num_photos * hit_template.assignments_per_hit
    print
    print '{} Photos, {} Assignments'.format(num_photos, num_assignments)
    print 'Estimated Payout: ${:0.02f}'.format(
        num_assignments * hit_template.reward_per_assignment)


def save_categorization_batch(data_gateway, batch_id, newest_categorized_dt):
    """Save a new categorization batch with the given information.

    :param data_gateway: A data gateway
    :type data_gateway: parkme.models.CategorizationBatchDataGateway
    :param batch_id: A unique ID for the batch
    :type batch_id: str or unicode
    :param newest_categorized_dt: Timestamp for newest photo categorized
    :type newest_categorized_dt: datetime.datetime
    :rtype: parkme.models.CategorizationBatch
    """
    categorization_batch = models.CategorizationBatch(
        categorization_batch_id=batch_id,
        newest_photo_timestamp=newest_categorized_dt,
        created_at=datetime.datetime.utcnow(),
        is_finished=False)
    data_gateway.save(categorization_batch)
    return categorization_batch


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

    batch_id = str(uuid.uuid4())
    num_photos = 0
    last_categorized_dt = get_last_categorized_dt(data_gateway)

    print 'BatchID:', batch_id

    for row in get_uncategorized_assets(dbconn, last_categorized_dt):
        if get_row_timestamp(row) >= last_categorized_dt:
            last_categorized_dt = get_row_timestamp(row)
        data = row_to_hit_data(row)
        print data
        if not options.dry_run:
            hit_template.create_hit(data, batch_id=batch_id)
        num_photos += 1

    print_summary(num_photos, hit_template)
    save_categorization_batch(data_gateway, batch_id, last_categorized_dt)

    dbconn.close()
