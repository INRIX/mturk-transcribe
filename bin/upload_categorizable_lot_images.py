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


if __name__ == '__main__':
    mturk_connection = connection.MTurkConnection(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    #dbconn = psycopg2.connect("dbname=pim user=pim")

    data_gateway = models.CategorizationBatchDataGateway('db.sqlite3')
    data_gateway.create_table()

    hit_template = CategorizeLotPhotoTemplate(mturk_connection)

    photos = [{
        'asset_id': 'dcabd582-4180-11e3-962d-22000afd0bd2',
        'lot_id': 'bd2c4a51-74ef-11df-bd82-e0cb4e8adbbe',
        'image_url': 'http://s3-w2.parkme.com/lot_img/16365/3d45e861571847e7945b4ca5441f0e63.jpg'
    }]
    
    batch_id = str(uuid.uuid4())
    num_photos = 0
    print 'BatchID:', batch_id
    for each in photos:
        #hit_template.create_hit(each, batch_id=batch_id)
        num_photos += 1
    num_assignments = num_photos * 3
    print
    print '{} Photos, {} Assignments'.format(num_photos, num_assignments)
    print 'Estimated Payout: ${:0.02f}'.format(
        num_assignments * hit_template.reward_per_assignment)

    #dbconn.close()
