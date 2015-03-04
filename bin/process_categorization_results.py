import sys
sys.path.append('')

import collections
import contextlib
import uuid

from boto.mturk import connection
import psycopg2

from parkme import settings
from parkme.turk import assignments


# Constants taken from ParkMe app.
CATEGORY_TO_LOT_ASSET_TYPE = {
    'rates': 4,
    'entrance': 2,
    'hours': 5
}


@contextlib.contextmanager
def db_cursor():
    """Simple context manager for creating a database cursor for user without
    having to do the manual cleanup.

    :rtype: psycopg2.Connection
    """
    db_connection = psycopg2.connect("dbname=pim user=pim")
    cursor = db_connection.cursor()
    yield cursor
    cursor.commit()
    cursor.close()
    db_connection.close()


def reject_empty_assignments(assignments, assignment_gateway):
    """Reject any assignments where the user did not choose an answer.

    :param assignments: A list of assignments
    :type assignments: list
    """
    for each in assignments:
        if each.categories is None:
            print '--> {} REJECTED AS EMPTY <--'.format(each.assignment_id)
            assignment_gateway.reject(
                each, feedback='Did not select any options')


def has_consensus_on_categories(assignments):
    """Indicates whether or not there is a most common category in the given
    list of assignments.

    :param assignments: A list of assignments
    :type assignments: list
    :rtype: bool
    """
    return bool(get_consensus_categories(assignments))


def get_consensus_categories(assignments):
    """Return the most common category.

    :param assignments: a list of assignments
    :type assignments: list
    :rtype: bool
    """
    if len(assignments) < 2:
        return []

    results = collections.Counter([])
    for item in assignments:
        if item.categories:
            results.update(item.categories)

    return [category for category, count in results.iteritems() if count >= 2]


def set_categories_for_asset(asset_id, categories):
    """Update the ParkMe asset with the given ID to have the given
    categories.

    :param asset_id: An asset id
    :type asset_id: str or unicode
    :param categories: A list of categories
    :type categories: list
    """
    with db_cursor() as cur:
        for category_name in categories:
            lot_asset_type_id = CATEGORY_TO_LOT_ASSET_TYPE[
                category_name.lower()]
            cur.execute('''
            INSERT INTO asset_lot_asset_type_xref
            (pk_asset_lot_asset_type_xref, pk_asset, pk_lot_asset_type,
            str_create_who, dt_create_date, str_modified_who, dt_modified_date)
            VALUES (%s, %s, %s, 'mturk', now(), 'mturk', now())
            ''', (str(uuid.uuid4()), asset_id, lot_asset_type_id))


def mark_show_quality(pk_asset):
    """Mark the given asset as show quality.

    :param pk_asset: An asset id
    :type pk_asset: str or unicode
    """
    with db_cursor() as cur:
        cur.execute(
            'UPDATE asset SET b_show_quality=true WHERE pk_asset=%s',
            (pk_asset,))


def unmark_show_quality(pk_asset):
    """Unmark the given asset as show quality.

    :param pk_asset: An asset id
    :type pk_asset: str or unicode
    """
    with db_cursor() as cur:
        cur.execute(
            'UPDATE asset SET b_show_quality=false WHERE pk_asset=%s',
            (pk_asset,))


def adjust_show_quality_images_for_lot(lot_id):
    """For the given lot ensure that it has one show quality image for each
    category. If there's a tie simply choose the most recent image in the given
    category.

    :param lot_id: A lot id
    :type lot_id: str or unicode
    """
    with db_cursor() as cur:
        # Fetch all of the assets for the given lot
        cur.execute('''
        SELECT pk_asset, pk_lot_asset_type, dt_photo
        FROM asset LEFT OUTER JOIN asset_lot_asset_type_xref USING (pk_asset)
        WHERE pk_lot_asset_type IS NOT NULL AND pk_lot=%s;
        ''', (lot_id,))
        # Sort assets by category
        category_to_assets = collections.defaultdict(list)
        for asset in cur:
            category_to_assets[asset[1]].append(asset)
        # For each category
        already_marked_asset_ids = set([])
        for _, assets in category_to_assets.iteritems():
            # Find all assets that haven't been marked in another category
            sorted_assets = sorted(assets, key=lambda x: x[2], reverse=True)
            mark_show_quality(sorted_assets[0][0])
            already_marked_asset_ids.add(sorted_assets[0][0])
            for asset in sorted_assets:
                if asset[0] not in already_marked_asset_ids:
                    unmark_show_quality(asset[0])


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: process_categorization_results.py [BATCH_ID]"
        print "Print out results of scanning validation results."
        exit(1)

    batch_id = int(sys.argv[1])

    assignments_for_assets = collections.defaultdict(list)
    accepted_hits = set([])
    rejected_hits = set([])
    lot_ids = set([])

    mturk_connection = connection.MTurkConnection(
       aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
       aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    assignment_gateway = assignments.AssignmentGateway.get(mturk_connection)
    all_assignments = assignment_gateway.get_by_batch_id(
        batch_id, assignments.ImageCategorizationAssignment)

    # Group all assignments by their referenced asset, accumulate lot ids
    for each in all_assignments:
        assignments_for_assets[each.asset_id].append(each)
        lot_ids.add(each.lot_id)

    # Check for any assignments where the answers do not match
    for asset_id, assignments in assignments_for_assets.iteritems():
        if len(assignments) >= 3:
            reject_empty_assignments(assignments, assignment_gateway)

            if has_consensus_on_categories(assignments):
                print '{} ACCEPTED {}'.format(assignments[0].hit_id, asset_id)
                winning_categories = get_consensus_categories(assignments)
                for each in assignments:
                    if each.categories:
                        assignment_gateway.accept(each)
                accepted_hits.add(assignments[0].hit_id)
                set_categories_for_asset(asset_id, winning_categories)
            else:
                print "{} REJECTED".format(assignments[0].hit_id)
                rejected_hits.add(assignments[0].hit_id)
        else:
            print '{} NOT ENOUGH'.format(assignments[0].hit_id)
        print [each.categories for each in assignments]

    percent_accepted = (
        float(len(accepted_hits)) / len(assignments_for_assets.keys())
        * 100.0)
    print
    print "RESULTS"
    print "{} Accepted, {} Rejected ({:0.02f}%)".format(
        len(accepted_hits), len(rejected_hits), percent_accepted)
    print
    print "REJECTED HIT IDS"
    for hit_id in rejected_hits:
        print hit_id
