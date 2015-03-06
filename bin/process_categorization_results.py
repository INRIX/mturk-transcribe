import sys
sys.path.append('')

import collections
import copy
import uuid

from boto.mturk import connection
import psycopg2

from parkme import db
from parkme import settings
from parkme.turk import assignments as turk_assignments


# Constants taken from ParkMe app.
CATEGORY_TO_LOT_ASSET_TYPE = {
    'rates': 4,
    'entrance': 2,
    'hours': 5,
    'operator': 11,
    'phone': 8,
    'paymenttypes': 16
}


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


def majority_considered_uncategorizable(assignments):
    """Indicates whether or not the majority of Mechanical Turk workers marked
    this assignment as not matching any categories or not.

    :param assignments: A list of assignments
    :type assignments: list of ImageCategorizationAssignment
    :rtype: bool
    """
    num_marked_does_not_match = len([
        each for each in assignments if each.does_not_match])
    return (num_marked_does_not_match + 1) >= len(assignments)


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
    with db.cursor() as (cur, conn):
        for category_name in categories:
            lot_asset_type_id = CATEGORY_TO_LOT_ASSET_TYPE[
                category_name.lower()]
            try:
                cur.execute('''
                INSERT INTO asset_lot_asset_type_xref
                (pk_asset_lot_asset_type_xref, pk_asset, pk_lot_asset_type,
                str_create_who, dt_create_date, str_modified_who, dt_modified_date)
                VALUES (%s, %s, %s, 'mturk', now(), 'mturk', now())
                ''', (str(uuid.uuid4()), asset_id, lot_asset_type_id))
                conn.commit()
            except psycopg2.IntegrityError:
                conn.rollback()
                continue


def mark_show_quality(pk_asset):
    """Mark the given asset as show quality.

    :param pk_asset: An asset id
    :type pk_asset: str or unicode
    """
    with db.cursor() as (cur, _):
        cur.execute(
            'UPDATE asset SET b_show_quality=true WHERE pk_asset=%s',
            (pk_asset,))


def unmark_show_quality(pk_asset):
    """Unmark the given asset as show quality.

    :param pk_asset: An asset id
    :type pk_asset: str or unicode
    """
    with db.cursor() as (cur, _):
        cur.execute(
            'UPDATE asset SET b_show_quality=false WHERE pk_asset=%s',
            (pk_asset,))


def mark_approved(pk_asset):
    """Mark the given asset as approved.

    :param pk_asset: An asset id
    :type pk_asset: str or unicode
    """
    with db.cursor() as (cur, _):
        cur.execute(
            'UPDATE asset SET pk_asset_status=2 WHERE pk_asset=%s',
            (pk_asset,))


def adjust_show_quality_images_for_lot(lot_id):
    """For the given lot ensure that it has one show quality image for each
    category. If there's a tie simply choose the most recent image in the given
    category.

    :param lot_id: A lot id
    :type lot_id: str or unicode
    """
    with db.cursor() as (cur, _):
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
            # Sorted to find the newest assets
            sorted_assets = sorted(assets, key=lambda x: x[2], reverse=True)
            sorted_asset_ids = [asset[0] for asset in sorted_assets]
            filtered_sorted_asset_ids = copy.copy(sorted_asset_ids)
            # Remove all already marked assets from sorted asset ids
            for next_asset_id in already_marked_asset_ids:
                try:
                    filtered_sorted_asset_ids.remove(next_asset_id)
                except ValueError:
                    # Item is not in list
                    pass
            # If there's only one asset, all assets are previously marked
            asset_id_to_mark = None
            asset_ids_to_unmark = []
            # If we have a case where we should just use the newest asset
            # These cases are:
            #   - There's only a single asset for this category
            #   - All assets have already been marked previously
            #   - The first asset hasn't been marked yet
            if (len(sorted_asset_ids) == 1 or
                    not filtered_sorted_asset_ids or
                    sorted_asset_ids[0] not in already_marked_asset_ids):
                asset_id_to_mark = sorted_asset_ids[0]
                asset_ids_to_unmark = sorted_asset_ids[1:]
            else:
                # Otherwise mark the newest filtered asset
                asset_id_to_mark = filtered_sorted_asset_ids[0]
                asset_ids_to_unmark = filtered_sorted_asset_ids[1:]
            mark_show_quality(asset_id_to_mark)
            already_marked_asset_ids.add(asset_id_to_mark)
            for next_asset_id in asset_ids_to_unmark:
                if next_asset_id not in already_marked_asset_ids:
                    unmark_show_quality(next_asset_id)


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
    assignment_gateway = turk_assignments.AssignmentGateway.get(
        mturk_connection)
    all_assignments = assignment_gateway.get_by_batch_id(
        batch_id, turk_assignments.ImageCategorizationAssignment)

    # Group all assignments by their referenced asset, accumulate lot ids
    for each in all_assignments:
        assignments_for_assets[each.asset_id].append(each)
        lot_ids.add(each.lot_id)

    # Check for any assignments where the answers do not match
    for asset_id, assignments in assignments_for_assets.iteritems():
        if len(assignments) >= 3:
            if has_consensus_on_categories(assignments):
                print '{} ACCEPTED {}'.format(assignments[0].hit_id, asset_id)
                reject_empty_assignments(assignments, assignment_gateway)
                winning_categories = get_consensus_categories(assignments)
                for each in assignments:
                    if each.categories:
                        assignment_gateway.accept(each)
                accepted_hits.add(assignments[0].hit_id)
                set_categories_for_asset(asset_id, winning_categories)
                mark_approved(asset_id)
            elif majority_considered_uncategorizable(assignments):
                # Accept all of the assignments (but don't keep categories)
                for each in assignments:
                    assignment_gateway.accept(each)
                # Mark the asset as approved
                mark_approved(asset_id)
            else:
                print "{} REJECTED".format(assignments[0].hit_id)
                reject_empty_assignments(assignments, assignment_gateway)
                rejected_hits.add(assignments[0].hit_id)
        else:
            print '{} NOT ENOUGH'.format(assignments[0].hit_id)
        print [each.categories for each in assignments]

    # Adjust show quality images based on classification results
    print "Adjusting show quality images..."
    for lot_id in lot_ids:
        print lot_id
        adjust_show_quality_images_for_lot(lot_id)

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
