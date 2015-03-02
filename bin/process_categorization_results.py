import sys
sys.path.append('')

import collections
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


def has_most_common_category(assignments):
    """Indicates whether or not there is a most common category in the given
    list of assignments.

    :param assignments: A list of assignments
    :type assignments: list
    :rtype: bool
    """
    if len(assignments) < 2:
        return False

    results = collections.Counter([
        '|'.join(each.categories) for each in assignments if each.categories])
    most_common = results.most_common(2)
    return len(most_common) == 1 or most_common[0][1] > most_common[1][1]


def get_most_common_category(assignments):
    """Return the most common category.

    :param assignments: a list of assignments
    :type assignments: list
    :rtype: bool
    """
    if len(assignments) < 2:
        return False

    results = collections.Counter([
        '|'.join(each.categories) for each in assignments if each.categories])
    most_common = results.most_common(1)
    return most_common[0][0].split('|')


def set_categories_for_asset(asset_id, categories):
    """Update the ParkMe asset with the given ID to have the given
    categories.

    :param asset_id: An asset id
    :type asset_id: str or unicode
    :param categories: A list of categories
    :type categories: list
    """
    conn = psycopg2.connect("dbname=pim user=pim")
    cur = conn.cursor()
    for each in categories:
        lot_asset_type_id = CATEGORY_TO_LOT_ASSET_TYPE[each.lower()]
        cur.execute('''
        INSERT INTO asset_lot_asset_type_xref
        (pk_asset_lot_asset_type_xref, pk_asset, pk_lot_asset_type,
        str_create_who, dt_create_date, str_modified_who, dt_modified_date)
        VALUES (?, ?, ?, 'mturk', now(), 'mturk', now())
        ''', str(uuid.uuid4()), asset_id, lot_asset_type_id)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: process_categorization_results.py [BATCH_ID]"
        print "Print out results of scanning validation results."
        exit(1)

    batch_id = int(sys.argv[1])

    assignments_for_assets = collections.defaultdict(list)
    accepted_hits = set([])
    rejected_hits = set([])

    mturk_connection = connection.MTurkConnection(
       aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
       aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    assignment_gateway = assignments.AssignmentGateway.get(mturk_connection)
    all_assignments = assignment_gateway.get_by_batch_id(
        batch_id, assignments.ImageCategorizationAssignment)

    # Group all assignments by their referenced asset
    for each in all_assignments:
        assignments_for_assets[each.asset_id].append(each)

    # Check for any assignments where the answers do not match
    for asset_id, assignments in assignments_for_assets.iteritems():
        if len(assignments) >= 3:
            reject_empty_assignments(assignments, assignment_gateway)

            if has_most_common_category(assignments):
                print '{} ACCEPTED'.format(assignments[0].hit_id)
                winning_categories = get_most_common_category(assignments)
                for each in assignments:
                    if each.categories:
                        assignment_gateway.accept(each)
                accepted_hits.add(assignments[0].hit_id)
            else:
                print "{} REJECTED".format(assignments[0].hit_id)
                print assignments[0].hit_id
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
    print "REJECTED HIT IDS:"
    for hit_id in rejected_hits:
        print hit_id
