import sys
sys.path.append('')

import collections

from boto.mturk import connection

from parkme import settings
from parkme.turk import assignments


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: process_categorization_results.py [BATCH_ID]"
        print "Print out results of scanning validation results."
        exit(1)

    batch_id = int(sys.argv[1])

    assignments_for_assets = collections.defaultdict(list)

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
        if all([
                set(each.categories) == set(assignments[0].categories)
                for each in assignments[1:]]):
            print "{} ACCEPTED".format(asset_id)
            for each in assignments:
                assignment_gateway.accept(each)
        else:
            print "{} REJECTED".format(asset_id)
        print [each.categories for each in assignments]
