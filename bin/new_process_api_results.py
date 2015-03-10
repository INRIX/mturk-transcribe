import sys
sys.path.append('')

import collections
import itertools

from boto.mturk import connection

from parkme import settings
from parkme.ratecard import models as ratecard_models
from parkme.turk import assignments
from parkme.turk import hits


def has_consensus_not_rate_card(assignments):
    """Indicate whether or not there is a consensus among the given assignments
    that the image shown was not a rate card.

    :param assignments: List of assignments
    :type assignments: list
    :rtype: bool
    """
    num_without_rates = sum([
        1 for each in assignments if each.does_not_contain_rates])
    return num_without_rates >= 2


def has_consensus_on_rates(assignments):
    """Indicates whether or not the given assignments have a consensus on rates.

    :param assignments: A list of assignments
    :type assignments: list
    :rtype: bool
    """
    parsed_assignments = []
    for each in assignments:
        if not each.rates:
            continue

        try:
            result = ratecard_models.ParseResult.get_for_assignment(each)
            # Has results and exactly one result per line
            original_lines = each.rates.splint('\r\n')
            parsed_lines = filter(None, result.parsed_rates)
            if (parsed_lines and len(parsed_lines) == len(original_lines)):
                parsed_assignments.append(result)
        except ratecard_models.ParseFailedException:
            continue

    if len(parsed_assignments) < 2:
        return False

    num_matches = 0
    for (lhs, rhs) in itertools.combinations(parsed_assignments, 2):
        if set(lhs.parsed_rates) == set(rhs.parsed_rates):
            num_matches += 2
    return num_matches >= 2


def get_consensus_rates(assignments):
    """Parse and return the consensus results for the given assignments.

    :param assignments: A list of assignments
    :type assignments: list
    :rtype: parkme.ratecard.models.ParseResult
    """
    parsed_assignments = []
    for each in assignments:
        if not each.rates:
            continue

        try:
            result = ratecard_models.ParseResult.get_for_assignment(each)
            # Has results and exactly one result per line
            if result and len(result.rates) == len(each.rates.split('\r\n')):
                parsed_assignments.append(result)
        except ratecard_models.ParseFailedException:
            continue

    if len(parsed_assignments) < 2:
        return None

    for (lhs, rhs) in itertools.combinations(parsed_assignments, 2):
        if set(lhs.parsed_rates) == set(rhs.parsed_rates):
            return lhs.parsed_rates

    return None


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: process_api_results.py [BATCH_ID]"
        print "Attempt to validate results from the given Mechanical Turk"
        print "batch."
        exit(1)

    batch_id = int(sys.argv[1])

    mturk_connection = connection.MTurkConnection(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    assignment_gateway = assignments.AssignmentGateway.get(mturk_connection)
    all_assignments = assignment_gateway.get_by_batch_id(
        batch_id, assignments.RateTranscriptionAssignment)

    hit_id_to_assignments = collections.defaultdict(list)
    for each in all_assignments:
        hit_id_to_assignments[each.hit_id].append(each)

    hit_ids_without_rate_card = set([])
    hit_ids_with_consensus = set([])
    hit_ids_without_consensus = set([])

    for hit_id, assignments in hit_id_to_assignments.iteritems():
        if len(assignments) == 3:
            if has_consensus_not_rate_card(assignments):
                hit_ids_without_rate_card.add(hit_id)
            elif has_consensus_on_rates(assignments):
                hit_ids_with_consensus.add(hit_id)
            else:
                hit_ids_without_consensus.add(hit_id)

    num_hits = (
        len(hit_ids_without_rate_card) +
        len(hit_ids_with_consensus) +
        len(hit_ids_without_consensus))
    num_hits_with_rates = (
        len(hit_ids_with_consensus) + len(hit_ids_without_consensus))
    print
    print 'FINAL RESULTS'
    print '{} HITs Total'.format(num_hits)
    print '{} Consensus, {} Not Rates, {} No Consensus'.format(
        len(hit_ids_with_consensus),
        len(hit_ids_without_rate_card),
        len(hit_ids_without_consensus))
    print 'Effectiveness: {:0.02f}%'.format(
        len(hit_ids_with_consensus) / float(num_hits_with_rates) * 100.0)

    print
    print 'CONSENSUS HIT IDS'
    for hit_id in hit_ids_with_consensus:
        print hit_id
        print get_consensus_rates(hit_id_to_assignments[hit_id])
