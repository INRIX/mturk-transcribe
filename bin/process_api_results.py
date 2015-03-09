import sys
sys.path.append('')

import collections
import itertools
import sqlite3

from boto.mturk import connection
import termcolor

import parse_turk_results
from parkme import models
from parkme.ratecard import parser
from parkme.ratecard import models as ratecard_models
from parkme import settings
from parkme.turk import assignments
from parkme.turk import hits


def get_consensus_results(results):
    """Get the consensus results from the parser if available.

    :param results: A list of parsed results
    :type results: list of turk.ratecard.models.ParseResult
    :rtype: str or unicode
    """
    for (lhs, rhs) in itertools.combinations(results, 2):
        if set(lhs.parsed_rates) == set(rhs.parsed_rates):
            return lhs
    return None


def has_consensus_on_parser_results(results):
    """Indicates whether or not the given results contain a consensus.

    :param results: A list of parsed results
    :type results: list of turk.ratecard.models.ParseResult
    :rtype: bool
    """
    num_matches = 0
    for (lhs, rhs) in itertools.combinations(results, 2):
        if set(lhs.parsed_rates) == set(rhs.parsed_rates):
            num_matches += 2
    return num_matches >= 2


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: process_api_results.py [BATCH_ID]"
        print "Attempt to validate results from the given Mechanical Turk"
        print "batch."
        exit(1)

    batch_id = int(sys.argv[1])

    transcribed_rate_gateway = models.TranscribedRateDataGateway('results.db')
    transcribed_rate_gateway.create_table()
    manual_review_gateway = models.ManualReviewDataGateway('results.db')
    manual_review_gateway.create_table()

    rejected_hits = collections.defaultdict(list)
    accepted_hits = collections.defaultdict(list)
    assets_without_rates = set([])
    assignment_to_results = {}

    hit_ids = set([])
    accepted_hit_ids = set([])

    mturk_connection = connection.MTurkConnection(
       aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
       aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    assignment_gateway = assignments.AssignmentGateway.get(mturk_connection)
    all_assignments = assignment_gateway.get_by_batch_id(
        batch_id, assignments.RateTranscriptionAssignment)

    for each in all_assignments:
        hit_ids.add(each.hit_id)

        if not each.rates:
            if each.does_not_contain_rates:
                assets_without_rates.add(each.asset_id)
            continue

        try:
            parse_result = ratecard_models.ParseResult.get_for_assignment(each)
        except ratecard_models.ParseFailedException as pfe:
            rejected_hits[each.hit_id].append(each.assignment_id)
            parse_turk_results.print_rate_results(
                each.hit_id, each.worker_id, each.rates)
            continue

        accepted_hits[each.hit_id].append(each)
        assignment_to_results[each] = parse_result
        parse_turk_results.print_rate_results(
            each.hit_id, each.worker_id, each.rates)

    for hit_id, assignments in rejected_hits.iteritems():
        if len(assignments) >= 2:
            print termcolor.colored('POTENTIAL TOO DIFFICULT: {}'.format(hit_id), 'green')
            manual_review = models.ManualReview(
                hit_id=hit_id, batch_id=batch_id)
            manual_review_gateway.save(manual_review)
        print termcolor.colored('REJECTED {}'.format(hit_id), 'red')

    print 'ASSETS NOT CONTAINING RATES'
    for asset_id in assets_without_rates:
        print asset_id

    for hit_id, assignments in accepted_hits.iteritems():
        if len(assignments) == 3:
            assignment_results = [
                assignment_to_results[each]
                for each in assignments]
            results = []
            accepted_hit_ids.add(hit_id)

            if has_consensus_on_parser_results(assignment_results):
                results = get_consensus_results(assignment_results)
            else:
                print
                print termcolor.colored(
                    'RESULT MISMATCH: {}'.format(hit_id), attrs=['bold'])
                manual_review = models.ManualReview(
                    hit_id=hit_id, batch_id=batch_id)
                manual_review_gateway.save(manual_review)
                continue

            print
            print termcolor.colored('Accepted Assignment', attrs=['bold'])
            print termcolor.colored('HITId: {}'.format(hit_id), attrs=['bold'])
            print termcolor.colored(
                'AssignmentId: {}'.format(results.assignment.assignment_id),
                attrs=['bold'])

            print results.rates_str

            new_rate = models.TranscribedRate(
                hit_id=hit_id,
                batch_id=batch_id,
                lot_id=results.assignment.lot_id,
                rates=results.rates_str,
                user_notes=results.notes_str)
            transcribed_rate_gateway.save(new_rate)

            for each in assignments:
                assignment_gateway.accept(assignment_to_results[each].assignment)

    print '{} Accepted, {} Total ({:0.02f}%)'.format(
        len(accepted_hit_ids) + len(assets_without_rates),
        len(hit_ids),
        (len(accepted_hit_ids) / float(len(hit_ids))) * 100.0)
