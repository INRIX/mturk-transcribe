import collections
import sqlite3
import sys

from boto.mturk import connection
import termcolor

import parse_turk_results
from parkme import models
from parkme.ratecard import parser
from parkme.ratecard import models as ratecard_models
from parkme.turk import assignments
from parkme.turk import hits


# AWS Credentials
AWS_ACCESS_KEY_ID = 'AKIAJFLF5ZQRGKEN3WCQ'
AWS_SECRET_ACCESS_KEY = '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'


def parser_results_are_equal(results_a, results_b):
    """Indicate whether or not parser results are equal.
    
    :param results_a: Left-hand results
    :type results_a: list
    :param results_b: Right-hand results
    :type results_b: list
    :rtype: bool
    """
    return set(results_a) == set(results_b)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: process_api_results.py [BATCH_ID]"
        print "Attempt to validate results from the given Mechanical Turk"
        print "batch."
        exit(1)

    batch_id = int(sys.argv[1])

    transcribed_rate_gateway = models.TranscribedRateDataGateway('results.db')
    transcribed_rate_gateway.create_table()

    rejected_hits = collections.defaultdict(list)
    accepted_hits = collections.defaultdict(list)
    assignment_to_results = {}

    mturk_connection = connection.MTurkConnection(
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    assignment_gateway = assignments.AssignmentGateway.get(mturk_connection)

    for each in assignment_gateway.get_by_batch_id(batch_id):
        if not each.rates:
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

    for hit_id, assignments in accepted_hits.iteritems():
        if len(assignments) == 2:
            print
            print parser_results_are_equal(
                assignment_to_results[assignments[0]].parsed_rates,
                assignment_to_results[assignments[1]].parsed_rates)
            print termcolor.colored('Accepted Assignment', attrs=['bold'])
            print termcolor.colored('HITId: {}'.format(hit_id), attrs=['bold'])
            print termcolor.colored(
                'AssignmentId: {}'.format(assignments[0].assignment_id),
                attrs=['bold'])

            results = assignment_to_results[assignments[0]]
            print results.rates_str

            new_rate = models.TranscribedRate(
                hit_id=hit_id,
                batch_id=batch_id,
                rates=results.rates_str,
                user_notes=results.notes_str)
            transcribed_rate_gateway.save(new_rate)
