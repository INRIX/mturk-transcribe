import collections
import sqlite3
import sys

from boto.mturk import connection
import termcolor

import parse_turk_results
from parkme import models
from parkme.ratecard import parser
from parkme.turk import assignments
from parkme.turk import hits


# AWS Credentials
AWS_ACCESS_KEY_ID = 'AKIAJFLF5ZQRGKEN3WCQ'
AWS_SECRET_ACCESS_KEY = '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'


def parse_all_results(answers):
    """For a given list of answer lines return the parser results from each
    line.

    :param answers: An iterable of answers
    :type answers: iterable
    :rtype: list
    """
    return [parser.parse_or_reject_line(each) for each in answers]


def parser_results_are_equal(results_a, results_b):
    """Indicate whether or not parser results are equal.
    
    :param results_a: Left-hand results
    :type results_a: list
    :param results_b: Right-hand results
    :type results_b: list
    :rtype: bool
    """
    for each in results_a:
        if each not in results_b:
            return False
    return True


# Criteria for acceptance
# 1. The result parsed correctly
# 2. At least 1 other turk result also parsed correctly
# Then all correctly parsed results will be accepted.


def accept_assignments_with_ids(mturk_conn, assignment_ids):
    """Accepts all of the assignments with the given ids.

    :param mturk_conn: A mechanical turk connection
    :type mturk_conn: mturk.connection.Connection
    :param assignment_ids: A list of assignment ids
    :type assignment_ids: list of str or unicode
    """
    for aid in assignment_ids:
        print 'Approved {}'.format(aid)
        try:
            mturk_conn.approve_assignment(
                aid, feedback='Approved by automatic transcription parser.')
        except connection.MTurkRequestError as mtre:
            # Assignment already approved
            if mtre.status != 200:
                raise mtre


def reject_assignments_with_ids(mturk_conn, assignment_ids):
    """Rejects all of the assignments with the given ids.

    :param mturk_conn: A mechanical turk connection
    :type mturk_conn: mturk.connection.Connection
    :param assignment_ids: A list of assignment ids
    :type assignment_ids: list of str or unicode
    """
    for aid in assignment_ids:
        print termcolor.colored('REJECTED {}'.format(aid), 'red')
        try:
            mturk_conn.reject_assignment(
                aid, feedback='Rejected by automatic transcription parser.')
        except connection.MTurkRequestError as mtre:
            # Assignment already rejected
            if mtre.status != 200:
                raise mtre


def get_all_assignments(conn):
    """Return all the available assignments on mechanical turk.

    :param conn: A Mechanical Turk connection
    :type conn: boto.mturk.MTurkConnection
    :rtype: iterable
    """
    all_hits = list(conn.get_all_hits())
    hits_in_batch = hits.filter_by_batch_id(all_hits, batch_id)
    return assignments.map_hits_to_assignments(hits_in_batch, conn)


def get_rates(assignment):
    """Return the rates answer associated with the given assignment.

    :param assignment: An assignment
    :type assignment: boto.mturk.Assignment
    :rtype: str or unicode or None
    """
    rate_answers = assignments.get_answer_to_question(assignment, 'Rates')
    if rate_answers and rate_answers.fields:
        return rate_answers.fields[0].lower()
    return None


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: api_test.py [BATCH_ID]"
        print "Attempt to validate results from the given Mechanical Turk"
        print "batch."
        exit(1)

    transcribed_rate_gateway = models.TranscribedRateDataGateway('results.db')
    transcribed_rate_gateway.create_table()

    batch_id = int(sys.argv[1])

    rejected_hits = collections.defaultdict(list)
    accepted_hits = collections.defaultdict(list)
    assignment_to_rates = {}
    assignment_to_notes = {}

    conn = connection.MTurkConnection(
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    all_assignments = get_all_assignments(conn)

    for each in all_assignments:
       rates = get_rates(each)

       if not rates:
           continue

       results, notes, rejected = parse_turk_results.parse_or_reject_answers(
           rates)

       if rejected:
           rejected_hits[each.HITId].append(each.AssignmentId)
       else:
           assignment_to_rates[each.AssignmentId] = '\r\n'.join([
               t[1] for t in results if t[1].strip()])
           assignment_to_notes[each.AssignmentId] = '\r\n'.join([
               t for t in notes])
           accepted_hits[each.HITId].append(each.AssignmentId)

       parse_turk_results.print_rate_results(each.HITId, each.WorkerId, rates)

    for hit_id, assignment_ids in rejected_hits.iteritems():
        #reject_assignments_with_ids(conn, assignment_ids)
        pass

    for hit_id, assignment_ids in accepted_hits.iteritems():
        if len(assignment_ids) >= 2:
            #accept_assignments_with_ids(conn, assignment_ids)
            print
            print termcolor.colored('Accepted Assignment', attrs=['bold'])
            print termcolor.colored('HITId: {}'.format(hit_id), attrs=['bold'])
            print termcolor.colored('AssignmentId: {}'.format(assignment_ids[0]), attrs=['bold'])
            rates = assignment_to_rates[assignment_ids[0]]
            notes = assignment_to_notes[assignment_ids[0]]
            new_rate = models.TranscribedRate(
                hit_id=hit_id, batch_id=batch_id, rates=rates, user_notes=notes)
            transcribed_rate_gateway.save(new_rate)
            print rates
