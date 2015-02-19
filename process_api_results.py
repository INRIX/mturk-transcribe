import collections
import sqlite3
import sys

from boto.mturk import connection

import parse_turk_results
from parkme.ratecard import parser
from parkme.turk import assignments
from parkme.turk import hits


# AWS Credentials
AWS_ACCESS_KEY_ID = 'AKIAJFLF5ZQRGKEN3WCQ'
AWS_SECRET_ACCESS_KEY = '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'


SIMILAR_ANSWERS = [
    ['Each 15 Minutes :: $2.00', 'Maximum :: $14'],
    ['Daily Max :: $14', 'Each 15 Mins :: $2']
]


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


def create_sqlite_connection(dbfile):
    """Create and return a new SQLite3 connection.

    :param dbfile: The database file
    :type dbfile: str or unicode
    :rtype: sqlite3.Connection
    """
    dbconn = sqlite3.connect(dbfile)

    cursor = dbconn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rates
    (hit_id TEXT PRIMARY KEY, result TEXT)
    ''')
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS rates_hit_id_idx ON rates (hit_id);
    ''')
    dbconn.commit()

    return dbconn


def write_rates(sqlite_conn, hit_id, rates):
    cursor = sqlite_conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO rates VALUES (?, ?)", (hit_id, rates))
    sqlite_conn.commit()


# Criteria for acceptance
# 1. The result parsed correctly
# 2. At least 1 other turk result also parsed correctly
# Then all correctly parsed results will be accepted.


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: api_test.py [BATCH_ID]"
        print "Attempt to validate results from the given Mechanical Turk"
        print "batch."
        exit(1)

    dbconn = create_sqlite_connection('results.db')

    batch_id = int(sys.argv[1])

    rejected_hits = collections.defaultdict(list)
    accepted_hits = collections.defaultdict(list)
    assignment_to_rates = {}

    conn = connection.MTurkConnection(
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    all_hits = list(conn.get_all_hits())
    hits_in_batch = hits.filter_by_batch_id(all_hits, batch_id)
    all_assignments = assignments.map_hits_to_assignments(hits_in_batch, conn)

    for each in all_assignments:
       rates = assignments.get_answer_to_question(each, 'Rates').fields[0].lower()
       results, rejected = parse_turk_results.parse_or_reject_answers(rates)

       if rejected:
           rejected_hits[each.HITId].append(each.AssignmentId)
       else:
           assignment_to_rates[each.AssignmentId] = '\r\n'.join([t[1] for t in results])
           accepted_hits[each.HITId].append(each.AssignmentId)

       parse_turk_results.print_rate_results(each.HITId, each.WorkerId, rates)

    for hit_id, assignment_ids in accepted_hits.iteritems():
        for aid in assignment_ids:
            print 'Approved {}'.format(aid)
            try:
                conn.approve_assignment(aid, feedback='Approved by automatic transcription parser.')
            except connection.MTurkRequestError as mtre:
                # Assignment already approved
                if mtre.status != 200:
                    raise mtre

    for hit_id, assignment_ids in accepted_hits.iteritems():
        if len(assignment_ids) >= 2:
            print '\nAccepted Assignment ({} - {})'.format(
                hit_id, assignment_ids[0])
            rates = assignment_to_rates[assignment_ids[0]]
            write_rates(dbconn, hit_id, rates)
            print rates

    dbconn.close()
