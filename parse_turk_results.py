import csv
import contextlib
import sys

import termcolor

from parkme.ratecard import parser
from parkme.ratecard import grammar


# TODO(etscrivner): Convert these to test cases
example_rates = [
    "0 - 1 Hour :: $11.00",
    "Max Daily :: $19.00",
    "Evenings :: $11.00",
    "Each 15 Minutes :: $2.00",
    "Daily Maximum ::  $14.00",
    "Mon-Fri :: 2$.00",
    "Weekends :: $5.00",
    "Sat-Sun :: $15.00",
    "Mon-Fri (7AM-Noon) :: $5.25",
    "Mon To Friday 2.00",
    "Mon - Fri :: $5.00",
    "Mon-Fri (after 5PM) :: $5.00",
    "Weekends (After 5PM) :: $11.00",
    "Evenings (After 9PM) :: $21.00",
    "Mon-Friday $2.00"
]

day_range_examples = [
    'Fri',
    'Sat',
    'Sat-Sun',
    'Mon-Thurs',
    'Monday-Thursday'
]


def turk_answers_from_file(file_path):
    """Generator that yields successive lines from a turk answer file.

    :param file_path: The path to the CSV file
    :type file_path: str or unicode
    """
    with open(file_path, 'rb') as csvfile:
        csvreader = csv.DictReader(csvfile)
        return list(csvreader)


def parse_or_reject_answers(answers):
    """Parse or reject answers on all lines in the given string.

    :param answers: String of answers separated by newline
    :type answers: str or unicode
    :return: List of inputs and outputs for each line and bool indicating
        whether the parse succeeded or failed.
    :rtype: (list of input and parsed output, notes, bool)
    """
    lines = answers.split('\r\n')
    # Remove any empty lines
    lines = filter(None, [each.strip() for each in lines])
    results = []
    parser.clear_user_visible_notes()
    for each in lines:
        results.append((each, parser.parse_or_reject_line(each)))
    notes = parser.get_user_visible_notes()
    rejected = any([v is False for k, v in results])
    return results, notes, rejected


def print_rate_results(hit_id, worker_id, rates):
    """Print the rate results to the console.

    :param hit_id: The HIT ID
    :type hit_id: str or unicode
    :param worker_id: The Worker ID
    :type worked_id: str or unicode
    :param rates: String of rates on separate lines
    :type rates: str or unicode
    """
    lines, notes, rejected = parse_or_reject_answers(rates)
    rejected_str = (
        termcolor.colored("REJECTED", 'red')
        if rejected
        else termcolor.colored("ACCEPTED", 'green')
    )
    print hit_id, worker_id, rejected_str
    print termcolor.colored('Rates:', attrs=['bold'])
    for inp, out in lines:
        print inp, '->', out
    if notes:
        print termcolor.colored('Notes:', attrs=['bold'])
        print '\r\n'.join(notes)
    print


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: ./parse_turk_results.py [CSVFILE] [OUTPUT]"
        print "Prints the result of parsing each sample in the CSV file, write"
        print "results to OUTPUT file."
        exit(1)

    csv_file = sys.argv[1]
    output_file = sys.argv[2]

    output_rows = []
    for answer in turk_answers_from_file(csv_file):
        results, rejected = parse_or_reject_answers(answer['Answer.Rates'])
        print_rate_results(
            answer['HITId'],
            answer['WorkerId'],
            answer['Answer.Rates'].lower())

        if rejected:
            answer['Reject'] = 'x'
            answer['RequesterFeedback'] = 'Automated parsing of answer failed.'
        else:
            answer['Approve'] = 'x'
            
        output_rows.append(answer)

    if output_rows:
        with open(output_file, 'wb') as csvfile:
            headers = output_rows[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(output_rows)
