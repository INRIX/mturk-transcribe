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

def parse_or_reject_line(line):
    """Parse the given line, indicate a rejection if parsing fails.

    :param line: A single text line
    :type line: str or unicode
    :rtype: str or unicode or False
    """
    try:
        return parser.convert_to_parkme_format(line)
    except parser.RateCardParsingException:
        return False


def turk_answers_from_file(file_path):
    """Generator that yields successive lines from a turk answer file.

    :param file_path: The path to the CSV file
    :type file_path: str or unicode
    """
    with open(file_path, 'rb') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = csvreader.next()
        for row in csvreader:
            yield dict(zip(headers, row))


def parse_or_reject_answers(answers):
    """Parse or reject answers on all lines in the given string.

    :param answers: String of answers separated by newline
    :type answers: str or unicode
    :return: List of inputs and outputs for each line and bool indicating
        whether the parse succeeded or failed.
    :rtype: (list of input and parsed output, bool)
    """
    lines = answers.split('\r\n')
    results = []
    for each in lines:
        results.append((each, parse_or_reject_line(each)))
    rejected = any([v is False for k, v in results])
    return results, rejected


def print_rate_results(hit_id, worker_id, rates):
    """Print the rate results to the console.

    :param hit_id: The HIT ID
    :type hit_id: str or unicode
    :param worker_id: The Worker ID
    :type worked_id: str or unicode
    :param rates: String of rates on separate lines
    :type rates: str or unicode
    """
    lines, rejected = parse_or_reject_answers(rates)
    rejected_str = (
        termcolor.colored("REJECTED", 'red')
        if rejected
        else termcolor.colored("ACCEPTED", 'green')
    )
    print hit_id, worker_id, rejected_str
    for inp, out in lines:
        print inp, '->', out
    print


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: ./parse_turk_results.py [CSVFILE]"
        print "Prints the result of parsing each sample in the CSV file"
        exit(1)

    for answer in turk_answers_from_file(sys.argv[1]):
        print_rate_results(
            answer['HITId'],
            answer['WorkerId'],
            answer['Answer.Rates'])
