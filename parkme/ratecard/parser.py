# -*- coding: utf-8 -*-
"""
    parkme.ratecard.parser
    ~~~~~~~~~~~~~~~~~~~~~~
    Contains rate-card parsing and error-handling.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import pyparsing

from parkme.ratecard import grammar
from parkme import exceptions


class RateCardParsingException(exceptions.Error):
    """Indicates that an error occurred during parsing."""
    pass


def parse_rate_card_line(line):
    """Parse a single rate card line and return the ParkMe format.

    Raises RateCardParsingException on error.

    :param line: A single text line
    :type line: str or unicode
    :return: List of tokens parsed from the line
    :rtype: list
    """
    try:
        return grammar.rate_card.parseString(line)
    except pyparsing.ParseException as parse_err:
        raise RateCardParsingException(parse_err)


def convert_to_parkme_format(line):
    """Convert the given line to the ParkMe internal format.

    :param line: A single text line
    :type line: str or unicode
    :rtype: str
    """
    return ' '.join(parse_rate_card_line(line))


def parse_or_reject_line(line):
    """Parse the given line, indicate a rejection if parsing fails.

    :param line: A single text line
    :type line: str or unicode
    :rtype: str or unicode or False
    """
    try:
        return convert_to_parkme_format(line)
    except RateCardParsingException:
        return False
