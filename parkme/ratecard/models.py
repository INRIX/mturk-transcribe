# -*- coding: utf-8 -*-
"""
    parkme.ratecard.models
    ~~~~~~~~~~~~~~~~~~~~~~
    Models related to ratecard parsing.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
from parkme import exceptions
from parkme.ratecard import parser


class ParseFailedException(exceptions.Error):
    """Exception raised when parsing fails."""

    def __init__(self, rates, *args, **kwargs):
        super(ParseFailedException, self).__init__(*args, **kwargs)
        self.rates = rates


class ParseResult(object):
    """Container class for parsed results."""

    def __init__(self, assignment, rates, notes):
        """Contains a set of parse results for a given assignment.

        :param assignment: An assignment
        :type assignment: parkme.turk.assignments.Assignment
        :param rates: The result of parsing the individual rate lines
        :type rates: list of str or bool
        :param notes: The notes parsed along with the rates
        :type notes: list of str
        """
        self.assignment = assignment
        self.rates = rates
        self.notes = notes

    @classmethod
    def get_for_assignment(cls, assignment):
        """Get parse results for the given assignment.

        :param assignment: An assignment
        :type assignment: parkme.turk.assignments.Assignment
        :rtype: ParseResult
        """
        if not assignment.rates:
            raise ParseFailedException(assignment.rates)

        rate_lines = assignment.rates.split('\r\n')
        # Remove any empty lines before parsing
        rate_lines = filter(None, [each.strip() for each in rate_lines])

        parser.clear_user_visible_notes()
        rates = [
            (each, parser.parse_or_reject_line(each))
            for each in rate_lines]
        notes = parser.get_user_visible_notes()

        has_rejected_lines = any([line is False for (_, line) in rates])

        if has_rejected_lines:
            raise ParseFailedException(rates)

        return cls(assignment, rates, notes)

    @property
    def parsed_rates(self):
        """Return just the parsed rate results from the rates.

        :rtype: list or str or unicode
        """
        return [each[1] for each in self.rates if each[1].strip()]

    @property
    def rates_str(self):
        """Return the internal rates in a single newline-separated string.

        :rtype: str or unicode
        """
        return '\r\n'.join([each[1] for each in self.rates if each[1].strip()])

    @property
    def notes_str(self):
        """Return the internal notes in a single newline-separated string.

        :rtype: str or unicode
        """
        return '\r\n'.join(self.notes)
