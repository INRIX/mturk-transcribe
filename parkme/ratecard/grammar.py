# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
    parkme.ratecard.grammar
    ~~~~~~~~~~~~~~~~~~~~~~~
    Contains a grammar to take transcribed rates and convert them to
    preferred formats.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import pyparsing


def debug_parts(t):
    """Useful for debugging"""
    print t
    return t


def final_join(t):
    """Final joining together of all parsed components"""
    return ' '.join(t) + ':'


def conjoin_parts(t):
    """Conjoin parts of an In (After|Before) Rate"""
    return "(In {} {})".format(t[1].capitalize(), t[2])


def decimalize_price(t):
    """Return a price with two numbers after the decimal point."""
    return "{0:.2f}".format(float(t[0]))


# Numbers
lone_number = pyparsing.Word(pyparsing.nums)
number_range_form = pyparsing.Combine(
    lone_number + "-" + lone_number,
    adjacent=False)
price_number_form = pyparsing.Regex(
    r'[0-9]+(\.[0-9]{2})?').setParseAction(decimalize_price)


# Times / Dates
hour = pyparsing.Regex(r'(H|h)(ou)?r').setParseAction(
    pyparsing.replaceWith('Hour'))
hours = pyparsing.Regex(r'(H|h)(ou)?rs').setParseAction(
    pyparsing.replaceWith('Hours'))
minute = pyparsing.Regex(r'(M|m)in(ute)?').setParseAction(
    pyparsing.replaceWith('Min'))
minutes = pyparsing.Regex(r'(M|m)in(ute)?s').setParseAction(
    pyparsing.replaceWith('Mins'))


hour_forms = pyparsing.Or([hour, hours])
minute_forms = pyparsing.Or([minute, minutes])
time_forms = pyparsing.Or([hour_forms, minute_forms])
timestamp_form = pyparsing.Combine(
    pyparsing.Or(lone_number, lone_number + ":" + lone_number)
    + pyparsing.Regex('(A|a|P|p)(M|m)'))
duration_form = pyparsing.Or([
    timestamp_form,
    pyparsing.Regex(r'(N|n)oon').setParseAction(pyparsing.replaceWith("12PM")),
    pyparsing.Regex(r'Midnight').setParseAction(pyparsing.replaceWith("12AM"))
])


evenings = pyparsing.Regex(r'(E|e)vening(s)?')
nights = pyparsing.Regex(r'(N|n)ight(s)?')


monday_forms = pyparsing.Regex(r'(M|m)on(day)?').setParseAction(
    pyparsing.replaceWith('Mon'))
tuesday_forms = pyparsing.Regex(r'(T|t)ue(s|sday)?').setParseAction(
    pyparsing.replaceWith('Tues'))
wednesday_forms = pyparsing.Regex(r'(W|d)ed(s|nesday)?').setParseAction(
    pyparsing.replaceWith('Wed'))
thursday_forms = pyparsing.Regex(r'(T|t)hur(s|sday)?').setParseAction(
    pyparsing.replaceWith('Thurs'))
friday_forms = pyparsing.Regex(r'(F|f)ri(day)?').setParseAction(
    pyparsing.replaceWith('Fri'))
saturday_forms = pyparsing.Regex(r'(S|s)at(urday)?').setParseAction(
    pyparsing.replaceWith('Sat'))
sunday_forms = pyparsing.Regex(r'(S|s)un(day)?').setParseAction(
    pyparsing.replaceWith('Sun'))
day_names = [
    monday_forms, tuesday_forms, wednesday_forms,
    thursday_forms, friday_forms, saturday_forms, sunday_forms
]
day_name_form = pyparsing.Or(day_names)
day_range_form = day_name_form + "-" + day_name_form
day_name_or_range_form = pyparsing.Combine(
    pyparsing.Or([day_name_form, day_range_form]),
    adjacent=False
)
in_after_before_form = (
    pyparsing.Regex(r'\(\s*((I|i)n)?') +
    pyparsing.Regex(r'((A|a)fter|(B|b)efore)') +
    duration_form +
    ")").setParseAction(conjoin_parts)
time_range_form = pyparsing.Combine(
    "(" +
    pyparsing.Or([
        duration_form,
        duration_form + "-" + duration_form]) +
    ")", adjacent=False)
parenthetical_form = pyparsing.Or([in_after_before_form, time_range_form])
day_range_after_before_form = (
    day_name_or_range_form + pyparsing.Optional(parenthetical_form))


# Daily Maximum
day_forms = pyparsing.Regex(r'(D|d)ay(s)?')
daily_forms = pyparsing.Regex(r'(D|d)aily')
maximum_forms = (
    pyparsing.Optional(daily_forms) +
    pyparsing.Regex(r'(M|m)ax(imum)?') +
    pyparsing.Optional(daily_forms))


# Currencies
currency_form = pyparsing.Or([pyparsing.Word('$')])
price_form = pyparsing.Combine(currency_form + price_number_form)


# Rate Types
hourly_rate = (
    pyparsing.Or([lone_number, number_range_form]) +
    pyparsing.Or([hour, hours]))
evening_forms = (
    pyparsing.Or([evenings, nights]).setParseAction(
        pyparsing.replaceWith('Evening'))
    + pyparsing.Optional(parenthetical_form))
daily_max_forms = pyparsing.Or([day_forms, maximum_forms]).setParseAction(
    pyparsing.replaceWith('Daily Max'))
flat_rate = pyparsing.Regex(r'(F|f)lat\s+(R|r)ate').setParseAction(
    pyparsing.replaceWith('Flat Rate'))
each_n = pyparsing.Combine(
    pyparsing.Regex(r'(E|e)ach').setParseAction(
        pyparsing.replaceWith('Each')) +
    pyparsing.Word(pyparsing.nums) +
    time_forms,
    joinString=' ',
    adjacent=False)
early_bird = pyparsing.Regex(r'(E|e)arly\s+(B|b)ird')
weekend = (
    pyparsing.Regex(r'(W|w)eekend(s)?').setParseAction(
        pyparsing.replaceWith('Sat-Sun'))
    + pyparsing.Optional(parenthetical_form))


# Rate Cards
rate_types = [
    hourly_rate, evening_forms, flat_rate,
    each_n, daily_max_forms, early_bird,
    weekend, day_range_after_before_form
]
rate_card = (
    pyparsing.Or(rate_types).setParseAction(final_join) +
    pyparsing.Optional(pyparsing.Word("::")).suppress() +
    price_form)
