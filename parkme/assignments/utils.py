# -*- coding: utf-8 -*-
"""
    parkme.assignments.utils
    ~~~~~~~~~~~~~~~~~~~~~~~~
    Miscellaneous utilities to be used across assignments.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""
import collections


def asset_to_image_url(asset_bucket, asset_path):
    """Convert the given asset information into a URL.

    :param asset_bucket: The bucket containing the asset
    :type asset_bucket: str or unicode
    :param asset_path: The path containing the asset
    :type asset_path: str or unicode
    :rtype: str or unicode
    """
    return u'http://{}/{}'.format(asset_bucket, asset_path)


def group_by_attribute(iterable, attribute):
    """Group the items of the given iterable into a dictionary keyed on the
    given attribute.

    :param iterable: An iterable
    :type iterable: iterable
    :param attribute: An attribute
    :type attribute: str or unicode
    :rtype: collections.defaultdict
    """
    results = collections.defaultdict(list)
    for each in iterable:
        results[getattr(each, attribute)] = each
    return results
