# -*- coding: utf-8 -*-
"""
    parkme.assignments.utils
    ~~~~~~~~~~~~~~~~~~~~~~~~
    Miscellaneous utilities to be used across assignments.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved.
"""


def asset_to_image_url(asset_bucket, asset_path):
    """Convert the given asset information into a URL.

    :param asset_bucket: The bucket containing the asset
    :type asset_bucket: str or unicode
    :param asset_path: The path containing the asset
    :type asset_path: str or unicode
    :rtype: str or unicode
    """
    return u'http://{}/{}'.format(asset_bucket, asset_path)
