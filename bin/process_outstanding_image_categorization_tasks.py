# -*- coding: utf-8 -*-
"""
    process_outstanding_image_categorization_tasks
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Processes the remaining image categorization tasks.

    Copyright (C) 2015 ParkMe Inc. All Rights Reserved
"""
from process_lot_image_categorization_results_from_api import process_results

from parkme import models


def process_outstanding_batches():
    """Process the outstanding image categorization batches."""
    data_gateway = models.CategorizationBatchDataGateway('db.sqlite3')
    for each in data_gateway.get_all_unfinished():
        if process_results(each.categorization_batch_id):
            updated_each = models.CategorizationBatch(
                categorization_batch_id=each.categorization_batch_id,
                newest_photo_timestamp=each.newest_photo_timestamp,
                created_at=each.created_at,
                num_photos=each.num_photos,
                is_finished=True
            )
            data_gateway.save(updated_each)


if __name__ == "__main__":
    process_outstanding_batches()
