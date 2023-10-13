---
layout: sub-navigation
order: 9
title: Using high watermarks
---

This guide is for developers using pg-bulk-ingest to make pipelines who have already read [Get started with Airflow](get-started.md)

The “high watermark” value of the batches function is how state is transferred from one run of the ingest function to the next, in order to avoid re-ingesting the same data as the previous run. This is what makes the pipelines “incremental”.

It’s very flexible, but that flexibility means it’s easy to do the wrong thing, so care must be taken. As per [Get started with Airflow](get-started.md), it’s usually recommended to ingest a small amount of data without using the high watermarking behaviour first, and then add this only if required.

## The lifecycle of a high watermark

```python
def batches(high_watermark):

    yield new_high_watermark, ..., ...
```

It’s yielded with each batch, and it committed to the database if and only if the data for the batch is committed to the database. Put another way, the saving of high watermark and data for a batch is atomic - both happen, or neither happen. 

Then on the next call to ingest the most recently committed high watermark is passed back into the batches function.

Which is then expected to yield batches of data after the high watermark. What “after” means is source-specific, and how this is ingested into the database is specific to the pipeline, as described below.

## Classes of ingest

There are roughly 3 classes of ingest possible.

1. Replace all existing data, but only if it’s changed

To do this pass delete=Delete.BEFORE_FIRST_BATCH into the ingest function, and if the data source has changed, yield it in a single batch

2. Upsert existing data based on primary key

To do this, ensure the table definition has a primary key, and upsert=Upsert.IF_PRIMARY_KEY is passed to the ingest function.

These can also be customised to delete data.

3. Always add to existing data

To do this, ensure upsert=Upsert.OFF is passed to the ingest function, or make sure the table does not have a primary key.

 

 

 