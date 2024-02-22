---
layout: sub-navigation
order: 9
title: Using high-watermarks
---

<div class="govuk-warning-text">
  <span class="govuk-warning-text__icon" aria-hidden="true">!</span>
  <strong class="govuk-warning-text__text">
    <span class="govuk-visually-hidden">Warning</span>
    High-watermarks can lead to lost or duplicated data if not used properly.
  </strong>
</div>

The high-watermark is a small piece of metadata that indicates how far an ingest has progressed. It is used on the next ingest to resume from that point. If used properly, it can offer a performance benefit over repeatedly re-ingesting the same data.

There are risks of either losing data or duplicating data when using high-watermarks. Subtle and often not well-documented properties of data sources can affect what you need to do to make an high-watermarked ingest robust. What works in one situation may not work in another.

> It's recommended to only use a high-watermark after you have written the ingest in a way that doesn't use a high-watermark. In this way you gain evidence that it's worth the effort and risk.


## The lifecycle of a high-watermark

The batches function is how data is supplied to pg-bulk-ingest for ingestion into the database. The high-level structure of a batches function is usually:

```python
def batches(high_watermark):

    source_batches = fetch_source_batches(...)

    for source_batch in source_batches:

        new_high_watermark = ...
        batch_metadata = ...
        batch_data = ...

        yield new_high_watermark, batch_metadata, batch_data
```

The high-watermark is yielded as part of each batch in the batches function. In the code above this is the `new_high_watermark` variable. It is committed to the database if and only if the data for the batch is committed to the database.

On the next ingest the most recently committed high-watermark is passed back into the batches function. In the code above this is the `high_watermark` variable. Your code in the batches function can then use it to pick up the ingest from where it left off last time.

On the first ingest `high_watermark` is `None`.


## Properties of a high-watermark

A high-watermark is highly dependant on the properties of the source system. Specifically, in order to be a high-watermark that allows incremental ingests that doesn't duplicate data but also doesn't miss any data:

1. The source system has to offer a way of fetching data that is "after" any given value of the high watermark.
2. The source system should not be able to subsequently release data "before" a high watermark.

For example, consider the case of retrieving data from a source system that supplies data in daily batches, and where the source system offers:

1. A way to fetch data from a specific date onwards
2. A guarentee that once a day of data is released then that data will never change

Then the date associated with each batch of data would be a good choice for high watermark.

The high-level code of a batches function in this case would be:

```python
def batches(high_watermark):

    source_batches_after_high_watermark = fetch_source_batches_after(high_watermark)

    for daily_source_batch in source_batches_after_high_watermark:

        new_high_watermark = date_associated_with(daily_source_batch)
        ...

        yield new_high_watermark, ..., ...
```

pg-bulk-ingest supports any JSON-encodable value as the high watermark, for example variables of type `str`.

> Python date and datetime objects are not JSON-encodable. If you wish to sue either of them as a high-watermark, you must convert them to a JSON-encodable value, for example by passing them through the `str` function.


## Avoid using the local time

In most case the current date or time according to the computer running the ingest should not be used as the high-watermark. This is because it introduces a dependency on this time matching the time of the source system. This increases the risks of either missing or duplicating data.

Instead, some property of the source data should be used as the high watermark, for example a date or time.


## Date and times can be repeated, or even go backwards

While a date or time supplied by the source system is often the best choice for high-watermark, a suprisingly high number of source system do not guarentee that data is released with a date or time that is stricly later than the date or time of all previously released data. In some cases dates and times can even be released that are ealier than previously released data, and so time can appear to go backwards.

For example, the [date/time functions in PostgreSQL](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT) do not offer a guarantee that once a datetime is visible to clients that rows earlier than that time will never subsequently be visible to clients.

Similarly AWS S3 objects with a [last modified](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#AmazonS3-GetObject-response-header-LastModified) time earlier than all previously visible objects can appear in a bucket. This is most obvious with objects created using multipart uploads, where the last modified time is closely aligned with when the upload began, rather than when it completed and became visible to clients.

If the system you're ingesting from suffers from issues such as these, you should re-ingest a small amount of data every ingest, while making sure to also use a mechanism to de-deuplicate. For example:

1. Configure the table you're ingesting into to have a primary key.
2. Pass `upsert=Upsert.IF_PRIMARY_KEY` to `ingest` to upsert based on this key.
3. On every ingest, re-ingest some amount of data from _before_ any existing high watermark onwards. The amount should be enough so you're sure that from the point of view of the source, time could not have gone backwards further.

You may have to make a judgement call on what's appropriate in each case because the source system does not document how far backwards time can go.
