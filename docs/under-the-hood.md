---
layout: sub-navigation
order: 2
title: Under the hood
---

It's an aim of pg-bulk-ingest for clients to not have to worry about internals. However in some cases it is useful to understand what's happening under the hood.

- Ingestion is done exclusively with `COPY FROM`.

- Ingestion is transactional - each batch is ingested completely or not at all

- If the table has a primary key and `upsert=IF_PRIMARY_KEY` then an "upsert" is performed. Data is ingested into an intermediate table, and an `INSERT ... ON CONFLICT(...) DO UPDATE` is performed to copy rows from this intermediate table to the existing table. This doesn't involve an ACCESS EXCLUSIVE lock on the live table, so SELECTs can continue in parallel.

- Migrations usually require a long running ACCESS EXCLUSIVE lock on the live table that prevent concurrent SELECTs from progressing. To avoid this, `ingest` creates an intermediate table that matches the required definition, it copies all existing data into this table (unless `delete=Delete.OFF`), and it replaces the live table at the end of the first batch. This replacement requires an ACCESS EXCLUSIVE lock, but only for a short time. Backends that hold locks that block migrations are forcably terminated after a delay using [pg-force-execute](https://github.com/uktrade/pg-force-execute).

- `delete=Delete.BEFORE_FIRST_BATCH` doesn't actually perform a delete. Instead, a new empty table is created that replaces the live table at the end of the first batch just as in the case of migrations.

- If an intermediate table is used due to a migration or `delete=Delete.BEFORE_FIRST_BATCH`, any `SELECT` permissions are copied from the old table to the new table.

- The high watermark is stored on the table as a COMMENT, JSON-encoded. For example if the most recent high watermark is the string `2014-07-31`, then the comment would be `{"pg-bulk-ingest": {"high-watermark": "2014-07-31"}}`.
