---
layout: sub-navigation
order: 5
title: Under the hood
---

It's an aim of pg-bulk-ingest for clients to not have to worry about internals. However in some cases it is useful to understand what's happening under the hood.

- Ingestion is done exclusively with `COPY FROM`.

- Ingestion is transactional - each batch is ingested completely or not at all

- If there are multiple tables to be ingested into, then data can be buffered in memory, up to a default maximum of 10000 rows per table. To avoid this either `COPY FROM` would have to not be used, which is undesirable for performance reasons, or multiple connections to the database would have to be used, which would not support a single transaction per batch.

- If the table has a primary key and `upsert=IF_PRIMARY_KEY` then an "upsert" is performed. Data is ingested into an intermediate table, and an `INSERT ... ON CONFLICT(...) DO UPDATE` is performed to copy rows from this intermediate table to the existing table. This doesn't involve an ACCESS EXCLUSIVE lock on the live table, so SELECTs can continue in parallel.

- Migrations usually require a long running ACCESS EXCLUSIVE lock on the live table that prevent concurrent SELECTs from progressing. To avoid this, `ingest` creates an intermediate table that matches the required definition, it copies all existing data into this table (unless `delete=Delete.OFF`), and it replaces the live table at the end of the first batch. This replacement requires an ACCESS EXCLUSIVE lock, but only for a short time. Backends that hold locks that block migrations are forcably terminated after a delay using [pg-force-execute](https://github.com/uktrade/pg-force-execute).

- `delete=Delete.BEFORE_FIRST_BATCH` doesn't actually perform a delete. Instead, a new empty table is created that replaces the live table at the end of the first batch just as in the case of migrations.

- If an new table is created due to a migration or `delete=Delete.BEFORE_FIRST_BATCH`:

   - `SELECT` permissions are copied from the old table to the new table.

   - Views (including materialized views) on the old table are dropped and recreated, as well as views on those views and so on. SELECT permissions on these views are also copied from the old views to the new.

- The high watermark is stored on the table as a COMMENT, JSON-encoded. For example if the most recent high watermark is the string `2014-07-31`, then the comment would be `{"pg-bulk-ingest": {"high-watermark": "2014-07-31"}}`.
