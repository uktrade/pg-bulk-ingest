---
layout: sub-navigation
order: 3
title: API
---


The API is a single function `ingest`, together with classes of string constants: `HighWatermark`, `Visibility`, `Upsert` and `Delete`. The constants are known strings rather than opaque identifiers to allow the strings to be easily passed from dynamic/non-Python environments.

---

`ingest`(conn, metadata, batches, on_before_visible=lambda conn, latest_batch_metadata: None, high_watermark=HighWatermark.LATEST, visibility=Visibility.AFTER_EACH_BATCH, upsert=Upsert.IF_PRIMARY_KEY, delete=Delete.OFF)

Ingests data into a table

- `conn` - A [SQLAlchemy connection](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection) not in a transaction, i.e. started by `connection` rather than `begin`.

- `metadata` - A SQLAlchemy metadata of a single table.

- `batches` - A function that takes a high watermark, returning an iterable that yields data batches that are strictly after this high watermark. See Usage above for an example.

- `on_before_visible` (optional) - A function that takes a SQLAlchemy connection in a transaction and batch metadata, called just before data becomes visible to other database clients. See Usage above for an example.

- `high_watermark` (optional) - A member of the `HighWatermark` class, or a JSON-encodable value.

    If this is `HighWatermark.LATEST`, then the most recent high watermark that been returned from a previous ingest's `batch` function whose corresponding batch has been succesfully ingested is passed into the `batches` function. If there has been no previous ingest, `None` will be passed.

    If this is `HighWatermark.EARLIEST`, then None will be passed to the batches function as the high watermark. This would typically be used to re-ingest all of the data.

    If this a JSON-encodable value other than `HighWatermark.LATEST` or `HighWatermark.EARLIEST`, then this value is passed directly to the `batches` function. This can be used to override any previous high-watermark. Existing data in the target table is not deleted unless specified by the `delete` parameter.

- `visibility` (optional) - A member of the `Visibilty` class, controlling when ingests will be visible to other clients.

- `upsert` (optional) - A member of the `Upsert` class, controlling whether an upsert is performed when ingesting data

- `delete` (optional) - A member of the `Delete` class, controlling if existing rows are to be deleted.

---

`HighWatermark`

A class of constants to indicate what high watermark should be passed into the batches function.

- `LATEST` - pass the most recent high watermark yielded from the batches function from the previous ingest into the batches function. If there is no previous ingest, the Python value `None` is passed. This is the string `__LATEST__`.

- `EARLIEST` - pass the Python value `None` into the batches function. This is the string `__EARLIEST__`.

---

`Visibility`

A class of constants to indicate when data changes are visible to other database clients. Schema changes become visible before the first batch.

- `AFTER_EACH_BATCH` - data changes are visible to other database clients after each batch. This is the string `__AFTER_EACH_BATCH__`.

---

`Delete`

A class of constants that controls how existing data in the table is deleted

- `OFF`

   There is no deleting of existing data. This is the string `__OFF__`.

- `BEFORE_FIRST_BATCH`

   All existing data in the table is deleted just before the first batch is ingested. If there are no batches, no data is deleted. This is the string `__BEFORE_FIRST_BATCH__`.

---

`Upsert`

A class of constants that controls if upserting is performed

- `OFF`

   No upserting is performed. This is the string `__OFF__`.

- `IF_PRIMARY_KEY`

   If the table contains a primary key, an upsert based on that primary key is performed on ingest. If there is no primary key then a plain insert is performed. This is useful to avoid duplication if batches overlap. This is the string `__IF_PRIMARY_KEY__`.

