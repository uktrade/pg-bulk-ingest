---
layout: sub-navigation
order: 1
title: Get started
---


## Prerequisites

Python 3.7+


## Installation
`pg-bulk-ingest` can be installed from  PyPI using `pip`. `psycopg2` or `psycopg` (Psycopg 3) must also be explicitly installed.

```
pip install pg-bulk-ingest psycopg
```


## Example

Ensure you have a PostgreSQL instance running. For example to test with Docker locally, on the command line run:

```bash
docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
```

Then in Python create a PostgreSQL table and ingest hard-coded data into it using the `ingest` function:

```python
import sqlalchemy as sa
from pg_bulk_ingest import HighWatermark, Visibility, Upsert, Delete, ingest

# This should work with the Docker example above, but would have to be changed
# if using a PostgreSQL instance elsewhere or setup differently
engine = sa.create_engine('postgresql+psycopg://postgres@127.0.0.1:5432/')

# A SQLAlchemy Metadata of a single table definition
metadata = sa.MetaData()
my_table = sa.Table(
    "my_table",
    metadata,
    sa.Column("id", sa.INTEGER, primary_key=True),
    sa.Column("value", sa.VARCHAR(16), nullable=False),
    schema="my_schema",
)

# A function that yields batches of data, where each is a tuple of
# (high watermark, batch metadata, data rows).
# - The batches should all be _after_ the high watermark passed into the function
# - Each high watermark must be JSON-encodable, or a callable that returns a
#   JSON-encodable value that is called after the data rows are iterated over
# - Each row must have the SQLAlchemy table associated with it
def batches(high_watermark):
    batch_high_watermark = '2015-01-01'
    if high_watermark is None or batch_high_watermark > high_watermark:
        yield batch_high_watermark, 'Any batch metadata', (
            (my_table, (3, 'a')),
            (my_table, (4, 'b')),
            (my_table, (5, 'c')),
        )

    batch_high_watermark = '2015-01-02'
    if high_watermark is None or batch_high_watermark > high_watermark:
        yield batch_high_watermark, 'Any other batch metadata', (
            (my_table, (6, 'd')),
            (my_table, (7, 'e')),
            (my_table, (8, 'f')),
        )

def on_before_visible(conn, ingest_table, batch_metadata):
    # Can perform validation or update metadata table(s) just before data
    # is visible to other database clients
    # conn: is a SQLAlchemy connection in the same transaction as this batch
    # ingest_table: the SQLAlchemy that data was ingested into
    # batch_metadata: the metadata for the most recent batch from the batches function

with engine.connect() as conn:
    ingest(
        conn, metadata, batches,
        on_before_visible=on_before_visible,
        high_watermark=HighWatermark.LATEST,     # Carry on from where left off
        visibility=Visibility.AFTER_EACH_BATCH,  # Changes are visible after each batch
        upsert=Upsert.IF_PRIMARY_KEY,            # Upsert if there is a primary key
        delete=Delete.OFF,                       # Don't delete any existing rows
    )
```


## Data types

The SQLAlchemy "CamelCase" data types are not supported in table definitions. Instead, you must use types specified with "UPPERCASE" data types. These are non-abstracted database-level types. This is to support automatic migrations - the real database type is required in order to make a comparison with the live table and the one passed into the `ingest` function.

Also not supported is the sqlalchemy.JSON type. Instead use `sa.dialects.postgresql.JSON` or `sa.dialects.postgresql.JSONB`.


## Indexes

Indexes can be added by passing `sqlalchemy.Index` objects after the column list when defining the table. The name of each index should be `None` - pg-bulk index chooses a random name so it does not conflict with other indexes.

```python
sa.Table(
    "my_table",
    metadata,
    sa.Column("id", sa.INTEGER, primary_key=True),
    sa.Column("value", sa.VARCHAR(16), nullable=False),
    sa.Index(None, "value"),
    schema="my_schema",
)
```


## Compatibility

- Python >= 3.7.1 (tested on 3.7.1, 3.8.0, 3.9.0, 3.10.0, and 3.11.0)
- psycopg2 >= 2.9.2 or Psycopg 3 >= 3.1.4
- SQLAlchemy >= 1.4.24 (tested on 1.4.24 and 2.0.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, and 16 Beta 2)

Note that SQLAlchemy < 2 does not support Psycopg 3, and for SQLAlchemy < 2 `future=True` must be passed to `create_engine`.
