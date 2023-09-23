---
layout: sub-navigation
order: 1
title: Get started
---

pg-bulk-ingest is a Python package that can be used to ingest large amounts of data into a PostgreSQL database, for example as part of a extract, transform and load (ETL) data pipeline.

This page is a step by step guide to installing pg-bulk-ingest and using it to ingest hard coded data into a local PostgreSQL database.


## Prerequisites

You need the following software to follow this guide:

- [Python 3.7+](https://www.python.org/)
- [Docker](https://www.docker.com/get-started/)
- a text editor or Integrated Development Environment (IDE), for example [VS Code](https://code.visualstudio.com/)

You should also have experience of progamming in Python, using the command line, and an understanding of how PostgreSQL tables are defined.


## Installation

`pg-bulk-ingest` can be installed from  PyPI using `pip`. `psycopg2` or `psycopg` (Psycopg 3) must also be explicitly installed.

```shell
pip install pg-bulk-ingest psycopg
```


## Start a PostgreSQL database

To run a PostrgreSQL database locally using Docker, on the command line run:

```shell
docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
```


## Create a basic ingest

1. Create an empty Python file, for example `ingest.py`.

1. In this file write code to use the  `ingest` function from the `pg_bulk_ingest` module to create a PostgreSQL table and ingest hard-coded data into it:

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

3. Run this file from the command line

```shell
python -m ingest
```
