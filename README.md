# pg-bulk-ingest

A Python utility function for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, and minimising locking

> Work-in-progress. This README serves as a rough design spec


## Installation

`pg-bulk-ingest` can be installed from  PyPI using `pip`. `psycopg2` or `psycopg` (Psycopg 3) must also be explicitly installed.

```
pip install pg-bulk-ingest psycopg
```


## Usage

The API is made of a single function, `ingest` that can be used to insert data into a table. This:

- creates the table if necessary
- migrates any existing table if necessary
- inserts the incoming data into the table
- if the table has a primary key, performs an "upsert" based matching on this primary key
- optionally deletes all existing rows before ingestion

For example:

```python
import sqlalchemy as sa
from pg_bulk_ingest import Mode, After, ingest

# Run postgresql locally should allow the below to run
# docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
engine = sa.create_engine('postgresql+psycopg://postgres@127.0.0.1:5432/')

# Collection of SQLAlchemy table definitions - a "Metadata"
metadata = sa.MetaData()
my_table = sa.Table(
    "my_table",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("value", sa.String(16), nullable=False),
    schema="my_schema",
)

# A function that yields batches of data, where each batch is
# a tuple of of (high watermark, data rows). The batches must all
# be strictly _after_ the high watermark passed into the function
# Each row much have one of the SQLAlchemy tables associated with it
def batches(high_watermark):
    if high_watermark < '2015-01-01',
        yield '2015-01-01', (
            (my_table, (3, 'a')),
            (my_table, (4, 'b')),
            (my_table, (5, 'c')),
        )
    if high_watermark < '2015-01-02',
        yield '2015-01-02', (
            (my_table, (6, 'd')),
            (my_table, (7, 'e')),
            (my_table, (8, 'f')),
        )

with engine.connect() as conn:
    ingest(
        conn, metadata, batches,
        after=After.HIGH_WATERMARK,             # Carry on from where left off
        mode=Mode.UPSERT_AND_COMMIT_EACH_BATCH, # Upsert based on primary key if present, committing each batch
    )
```


## Under the hood

- Ingestion is done exclusively with `COPY FROM`.
- Ingestion is transactional, each batch is ingested completely or not at all
- Tables are migrated to match the definitions, using techniques to avoid exclusively locking the table to allow parallel SELECT queries.
- If the table has a primary key, then an "upsert" is performed. Data is ingested into an intermediate table, and an `INSERT ... ON CONFICT(...) DO UPDATE` is performed to copy rows from this intermediate table to the existing table. This doesn't involve an exclusive lock on the live table, unless a migration requires it as follows.
- If there is no known technique for a migration without a long-running exclusive lock, then an intermediate table is used, swapped with the live table at the end of the ingest. This swap does require an exclusive lock, but only for a short time. Backends that hold locks that conflict with this lock are forcably terminated after a delay.


## Compatibility

- Python >= 3.7.1 (tested on 3.7.1, 3.8.0, 3.9.0, 3.10.0, and 3.11.0)
- psycopg2 >= 2.9.2 or Psycopg 3 >= 3.1.4
- SQLAlchemy >= 1.4.24 (tested on 1.4.24 and 2.0.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, and 15.0)

Note that SQLAlchemy < 2 does not support Psycopg 3, and for SQLAlchemy < 2 `future=True` must be passed to `create_engine`.
