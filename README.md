# pg-bulk-ingest

A collection of Python utility functions for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, and minimising locking

> Work-in-progress. This README serves as a rough design spec


## Installation

Installation depends on which underlying psycopg driver you would like to use. Binary and non-binary versions of both psycopg2 and Psycopg 3 are supported.

```
pip install pg-bulk-ingest[psycopg2]         # psycopg2
pip install pg-bulk-ingest[psycopg2-binary]  # psycopg2-binary
pip install pg-bulk-ingest[psycopg]          # psycopg (Psycopg 3)
pip install pg-bulk-ingest[psycopg-binary].  # psycopg[binary] (Psycopg 3)
```

This is more complex than is ideal because psycopg2 and Psycopg 3 are published as different packages in PyPI, as are their binary versions, and because SQLAlchemy < 2 doesn't support psycopg2.


## Usage

The API is made of 3 functions:

- `replace` - replaces all the existing rows in the table with the incoming rows
- `insert` - inserts the incoming rows into the table, leaving existing rows alone
- `upsert` - inserts the incoming rows into the table, but if a primary key matches an existing row, updates the existing row

In each case under hood:

- Ingestion is done exclusively with `COPY FROM`.
- Ingestion happens in a transaction - it is all ingested or none at all
- Tables are migrated to match the definitions, using techniques to avoid exclusively locking the table to allow parallel SELECT queries.
- For `upsert`, data is ingested into an intermediate table, and an `INSERT ... ON CONFICT(...) DO UPDATE` is performed to copy rows from this intermediate table to the existing table. This doesn't involve an exclusive lock on the live table, unless a migration requires it as follows.
- For all 3 functions, if there is no known technique for a migration without a long-running exclusive lock, then an intermediate table is used, swapped with the live table at the end of the ingest. This swap does require an exclusive lock, but only for a short time. Backends that hold locks that conflict with this lock are forcably terminated after a delay.

For example to `upsert`:

```python
import sqlalchemy as sa
from pg_bulk_ingest import upsert

# Run postgresql locally should allow the below to run
# docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
engine = sa.create_engine('postgresql+psycopg://postgres@127.0.0.1:5432/')

# Any iterable of tuples
rows = (
    (3, 'd'),
    (4, 'a'),
    (5, 'q'),
)

# Collection of SQLAlchemy table definitions - a "Metadata"
metadata = sa.MetaData()
my_table = sa.Table(
    "my_table",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("value", sa.String(16), nullable=False),
    schema="my_schema",
)
with engine.begin() as conn:
    upsert(conn, metadata, ((row, my_table) for row in rows))
```

## Compatibility

- Python >= 3.7.1 (tested on 3.7.1, 3.8.0, 3.9.0, 3.10.0, and 3.11.0)
- psycopg2 >= 2.9.2 or Psycopg 3 >= 3.1.4
- SQLAlchemy >= 1.4.24 (tested on 1.4.24 and 2.0.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, and 15.0)
