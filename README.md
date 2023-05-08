# pg-bulk-ingest

A collection of Python utility functions for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, and minimising locking

> Work-in-progress. This README serves as a rough design spec


## Usage

The API is made of 3 functions:

- `overwrite` - deletes all existing data in the table before ingesting the incoming data
- `append` - appends all incoming data to the table
- `upsert` - inserts the incoming data into the table, but if a primary key matches an existing row, updates the existing row

In each case:

- Ingestion happens in a transaction - it is all ingested or none at all
- Tables are migrated to match the definitions, using techniques to avoid exclusively locking the table.
- If it's not possible to not exclusively lock the table, then an intermediate table is used, swapped with the live table at the end of the ingest. This does require an exclusive lock, but only for a short time. Backends that hold locks that conflict with this lock are forcably terminated after a delay.
- Ingestion is done exclusively with `COPY FROM`.
- `VACUUM ANALYZE` is run at the end of the ingest.

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

# A SQLAlchemy table definition
my_table = sa.Table(
    "my_table",
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("value", sa.String(16), nullable=False),
    schema="my_schema",
)
with engine.begin() as conn:
    upsert(conn, my_table, ((row, my_table) for row in rows))
```
