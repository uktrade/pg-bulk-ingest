# pg-bulk-ingest

A collection of Python utility functions for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, and minimising locking

> Work-in-progress. This README serves as a rough design spec


## Usage

The API is made of 3 functions:

- `overwrite` - deletes everything from the table before ingesting
- `append` - appends to the table
- `merge` - merges data with the existing data in the table based on primary key

In each case:

- Ingestion happens in a transaction - it is all ingested or none at all
- Tables are migrated to match the definitions, using techniques to avoid exclusively locking the table.
- If it's not possible to not exclusively lock the table, then an intermediate table is used, swapped with the live table at the end of the ingest. This does require an exclusive lock, but only for a short time. Backends that hold locks that conflict with this lock are forcably terminated after a delay.


```python
import sqlalchemy as sa
from pg_bulk_ingest import merge

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
    merge(conn, my_table, ((row, my_table) for row in rows))
```
