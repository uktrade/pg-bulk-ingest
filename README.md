# pg-bulk-ingest

A Python utility function for ingesting data into a SQLAlchemy-defined PostgreSQL table, automatically migrating it as needed, and minimising locking.


## Installation

`pg-bulk-ingest` can be installed from  PyPI using `pip`. `psycopg2` or `psycopg` (Psycopg 3) must also be explicitly installed.

```
pip install pg-bulk-ingest psycopg
```


## Usage

Data ingest to a table is done through the function `ingest`. This function:

- creates the table if necessary
- migrates any existing table if necessary, minimising locking
- inserts the incoming data into the table
- if the table has a primary key, performs an "upsert", matching on this primary key
- handles "high-watermarking" to carry on from where a previous ingest finished or errored
- optionally deletes all existing rows before ingestion

For example:

```python
import sqlalchemy as sa
from pg_bulk_ingest import HighWatermark, Visibility, Delete, ingest

# Run postgresql locally should allow the below to run
# docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
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

# A function that yields batches of data, where each is a tuple of of (high watermark, data rows).
# The batches must all be strictly _after_ the high watermark passed into the function
# Each high watermark must be JSON-encodable
# Each row must have the SQLAlchemy table associated with it
def batches(high_watermark):
    if high_watermark is None or high_watermark < '2015-01-01':
        yield '2015-01-01', (
            (my_table, (3, 'a')),
            (my_table, (4, 'b')),
            (my_table, (5, 'c')),
        )
    if high_watermark is None or high_watermark < '2015-01-02':
        yield '2015-01-02', (
            (my_table, (6, 'd')),
            (my_table, (7, 'e')),
            (my_table, (8, 'f')),
        )

with engine.connect() as conn:
    ingest(
        conn, metadata, batches,
        high_watermark=HighWatermark.LATEST,     # Carry on from where left off
        visibility=Visibility.AFTER_EACH_BATCH,  # Changes are visible after each batch
        delete=Delete.OFF,                       # Don't delete any existing rows
    )
```


## API

The API is a single function `ingest`, together with classes of string constants: `HighWatermark`, `Visibility`, and `Delete`. The constants are known strings rather than opaque identifiers to allow the strings to be easily passed from dynamic/non-Python environments.

---

`ingest`(conn, metadata, batches, high_watermark=HighWatermark.LATEST, visibility=Visibility.AFTER_EACH_BATCH, delete=Delete.OFF)

Ingests data into a table

- `conn` - A [SQLAlchemy connection](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection) not in a transaction, i.e. started by `connection` rather than `begin`.

- `metadata` - A SQLAlchemy metadata of a single table.

- `batches` - A function that takes a high watermark, returning an iterable that yields data batches that are strictly after this high watermark. See Usage above for an example.

- `high_watermark` (optional) - A member of the `HighWatermark` class, or a JSON-encodable value.

    If this is `HighWatermark.LATEST`, then the most recent high watermark that been returned from a previous ingest's `batch` function whose corresponding batch has been succesfully ingested is passed into the `batches` function. If there has been no previous ingest, `None` will be passed.

    If this a JSON-encodable value other than `HighWatermark.LATEST`, then this value is passed directly to the `batches` function. This can be used to override any previous high-watermark. Existing data in the target table is not deleted unless specified by the `delete` parameter.

- `visibility` (optional) - A member of the `Visibilty` class, controlling when ingests will be visible to other clients, 

- `delete` (optional) - A member of the `Delete` class, controlling if existing rows are to be deleted.

---

`HighWatermark`

A class of constants to indicate to the `ingest` function how it should use any previously stored high watermark. Its single member is:

- `LATEST` - use the most recently high watermark, passing it to the batches function. This is the string `__LATEST__`.

---

`Visibility`

A class of constants to indicate when data changes are visible to other database clients. Schema changes become visible before the first batch.

- `AFTER_EACH_BATCH` - data changes are visible to other database clients after each batch. This is the string `__AFTER_EACH_BATCH__`.

---

`Delete`

A class of constants that controls how existing data in the table is deleted

- `OFF`

   There is no deleting of existing data. This is the string `__OFF__`.

- `ALL`

   All existing data in the table is deleted. This is the string `__ALL__`.


## Data types

The SQLAlchemy "CamelCase" data types are not supported in table definitions. Instead, you must use types specified with "UPPERCASE" data types. These are non-abstracted database-level types. This is to support automatic migrations - the real database type is required in order to make a comparison with the live table and the one passed into the `ingest` function.

Also not supported is the sqlalchemy.JSON type. Instead use `sa.dialects.postgresql.JSON` or `sa.dialects.postgresql.JSONB`.


## Under the hood

- Ingestion is done exclusively with `COPY FROM`.
- Ingestion is transactional, each batch is ingested completely or not at all
- The table is migrated to match the definition, using techniques to avoid exclusively locking the table to allow parallel SELECT queries.
- If the table has a primary key, then an "upsert" is performed. Data is ingested into an intermediate table, and an `INSERT ... ON CONFLICT(...) DO UPDATE` is performed to copy rows from this intermediate table to the existing table. This doesn't involve an ACCESS EXCLUSIVE lock on the live table, so SELECTs can continue in parallel.
- Migrations usually require ACCESS EXCLUSIVE lock on the live table. However, if there is no known technique for a migration without a long-running lock, then an intermediate table is created, matching the required definition, existing data is copied into this table, and it replaces the live table before the first batch. This replacement requires an ACCESS EXCLUSIVE lock, but only for a short time. Backends that hold locks that block migrations are forcably terminated after a delay.
- The high watermark is stored on the table as a COMMENT, JSON-encoded. For example if the most recent high watermark is the string `2014-07-31`, then the comment would be `{"pg-bulk-ingest": {"high-watermark": "2014-07-31"}}`.


## Compatibility

- Python >= 3.7.1 (tested on 3.7.1, 3.8.0, 3.9.0, 3.10.0, and 3.11.0)
- psycopg2 >= 2.9.2 or Psycopg 3 >= 3.1.4
- SQLAlchemy >= 1.4.24 (tested on 1.4.24 and 2.0.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, and 15.0)

Note that SQLAlchemy < 2 does not support Psycopg 3, and for SQLAlchemy < 2 `future=True` must be passed to `create_engine`.
