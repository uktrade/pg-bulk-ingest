# pg-bulk-ingest

A Python utility function for ingesting data into a SQLAlchemy-defined PostgreSQL table, automatically migrating it as needed, allowing concurrent reads as much as possible.

Allowing concurrent writes is not an aim of this function. This is designed for cases where PostgreSQL is used as a data warehouse, and the only writes to the table are from pg-bulk-ingest. It is assumed that there is only one pg-bulk-ingest running against a given table at any one time.


## Installation

`pg-bulk-ingest` can be installed from  PyPI using `pip`. `psycopg2` or `psycopg` (Psycopg 3) must also be explicitly installed.

```
pip install pg-bulk-ingest psycopg
```


## Usage

Data ingest to a table is done through the function `ingest`. This function:

- creates the table if necessary
- migrates any existing table if necessary, minimising locking
- inserts the incoming data into the table in batches, where each batch is ingested in its own transaction
- if the table has a primary key, performs an "upsert", matching on this primary key
- handles "high-watermarking" to carry on from where a previous ingest finished or errored, or to set pipeline to ingest from the start
- optionally deletes all existing rows before ingestion
- optionally calls a callback just before each batch is visible to other database clients

Full example:

```python
import sqlalchemy as sa
from pg_bulk_ingest import HighWatermark, Visibility, Upsert, Delete, ingest

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

# A function that yields batches of data, where each is a tuple of
# (high watermark, batch metadata, data rows).
# The batches should all be _after_ the high watermark passed into the function
# Each high watermark must be JSON-encodable
# Each row must have the SQLAlchemy table associated with it
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

def on_before_visible(conn, batch_metadata):
    # Can perform validation or update metadata table(s) just before data
    # is visible to other database clients
    # conn: is a SQLAlchemy connection in the same transaction as this batch
    # batch_metadata: the metadata for the most recent batch from the batches
    # function

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


## API

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


## Data types

The SQLAlchemy "CamelCase" data types are not supported in table definitions. Instead, you must use types specified with "UPPERCASE" data types. These are non-abstracted database-level types. This is to support automatic migrations - the real database type is required in order to make a comparison with the live table and the one passed into the `ingest` function.

Also not supported is the sqlalchemy.JSON type. Instead use `sa.dialects.postgresql.JSON` or `sa.dialects.postgresql.JSONB`.


## Indexes

Indexes can be added by any of two mechanisms:

1. Setting `index=True` on a column.

    ```python
    sa.Table(
        "my_table",
        metadata,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False, index=True),
        schema="my_schema",
    )
    ```

2. Passing `sqlalchemy.Index` objects after the column list when defining the table. The name of each index should be `None`, which allows SQLAlchemy to give it a name unlikely to conflict with other indexes.

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
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, and 16 Beta 2)

Note that SQLAlchemy < 2 does not support Psycopg 3, and for SQLAlchemy < 2 `future=True` must be passed to `create_engine`.
