---
layout: sub-navigation
order: 4
title: Table definitions
---

Tables must be defined via a [SQLAlchemy metadata](https://docs.sqlalchemy.org/en/20/core/metadata.html) instance. Only some SQLAlchemy/PostgreSQL features are supported.


## Data types

The SQLAlchemy "CamelCase" data types are not supported in table definitions. Instead, you must use types specified with "UPPERCASE" data types. These are non-abstracted database-level types. This is to support automatic migrations - the real database type is required in order to make a comparison with the live table and the one passed into the `ingest` function.

Also not supported is the `sqlalchemy.JSON` type. Instead use `sa.dialects.postgresql.JSON` or `sa.dialects.postgresql.JSONB`.


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
