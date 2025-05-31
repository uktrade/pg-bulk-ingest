---
layout: sub-navigation
order: 6
title: Compatibility
---

pg-bulk-ingest is compatible with 3 SQLAlchemy dialects:

- `postgresql+psycopg2`: bundled with SQLAlchemy
- `postgresql+psycopg`: bundled with SQLAlchemy 2.0.0 onwards
- `postgresql+pgarrow`: https://pypi.org/project/pgarrow/

Each of these are compatible with different ranges of PostgreSQL, Python, and its required libraries:

## `postgresql+psycopg2`

- Python >= 3.7.7 (tested on 3.7.7, 3.8.2, 3.9.0, 3.10.0, 3.11.1, 3.12.0, and 3.13.0)
- psycopg2 >= 2.9.2 with Python < 3.13.0, or >= 2.9.10 on Python >= 3.13.0 (tested on 2.9.2 with Python < 3.13.0; and 2.9.10 with Python 3.13.0)
- SQLAlchemy >= 1.4.24, except between 2.0.0 and 2.0.30 on Python >= 3.13 (tested on 1.4.24 with all Python versions; 2.0.0 and 2.0.7 with Python < 3.13.0; and 2.0.31 and 2.0.41 with Python 3.13.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, and 16)

## `postgresql+psycopg`

- Python >= 3.7.7 (tested on 3.7.7, 3.8.2, 3.9.0, 3.10.0, 3.11.1, 3.12.0, and 3.13.0)
- psycopg >= 3.1.4 (tested on 3.1.4 and 3.2.0)
- SQLAlchemy >= 2.0.0 with Python < 3.13, or >= 2.0.31 with Python >= 3.13.0 (tested on 2.0.0 and 2.0.7 with Python < 3.13.0; and 2.0.31 and 2.0.41 with Python 3.13.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, and 16)

## `postgresql+pgarrow`

- Python >= 3.9.0 (tested on 3.9.0, 3.10.0, 3.11.1, 3.12.0, and 3.13.0)
- pgarrow >= 0.0.7 (tested on 0.0.7)
- pyarrow >= 18.0.0 (tested on 18.0.0)
- psycopg >= 3.2.0 - its sql module is used for dynamically constructing queries (tested on 3.2.0)
- SQLAlchemy >= 1.4.24, except between 2.0.0 and 2.0.6 on Python < 3.13.0, and except between 2.0.0 and 2.0.40 on Python >= 3.13.0 (tested on 1.4.24 with all Python versions; 2.0.7 with Python < 3.13.0; 2.0.41 with Python 3.13.0)
- PostgreSQL >= 13.0 (tested on 13.0, 14.0, 15.0, and 16)

For SQLAlchemy < 2 `future=True` must be passed to its `create_engine` function.

There are no plans to drop support for any of the above.
