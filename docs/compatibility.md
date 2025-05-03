---
layout: sub-navigation
order: 6
title: Compatibility
---

pg-bulk-ingest aims to be compatible with a wide range of Python and other dependencies:

- Python >= 3.7.7 (tested on 3.7.7, 3.8.2, 3.9.0, 3.10.0, 3.11.1, and 3.12.0)
- psycopg2 >= 2.9.2 and Psycopg 3 >= 3.1.4
- SQLAlchemy >= 1.4.24 (tested on 1.4.24 and 2.0.0)
- PostgreSQL >= 9.6 (tested on 9.6, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, and 16 Beta 2)

Note that SQLAlchemy < 2 does not support Psycopg 3, and for SQLAlchemy < 2 `future=True` must be passed to its `create_engine` function.

There are no plans to drop support for any of the above.
