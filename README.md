# pg-bulk-ingest

A collection of Python utility functions for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, and minimising locking

> Work-in-progress. This README serves as a rough design spec


## Concepts

Data is consumed by pg-bulk-ingest by iterables, so to pass data into pg-bulk-ingest functions, it must be defined as an iterable.

To ingest from the iterable, under the hood `COPY FROM` is used. Depending on the function, this is either via an intermediate table, or directly to the target table.

Techniques that avoid exclusive locks are used. If this locking is unavoidable, then after a configurable delay, clients that hold locks that conflict with the locks needed to ingest the data or migrate the tables are terminated.
