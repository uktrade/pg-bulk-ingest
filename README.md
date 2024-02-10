# pg-bulk-ingest

[![PyPI package](https://img.shields.io/pypi/v/pg-bulk-ingest?label=PyPI%20package&color=%234c1)](https://pypi.org/project/pg-bulk-ingest/) [![Test suite](https://img.shields.io/github/actions/workflow/status/uktrade/pg-bulk-ingest/test.yml?label=Test%20suite)](https://github.com/uktrade/pg-bulk-ingest/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/uktrade/pg-bulk-ingest?label=Code%20coverage)](https://app.codecov.io/gh/uktrade/pg-bulk-ingest)

A Python utility function for ingesting data into SQLAlchemy-defined PostgreSQL tables, automatically migrating them as needed, allowing concurrent reads as much as possible.

Allowing concurrent writes is not an aim of pg-bulk-ingest. It is designed for use in ETL pipelines where PostgreSQL is used as a data warehouse, and the only writes to the table are from pg-bulk-ingest. It is assumed that there is only one pg-bulk-ingest running against a given table at any one time.


## Features

pg-bulk-ingest exposes a single function as its API that:

- Creates the tables if necessary
- Migrates any existing tables if necessary, minimising locking
- Ingests data in batches, where each batch is ingested in its own transaction
- Handles "high-watermarking" to carry on from where a previous ingest finished or errored
- Optionally performs an "upsert", matching rows on primary key
- Optionally deletes all existing rows before ingestion
- Optionally calls a callback just before each batch is visible to other database clients


---

Visit the [pg-bulk-ingest documentation](https://pg-bulk-ingest.docs.trade.gov.uk/) for usage instructions.
