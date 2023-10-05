---
layout: sub-navigation
order: 3
title: Get started with Dagster
---

pg-bulk-ingest can be used from within [Dagster](https://docs.dagster.io/), an orchestration tool often used for extract, transform and load (ETL) data pipelines. This page is a step by step guide to setting up a basic Dagster instance that creates such a pipeline to ingest data into a local PostgreSQL database. Dagster usually refers to a pipeline in terms of assets, as does this page.



## Prerequisites

You need the following software to follow this guide:

- a text editor or Integrated Development Environment (IDE), for example [VS Code](https://code.visualstudio.com/)

You should also have experience of progamming in [Python](https://www.python.org/), using the command line, and an understanding of [how PostgreSQL tables are defined](https://www.postgresql.org/docs/current/ddl-basics.html).


## Setup Dagster

Dagster can be setup locally to use pg-bulk-ingest using a variation of its standard instructions. If you have an existing Dagster setup, this section can be skipped and you should follow the instructions for that setup.

1. Follow the [instructions to start up a Dagster instance](https://docs.dagster.io/tutorial/)

2. Add the following dependencies to your `setup.py` file before running `pip install -e ."[dev]"`

    ```
    install_requires=[
        ...
        ...
        "pg-bulk-ingest",
        "psycopg==3.1.10",
    ],
    ```

3. Start your local Dagster instance by running

   ```
   dagster dev
   ```

4. Go to [http://localhost:8080/](http://localhost:8080/) in your browser. You should see the Dagster interface.


## Create an asset that creates an empty table

A good first step is to make an asset that does nothing but creates an empty table.

1. Create a directory within your Dagster directory called `assets`. Within this, create an empty file named `assets.py` and another file named `__init__.py` which contains the following:

```python
from dagster import Definitions, load_assets_from_modules, define_asset_job

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)


```

2. Add the below into the `assets.py` file. This is an optional step, but it highlights the main sections.

```python
# 1. Define the function that makes up this asset

# ...

# 2. Define the specs for the asset you want to create

# ...

# 3. Define the function to build assets from these specs

# ...
```

3. Replace the contents of the file with the below. The environment variable in this case, `POSTGRES_URL`, ingests into the same PostgreSQL that Dagster stores its own metadata. This is is likely to be different in a production setup.

```python

from dagster import asset, AssetsDefinition, RetryPolicy, Backoff, get_dagster_logger

import os

import sqlalchemy as sa
from pg_bulk_ingest import ingest

# 1. Define the function that makes up this asset
def sync(
    schema,
    table_name,
    high_watermark="",
    delete="",
):
    logger = get_dagster_logger()
    engine = sa.create_engine(os.environ['POSTGRES_URL'], future=True)
    logger.info("Schema: %s", schema)
    logger.info("Table name: %s", table_name)
    # The SQLAlchemy definition of the table to ingest data into
    metadata = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata,
        sa.Column("code", sa.VARCHAR(10), primary_key=True),
        sa.Column("suffix", sa.VARCHAR(2), primary_key=True),
        sa.Column("description", sa.VARCHAR(), nullable=False),
        sa.Index(None, "code"),
        schema=schema,
    )

    def batches(high_watermark):
        # No data for now
        yield from ()

    def on_before_visible(
        conn, ingest_table, source_modified_date
    ):
        pass

    with engine.connect() as conn:
        ingest(
            conn,
            metadata,
            batches,
            on_before_visible=on_before_visible,
            high_watermark=high_watermark,
            delete=delete,
        )

# 2. Define the specs for the asset you want to create
specs = [
    {"schema": "dbt", "table_name": "commodity_codes"},
]

# 3. Define the function to build assets from these specs
def build_asset(spec) -> AssetsDefinition:
    schema=spec["schema"]
    table_name=spec["table_name"]
    @asset(
            name=f"{schema}__{table_name}".format(),
            retry_policy=RetryPolicy(
                max_retries=5,
                backoff=Backoff.EXPONENTIAL,
                delay=60,
    ))
    def _asset():
        sync(table_name=spec["table_name"], schema=spec["schema"])
    return _asset


assets=[build_asset(spec) for spec in specs]

```

4. At [http://localhost:8080/](http://localhost:8080/) find and materialize this asset.


## Modify the asset to ingest hard coded data

The ingest function ingests data in batches. An entire batch of data is visible, or none of it is. In the above example, the batches generator function is responsible for supplying batches of data. Each item it yields is a batch. It can yield no batches, as in the above example, one batch, or many more batches.

A batch is a tuple with 3 elements:

1. The first element is the so-called “high watermark” for the batch. If the batch is ingested successfully, this gets stored against the table, and the next time ingest is called, this value gets passed back into the batches function as its first and only argument. It can be None.

2. The second element is metadata for the batch. This gets passed into the on_before_visible callback that is called just before the batch is visible to other database users. It can be any value, but it is often helpful for it to be the time the data was last modified. It can be None.

3. The third element is the data for the batch itself. It must be an iterable of tuples.

    1. The first item must be the SQLAlchemy table for the row

    2. The second item must be a tuple of data for the row, exactly equal to the order of the columns defined in in the SQLAlchemy table definition

With this in mind, you can modify the batches function so every time the asset materializes, 2 batches of fake data are ingested.


```python
def batches(high_watermark):
    yield (None, None, (
        (table, ('0100000000', '80', 'LIVE ANIMALS')),
        (table, ('0101000000', '80', 'Live horses, asses, mules and hinnies')),
    ))

    yield (None, None, (
        (table, ('0101210000', '10', 'Horses')),
        (table, ('0101210000', '80', 'Pure-bred breeding animals')),
    ))
```

## Modify the asset to re-ingest all real data every run

The next step is to create a asset that re-ingests all data every run. If the total data sizes are too large to do this, ingesting only a small amount of real data is often reasonable.

The code to do this depends on the data source. In this example, we can modify the batches function to ingest all UK Tariff commodity codes from a public CSV file hosted on the department's Public Data API. This example uses iterables heavily to avoid loading the entire batch into memory at once.


```python
import csv
import io

import requests

from pg_bulk_ingest import to_file_like_object

def batches(high_watermark):
   url = 'https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/versions/latest/tables/commodities-report/data?format=csv'
   with requests.get(url, stream=True) as response:
      chunks_iter = response.iter_content(65536)
      chunks_file = to_file_like_obj(chunks_iter, bytes)
      lines = io.TextIOWrapper(chunks_file, encoding="utf-8", newline="")
      dicts = csv.DictReader(lines)
      tuples = ((d['commodity__code'], d['commodity__suffix'], d['commodity__description']) for d in dicts)
      yield None, None, (
         (table, t) for t in tuples
      )
```

## Modify the asset to ingest only on change

If necessary, the asset can be modified to ingest data only if it something has changed. How this is done depends on the source - it must support some mechanism of detecting that something has changed since the previous ingest.

In the above example, each dataset in the Public Data API has a “version”, and a mechanism for finding the latest version. The “high watermark” can be used to only ingest data if the latest version is greater than the version previously ingested.


```python
import re

def batches(high_watermark):
   # Find latest version
   url = 'https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/versions/{version}/tables/commodities-report/data?format=csv'
   response = requests.get(url.format(version='latest'), allow_redirects=False)
   latest_version = re.match(r'^.*v(\d+\.\d+\.\d+).*$', response.headers['Location']).groups()[0].split('.')

   # If we have ingested this version or later, do nothing
   if high_watermark is not None and high_watermark >= latest_version:
       return

   # Otherwise, ingest all the data from this version
   with requests.get(url.format(version='v' + '.'.join(latest_version)), stream=True) as response:
      chunks_iter = response.iter_content(65536)
      chunks_file = to_file_like_obj(chunks_iter, bytes)
      lines = io.TextIOWrapper(chunks_file, encoding="utf-8", newline="")
      dicts = csv.DictReader(lines)
      tuples = ((d['commodity__code'], d['commodity__suffix'], d['commodity__description']) for d in dicts)
      yield latest_version, None, (
         (table, t) for t in tuples
      )
```

## Incremental assets

The above examples replace all the data in the existing table on every ingest. This is not always desirable. However, if a data source has some mechanism to fetch only data that has not yet been fetched then pg-bulk-ingest can be used to ingest from this source incrementally, i.e. on each ingest add to the exist data rather than replace it.
