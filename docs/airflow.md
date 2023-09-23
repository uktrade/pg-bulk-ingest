---
layout: sub-navigation
order: 2
title: Airflow
---

pg-bulk-ingest can be used from within [Apache Airflow](https://airflow.apache.org/), an orchestration tool often used for extract, transform and load (ETL) data pipelines. This page is a step by step guide to setting up a basic Airflow instance that creates such a pipeline to ingest data into a local PostgreSQL database. Airflow usually refers to a pipeline as a directed acyclic graph (DAG), as does this page.

Airflow is very flexible. If you have an existing Airflow setup, not all parts of this guide may be applicable.


## Prerequisites

You need the following software to follow this guide:

- [Docker](https://www.docker.com/get-started/)
- a text editor or Integrated Development Environment (IDE), for example [VS Code](https://code.visualstudio.com/)

You should also have experience of progamming in [Python](https://www.python.org/), and of using the command line.


## Setup Airflow

Airflow can be setup locally to use pg-bulk-ingest using a variation of its standard instructions. If you have an existing Airflow setup, this section can be skipped and you should follow the instructions for that setup.

1. Follow the instructions at [https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) up to, but not including, the "Running Airflow" stage.

2. Create a plain text file named `Dockerfile` in the local folder that contains the following.

   ```
   FROM apache/airflow:2.7.1

   RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pg-bulk-ingest==0.0.42 psycopg==3.1.10
   ```
   This defines a Docker container has pg-bulk-ingest installed on it.

3. Modify the `docker-commpose.yaml` file that you downloaded as part of the step 1. Prefix the line that contains

   ```
   image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:
   ```
   with a `#` symbol to comment it out, and remove the `#` from the line that contains

   ```
   build: .
   ```
   to uncomment it. This means that Airflow will run on the Docker container defined in step 2.

2. Start your local Airflow instance by running

   ```
   docker compose up --build
   ```

3. Go to [http://localhost:8080/](http://localhost:8080/) in your browser. You should see the Airflow interface.


## Create a DAG that creates an empty table

A good first step is to make a DAG that does nothing but creates an empty table.

1. Create an empty file in your dags directory that indicates the source of the data. If you are making changes to an existing Airflow setup, you may have a naming convention to follow in order for Airflow to detect the file. For example, you may have to end the file name in `_pipeline.py` or `_dag.py`.

2. Add the below into the file. This is an optional step, but it highlights the main sections.

```python
# 1. Define the function that makes up the single task of this DAG

# ...

# 2. Define the function that creates the DAG from the single task

# ...

# 3. Call the function that creates the DAG

# ...
```

3. Replace the contents of the file with the below. The environment variable in this case, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, ingests into the same PostgreSQL that Airflow stores its own metadata. This is is likely to be different in a production setup.

```python
from datetime import datetime, timedelta
import logging
import os

import sqlalchemy as sa
from pg_bulk_ingest import ingest

from airflow.datasets import Dataset
from airflow.decorators import dag, task


# 1. Define the function that makes up the single task of this DAG

@task(
    retries=5,
    retry_exponential_backoff=True,
    retry_delay=timedelta(seconds=60),
    max_retry_delay=timedelta(minutes=30),
)
def sync(
    dag_id,
    schema,
    table_name,
    high_watermark,
    delete,
):
    engine = sa.create_engine(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'], future=True)

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


# 2. Define the function that creates the DAG from the single task

def create_dag(dag_id, schema, table_name):
    @dag(
        dag_id=dag_id,
        start_date=datetime(2022, 5, 16),
        schedule='@daily',
        catchup=False,
        max_active_runs=1,
        params={
            'high_watermark': '__LATEST__',
            'delete': '__BEFORE_FIRST_BATCH__',
        },
    )
    def Pipeline():
        sync.override(
            outlets=[
                Dataset(f'postgresql://datasets/{schema}.{table_name}'),
            ]
        )(
            dag_id=dag_id,
            schema=schema,
            table_name=table_name,
            high_watermark="{{ params.high_watermark }}",
            delete="{{ params.delete }}",
        )

    Pipeline()

# 3. Call the function that creates the DAG

create_dag('CommodityCodes', 'dbt', 'commodity_codes')

```

If you are familiar with Airflow, you may notice that there is an extra wrapper function - the create_dag wrapper function does not exist in Airflow’s documentation, for example at https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html. However, it allows the creation of several DAGs at the same time that differ only slightly, and so is the recommended pattern.

4. At [http://localhost:8080/](http://localhost:8080/) find and run this DAG.


## Modify the DAG to ingest hard coded data

The ingest function ingests data in batches. An entire batch of data is visible, or none of it is. In the above example, the batches generator function is responsible for supplying batches of data. Each item it yields is a batch. It can yield no batches, as in the above example, one batch, or many more batches.

A batch is a tuple with 3 elements:

1. The first element is the so-called “high watermark” for the batch. If the batch is ingested successfully, this gets stored against the table, and the next time ingest is called, this value gets passed back into the batches function as its first and only argument. It can be None.

2. The second element is metadata for the batch. This gets passed into the on_before_visible callback that is called just before the batch is visible to other database users. It can be any value, but it is often helpful for it to be the time the data was last modified. It can be None.

3. The third element is the data for the batch itself. It must be an iterable of tuples.

    1. The first item must be the SQLAlchemy table for the row

    2. The second item must be a tuple of data for the row, exactly equal to the order of the columns defined in in the SQLAlchemy table definition

With this in mind, you can modify the batches function so every time the DAG runs, 2 batches of fake data are ingested.


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

## Modify the DAG to re-ingest all real data every run

The next step is to create a DAG that re-ingests all data every run. If the total data sizes are too large to do this, ingesting only a small amount of real data is often reasonable.

The code to do this depends on the data source. In this example, we can modify the batches function to ingest all UK Tariff commodity codes from a public CSV file hosted on the department's Public Data API. This example uses iterables heavily to avoid loading the entire batch into memory at once.


```python
import csv
import io

import requests

from io import IOBase

def to_file_like_obj(iterable, base):
    chunk = base()
    offset = 0
    it = iter(iterable)

    def up_to_iter(size):
        nonlocal chunk, offset

        while size:
            if offset == len(chunk):
                try:
                    chunk = next(it)
                except StopIteration:
                    break
                else:
                    offset = 0
            to_yield = min(size, len(chunk) - offset)
            offset = offset + to_yield
            size -= to_yield
            yield chunk[offset - to_yield : offset]

    class FileLikeObj(IOBase):
        def readable(self):
            return True

        def read(self, size=-1):
            return base().join(
                up_to_iter(float('inf') if size is None or size < 0 else size)
            )

    return FileLikeObj()

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

## Modify the DAG to ingest only on change

If necessary, the DAG can be modified to ingest data only if it something has changed. How this is done depends on the source - it must support some mechanism of detecting that something has changed since the previous ingest.

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

## Incremental DAGs

The above examples replace all the data in the existing table on every ingest. This is not always desirable. However, if a data source has some mechanism to fetch only data that has not yet been fetched then pg-bulk-ingest can be used to ingest from this source incrementally, i.e. on each ingest add to the exist data rather than replace it.
