#!/bin/bash

set -e

docker build . -t postgres-pg-bulk-ingest --build-arg VERSION=${1:-"14.0"}
docker run --rm -it --name pg-bulk-ingest-postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 -d postgres-pg-bulk-ingest
