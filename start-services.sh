#!/bin/bash

set -e

docker run --rm -it --name pg-bulk-ingegst-postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 -d ${1:-postgres}
