#!/bin/bash

# be sure your environment vars are set before running this.
# You can get these from your sso auth with:
#   eval "$(aws configure export-credentials --profile [PROFILE] --format env)"

docker build -t parquetbenchmark:latest .
docker run --rm -it \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
    -v $HOME/.aws:/home/appuser/.aws parquetbenchmark:latest --key s3a://omf-internal-usw2/test/alastowka/spatial-partition-benchmark/geohashRange.parquet/
