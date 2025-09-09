#!/bin/bash

# be sure your environment vars are set before running this.
# You can get these from your sso auth with:
#   eval "$(aws configure export-credentials --profile [PROFILE] --format env)"

docker build -t parquetbenchmark:latest .
docker run --rm -it \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
    -v $HOME/.aws:/home/appuser/.aws parquetbenchmark:latest \
    --input s3://overturemaps-us-west-2/release/2025-08-20.1/theme=places/type=place/* \
    --output s3://omf-internal-usw2/test/alastowka/spatial-partition-benchmark/results/