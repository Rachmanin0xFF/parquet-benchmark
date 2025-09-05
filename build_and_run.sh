#!/bin/bash

docker build -t parquetbenchmark:latest .
docker run --rm -it parquetbenchmark:latest
