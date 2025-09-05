# parquet-benchmark
benchmark (geospatial) parquet files

Development spec:

This is a tool designed to benchmark and analyze parquet files. Initially, it should support querying local paths + paths from S3. Maybe Azure blob storage in the far future.

A few things about this project:
1. It should run in a Docker container. It should have a strategy for passing AWS credentials through to Docker. This program may also end up being run on ECS or something else on AWS itself.
2. Everything should be written in Python, using submodule commands when necessary. In order to capture HTTP traffic, it should use the mitmproxy Python module.
3. It should benchmark using two different query engines: DuckDB (the most recent version) and Apache Spark (3.5.2). Docker should be configured properly for Spark to work (I'm not sure if hadoop jars need to be included).
4. Code should be minimalistic, not too much fluff or anything.
5. The tool should have a CLI using argparse that allows specification of the parquet path, whether to save diagrams, etc.

For the output tables:

Generate a table for all queries with the columns:
    - request_type 
    - request_url 
    - byte_lower 
    - byte_upper 
    - total_bytes 
    - time_start 
    - time_end 
    - time_elapsed

Then we'll have a table with per-test-set information:
    - filename
    - # of partitions
    - # of row groups
    - # of columns
    - Average rows / row group
    - Row group size histogram
    - Has page statistics?
    - Queries [time, bbox, method, bytes read, rows read]
    - Average query time w/ bbox
    - Average query time w/ geometric filtering
    - Which query engine was used