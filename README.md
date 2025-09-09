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

## S3 Output Strategy

The benchmark now automatically uploads results to S3 with the following structure:

```
s3://your-bucket/your-path/ds=YYYYMMDD_HHMMSS/
├── traffic.parquet          # Combined HTTP traffic logs from all queries
└── [query_name]_gantt.html  # Gantt charts for queries with display_traffic enabled
```

### Usage

```bash
# Run benchmark with S3 output
python benchmark.py \
  --input s3://bucket/path/to/data/* \
  --output s3://bucket/results/ \
  --engines "duckdb,spark"

# If bucket is not in the output path, specify it separately
python benchmark.py \
  --input s3://bucket/path/to/data/* \
  --output results/parquet-benchmark/ \
  --bucket my-bucket \
  --engines "duckdb,spark"
```

### Traffic Data Schema

The `traffic.parquet` file contains the following columns:
- All original HTTP traffic columns (request_type, url, status_code, response_time_ms, etc.)
- `query_name`: Name of the query and engine (e.g., "Tiny bbox_duckdb")
- `query_index`: Unique index for each query execution
- `bbox_min_lon`, `bbox_min_lat`, `bbox_max_lon`, `bbox_max_lat`: Bounding box coordinates

### Authentication

Ensure AWS credentials are available either through:
- Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
- AWS credentials file (~/.aws/credentials)
- IAM roles (when running on AWS)
