import argparse
import logging
from parquet_benchmark.trafficlogger import TrafficLogger
from parquet_benchmark.queryengine import *
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="Queries Parquet files in S3 via an HTTP proxy and benchmarks different query engines"
    )
    parser.add_argument(
        "--key",
        type=str,
        required=True,
        help="An S3 object key pointing to a parquet file.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        required=False,
        default=10,
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=False,
        default=42,
        help="Random seed for stochastic sampling",
    )
    parser.add_argument(
        "--bbox",
        type=str,
        required=False,
        default="-77.024438,38.898135,-77.022400,38.900030", # random spot in DC
        help="Comma-separated bbox coordinates: min_lon,min_lat,max_lon,max_lat",
    )
    parser.add_argument(
        "--port",
        type=str,
        required=False,
        default=8080,
        help="The port for the HTTP proxy server",
    )
    parser.add_argument(
        "--engines",
        type=str,
        required=False,
        default="duckdb, spark",
        help="comma-separated list of query engines to benchmark (options: duckdb, spark)",
    )
    args = parser.parse_args()
    print(args)

    proxy = TrafficLogger(listen_host="127.0.0.1", listen_port=args.port)
    proxy.start()
    time.sleep(2)  # Give the proxy a moment to start
    
    engines = [x.strip() for x in args.engines.split(",")]
    bbox = [x.strip() for x in args.bbox.split(",")]

    if "duckdb" in engines:
        engine = DuckDBEngine(data_path=args.key, proxy_port=args.port, bbox=bbox)
        engine.run_bbox_query()
    
    if "spark" in engines:
        engine = SparkEngine(data_path=args.key, proxy_port=args.port, bbox=bbox)
        engine.run_bbox_query()
    
    proxy.shutdown()

if __name__ == "__main__":
    main()
