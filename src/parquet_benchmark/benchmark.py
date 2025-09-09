import argparse
import logging
from parquet_benchmark.trafficlogger import TrafficLogger
from parquet_benchmark.queryengine import *
from parquet_benchmark.visualizer import *
from parquet_benchmark.config import default_config
import pandas as pd
import numpy as np
import time
import boto3
from datetime import datetime
import os
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Query:
    def __init__(self, data_path, engine, proxy, index, bbox, query_name=""):
        self.data_path = data_path
        self.engine = engine
        self.proxy = proxy
        self.index = index
        self.query_name = query_name
        self.results = None
        self.events = []
        self.bbox = bbox
        self.run()
    
    def run(self):
        self.events = []
        self.results = self.engine.run_bbox_query(self.data_path, self.bbox)
        while True:
            event = self.proxy.get_event(timeout=1)
            if event:
                self.events.append(event)
            else:
                break
        self.events = pd.DataFrame(self.events)


def generate_bbox(bbox_config, samples=1):
    """Generate bbox(es) based on config"""
    if bbox_config["type"] == "uniform":
        if "geometry" in bbox_config:
            return [bbox_config["geometry"]]
    elif bbox_config["type"] == "random":
        size_range = bbox_config["size"]
        
        # Generate random bboxes globally (not just US)
        bboxes = []
        for i in range(samples):
            # Global bounds: longitude -180 to 180, latitude -85 to 85
            center_lon = np.random.uniform(-180, 180)
            center_lat = np.random.uniform(-85, 85)
            
            # Random size within range
            width = np.random.uniform(size_range[0], size_range[1])
            height = np.random.uniform(size_range[0], size_range[1])
            
            bbox = [
                max(-180, center_lon - width/2),  # min_lon (clamp to valid range)
                max(-85, center_lat - height/2),  # min_lat
                min(180, center_lon + width/2),   # max_lon
                min(85, center_lat + height/2)    # max_lat
            ]
            
            print(f"Generated random bbox {i+1}: {bbox}")
            bboxes.append(bbox)
        return bboxes
    return []


def upload_to_s3(local_file_path, s3_output_path, bucket_name=None):
    """Upload a local file to S3"""
    try:
        s3_client = boto3.client('s3')
        
        # Parse S3 output path to get bucket and key
        if s3_output_path.startswith('s3://'):
            parsed = urlparse(s3_output_path)
            bucket = bucket_name or parsed.netloc
            key = parsed.path.lstrip('/')
        else:
            # Assume it's just the key and bucket is provided separately
            bucket = bucket_name
            key = s3_output_path
        
        if not bucket:
            raise ValueError("S3 bucket name must be provided either in the output path or as bucket_name parameter")
        
        logger.info(f"Uploading {local_file_path} to s3://{bucket}/{key}")
        s3_client.upload_file(local_file_path, bucket, key)
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to S3: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Queries Parquet files in S3 via an HTTP proxy and benchmarks different query engines"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="An S3 object key pointing to a parquet file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="S3 output path for saving results (e.g., s3://bucket/path/ or just bucket/path/). Results will be saved with a timestamp."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=False,
        help="S3 bucket name (only needed if not included in --output path)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=False,
        default=42+1,
        help="Random seed for stochastic sampling",
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
    np.random.seed(args.seed)

    # Generate timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create local output directory in a location the user can write to
    local_output_dir = f"/tmp/output/{timestamp}"
    os.makedirs(local_output_dir, exist_ok=True)

    proxy = TrafficLogger(listen_host="127.0.0.1", listen_port=args.port)
    proxy.start()
    time.sleep(2)  # Give the proxy a moment to start
    
    engines_list = [x.strip() for x in args.engines.split(",")]
    
    # Initialize engines
    engines = {}
    if "duckdb" in engines_list:
        engines["duckdb"] = DuckDBEngine(proxy_port=args.port)
    if "spark" in engines_list:
        engines["spark"] = SparkEngine(proxy_port=args.port)

    # Run all queries from config
    all_queries = []
    query_index = 0
    
    for query_config in default_config["queries"]:
        query_name = query_config["name"]
        bbox_config = query_config["bbox"]
        samples = query_config.get("samples", 1)
        
        # Generate bboxes for this query
        bboxes = generate_bbox(bbox_config, samples=samples)
        
        # Run each bbox with each engine
        for bbox in bboxes:
            for engine_name, engine in engines.items():
                logger.info(f"Running query '{query_name}' with {engine_name} engine")
                q = Query(
                    data_path=args.input, 
                    engine=engine, 
                    proxy=proxy, 
                    index=query_index,
                    bbox=bbox,
                    query_name=f"{query_name}_{engine_name}"
                )
                all_queries.append(q)
                query_index += 1
                
    # Create and save gantt charts for queries that have display_traffic enabled
    for query_config in default_config["queries"]:
        if query_config.get("display_traffic", False):
            query_name = query_config["name"]
            matching_queries = [q for q in all_queries if q.query_name.startswith(query_name)]
            
            if matching_queries:
                # Group queries by engine
                queries_by_engine = {}
                for q in matching_queries:
                    # Extract engine from query_name (format: "Query Name_engine")
                    engine = q.query_name.split('_')[-1]
                    if engine not in queries_by_engine:
                        queries_by_engine[engine] = []
                    queries_by_engine[engine].append(q)
                
                # Create separate gantt chart for each engine
                for engine, engine_queries in queries_by_engine.items():
                    all_events = []
                    for q in engine_queries:
                        if not q.events.empty:
                            # Add query info to events
                            events_copy = q.events.copy()
                            events_copy['query_name'] = q.query_name
                            events_copy['query_index'] = q.index
                            all_events.append(events_copy)
                    
                    if all_events:
                        combined_events = pd.concat(all_events, ignore_index=True)
                        fig = create_time_based_gantt_with_lanes(combined_events)
                        
                        # Save gantt chart locally with engine name
                        chart_filename = f"{query_name.replace(' ', '_').lower()}_{engine}_gantt.html"
                        local_chart_path = os.path.join(local_output_dir, chart_filename)
                        fig.write_html(local_chart_path)
                        logger.info(f"Saved gantt chart for {engine} engine to {local_chart_path}")
                        
                        # Upload to S3
                        s3_chart_path = f"{args.output.rstrip('/')}/ds={timestamp}/{chart_filename}"
                        upload_to_s3(local_chart_path, s3_chart_path, args.bucket)
                    else:
                        logger.info(f"No traffic events recorded for query '{query_name}' with {engine} engine")
                        
                # Also create a combined chart with all engines for comparison
                all_events = []
                for q in matching_queries:
                    if not q.events.empty:
                        # Add query info to events
                        events_copy = q.events.copy()
                        events_copy['query_name'] = q.query_name
                        events_copy['query_index'] = q.index
                        # Add engine info for color coding
                        events_copy['engine'] = q.query_name.split('_')[-1]
                        all_events.append(events_copy)
                
                if all_events:
                    combined_events = pd.concat(all_events, ignore_index=True)
                    fig = create_time_based_gantt_with_lanes(combined_events, color_by='engine')
                    
                    # Save combined gantt chart locally
                    chart_filename = f"{query_name.replace(' ', '_').lower()}_all_engines_gantt.html"
                    local_chart_path = os.path.join(local_output_dir, chart_filename)
                    fig.write_html(local_chart_path)
                    logger.info(f"Saved combined gantt chart for all engines to {local_chart_path}")
                    
                    # Upload to S3
                    s3_chart_path = f"{args.output.rstrip('/')}/ds={timestamp}/{chart_filename}"
                    upload_to_s3(local_chart_path, s3_chart_path, args.bucket)
            else:
                logger.info(f"No matching queries found for '{query_name}'")
    
    # Concatenate all HTTP logs into a single dataframe
    all_events = []
    for q in all_queries:
        if not q.events.empty:
            # Add query metadata to events
            events_copy = q.events.copy()
            events_copy['query_name'] = q.query_name
            events_copy['query_index'] = q.index
            events_copy['bbox_min_lon'] = q.bbox[0]
            events_copy['bbox_min_lat'] = q.bbox[1] 
            events_copy['bbox_max_lon'] = q.bbox[2]
            events_copy['bbox_max_lat'] = q.bbox[3]
            all_events.append(events_copy)
    
    if all_events:
        # Combine all events and save as parquet
        combined_events = pd.concat(all_events, ignore_index=True)
        
        # Save traffic data locally
        local_traffic_path = os.path.join(local_output_dir, "traffic.parquet")
        combined_events.to_parquet(local_traffic_path, index=False)
        logger.info(f"Saved combined traffic data to {local_traffic_path}")
        
        # Upload to S3
        s3_traffic_path = f"{args.output.rstrip('/')}/ds={timestamp}/traffic.parquet"
        upload_to_s3(local_traffic_path, s3_traffic_path, args.bucket)
        
        logger.info(f"Completed {len(all_queries)} queries")
        logger.info(f"Combined traffic data contains {len(combined_events)} events")
    else:
        logger.warning("No traffic events were recorded across all queries")
    
    # Optional: Print summary
    for q in all_queries:
        logger.info(f"{q.query_name}: {len(q.results)} results, {len(q.events)} events")
    
    proxy.shutdown()


if __name__ == "__main__":
    main()
