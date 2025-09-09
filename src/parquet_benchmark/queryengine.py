from abc import ABC, abstractmethod

class QueryEngine(ABC):
    def __init__(self, proxy_port, s3_region='us-west-2'):
        self.proxy_port = proxy_port
        self.s3_region = s3_region
        self.engine = None
        self.query_count = 0
    
    def fix_path(self, path):
        data_path = path
        if data_path.startswith("s3://"):
            data_path = data_path.replace("s3://", "s3a://")
        if data_path.endswith("*"):
            data_path += ".parquet"
        if not ".parquet" in data_path:
            data_path += "/"
        if data_path.endswith("/"):
            data_path += "*.parquet"
        return data_path

    def run_bbox_query(self, data_path, bbox, columns="*"):
        self.query_count += 1
        return self._run_bbox_query(self.fix_path(data_path), bbox, columns)

    @abstractmethod
    def _run_bbox_query(self, data_path, bbox, columns="*"):
        pass

import duckdb

class DuckDBEngine(QueryEngine):
    def __init__(self, proxy_port, s3_region='us-west-2'):
        super().__init__(proxy_port, s3_region)
        
        import os
        print(f"Checking AWS credentials accessibility...")
        aws_dir = "/home/appuser/.aws"
        if os.path.exists(aws_dir):
            print(f"AWS directory exists: {aws_dir}")
            for file in os.listdir(aws_dir):
                file_path = os.path.join(aws_dir, file)
                print(f"  {file}: exists={os.path.exists(file_path)}, readable={os.access(file_path, os.R_OK)}")
        else:
            print(f"AWS directory does not exist: {aws_dir}")

        query = f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            REGION '{self.s3_region}'
        );
        """
        self.con = duckdb.connect()

        self.con.sql(query)
        for ext in ['s3', 'spatial', 'httpfs']:
            self.con.install_extension(ext)
            self.con.load_extension(ext)
        self.con.execute(f"SET http_proxy = 'http://localhost:{self.proxy_port}'")
        self.con.execute(f"SET s3_region = '{self.s3_region}'")

        self.engine = "DuckDB"
    
    def _run_bbox_query(self, data_path, bbox, columns="*"):
        query = f"""
        SELECT 
            {columns}
        FROM read_parquet('{data_path}')
        WHERE (bbox.xmin <= {bbox[2]} AND bbox.xmax >= {bbox[0]})
          AND (bbox.ymin <= {bbox[3]} AND bbox.ymax >= {bbox[1]})
        """
        pandas_df = self.con.sql(query).df()
        pandas_df['engine'] = self.engine
        pandas_df['query_count'] = self.query_count
        return pandas_df

from pyspark.sql import SparkSession
from sedona.spark import *
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext
import logging

class SparkEngine(QueryEngine):
    def __init__(self, proxy_port, s3_region='us-west-2'):
        super().__init__(proxy_port, s3_region)
        
        # Quiet py4j logging
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)
        
        config = (
            SedonaContext.builder()
            .master("local[*]")
            .appName("ParquetBenchmark")
            .config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.0,"
                "org.datasyslab:geotools-wrapper:1.7.0-28.5,"
                "org.apache.hadoop:hadoop-common:3.3.4,"
                "com.google.code.gson:gson:2.9.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.parquet:parquet-hadoop:1.15.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{s3_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.proxy.host", "localhost")
            .config("spark.hadoop.fs.s3a.proxy.port", str(proxy_port))
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.serializer", KryoSerializer.getName)
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
            .config("spark.sql.parquet.enableNestedColumnVectorizedReader", "false")
            .config("spark.sql.parquet.filterPushdown", "false")
            .config("spark.sql.parquet.mergeSchema", "false")
            .getOrCreate()
        )
        
        self.spark = SedonaContext.create(config)
        self.engine = "PySpark+Sedona"
    
    def _run_bbox_query(self, data_path, bbox, columns="*"):
        df = self.spark.read.parquet(data_path)
        if bbox:
            df = df.filter(
                (df.bbox.xmin <= float(bbox[2])) & (df.bbox.xmax >= float(bbox[0])) &
                (df.bbox.ymin <= float(bbox[3])) & (df.bbox.ymax >= float(bbox[1]))
            )
        pandas_df = df.selectExpr(columns).toPandas()
        pandas_df['engine'] = self.engine
        pandas_df['query_count'] = self.query_count
        return pandas_df