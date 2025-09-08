from abc import ABC, abstractmethod

class QueryEngine(ABC):
    def __init__(self, data_path, proxy_port, bbox=None, s3_region='us-west-2'):
        self.data_path = data_path

        if self.data_path.startswith("s3://"):
            self.data_path = self.data_path.replace("s3://", "s3a://")
        if not ".parquet" in self.data_path:
            self.data_path += "/"
        if self.data_path.endswith("/"):
            self.data_path += "*.parquet"
        
        self.proxy_port = proxy_port
        self.s3_region = s3_region
        self.bbox = bbox

    @abstractmethod
    def run_bbox_query(self, columns="*"):
        pass

import duckdb

class DuckDBEngine(QueryEngine):
    def __init__(self, data_path, proxy_port, bbox=None, s3_region='us-west-2'):
        super().__init__(data_path, proxy_port, bbox, s3_region)
        
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
    
    def run_bbox_query(self, columns="*"):
        query = f"""
        SELECT 
            {columns}
        FROM read_parquet('{self.data_path}')
        WHERE (bbox.xmin <= {self.bbox[2]} AND bbox.xmax >= {self.bbox[0]})
          AND (bbox.ymin <= {self.bbox[3]} AND bbox.ymax >= {self.bbox[1]})
        """
        return self.con.sql(query).df()

from pyspark.sql import SparkSession

class SparkEngine(QueryEngine):
    def __init__(self, data_path, proxy_port, bbox=None, s3_region='us-west-2'):
        super().__init__(data_path, proxy_port, bbox, s3_region)
        self.spark = SparkSession.builder \
            .appName("ParquetBenchmark") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{self.s3_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.proxy.host", "localhost") \
            .config("spark.hadoop.fs.s3a.proxy.port", str(proxy_port)) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()
    
    def run_bbox_query(self, columns="*"):
        df = self.spark.read.parquet(self.data_path)
        if self.bbox:
            df = df.filter(
                (df.bbox.xmin <= float(self.bbox[2])) & (df.bbox.xmax >= float(self.bbox[0])) &
                (df.bbox.ymin <= float(self.bbox[3])) & (df.bbox.ymax >= float(self.bbox[1]))
            )
        return df.selectExpr(columns).toPandas()