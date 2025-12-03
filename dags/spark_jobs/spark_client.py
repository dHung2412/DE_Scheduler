import os
import sys
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from pyspark.sql import SparkSession
from config_2 import Config_2

config = Config_2()

class SparkClient:
    def __init__(self, app_name: str, job_type: str = "batch"):
        """
        job_type: 
            'streaming' -> Tốn tài nguyên
            'batch'     -> Tiết kiệm tài nguyên
        """
        self.app_name = app_name
        self.job_type = job_type
        self.logger = logging.getLogger(__name__)

        self.logger.info(f"-----> [DEBUG] Python Executable: {sys.executable}")

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    def get_session(self):
        self.logger.info(f"-----> [SPARK_CLIENT] [{self.app_name}] Khởi tạo Spark Session ({self.job_type})...")    

        is_docker = os.getenv("HOSTNAME") is not None and not os.getenv("HOSTNAME").startswith("DESKTOP")
        
        minio_host = "minio:9000" if is_docker else "localhost:9000"
        rest_host = "rest:8181" if is_docker else "localhost:8181"
        
        self.logger.info(f"-----> [SPARK_CLIENT] Environment: {'Docker' if is_docker else 'Local'}")
        self.logger.info(f"-----> [SPARK_CLIENT] MinIO: {minio_host}, REST Catalog: {rest_host}")

        master_conf = "local[*]" if self.job_type == "streaming" else "local[2]"

        builder= SparkSession.builder \
            .appName(self.app_name) \
            .master(master_conf) \
            \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_host}") \
            .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ROOT_USER) \
            .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_ROOT_PASSWORD) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "rest") \
            .config("spark.sql.catalog.demo.uri", f"http://{rest_host}") \
            .config("spark.sql.catalog.demo.warehouse", config.ICEBERG_WAREHOUSE_PATH) \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.demo.s3.endpoint", f"http://{minio_host}") \
            .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
            .config("spark.sql.catalog.demo.s3.connection.ssl.enabled", "false") \
            .config("spark.sql.catalog.demo.s3.access-key-id", config.MINIO_ROOT_USER) \
            .config("spark.sql.catalog.demo.s3.secret-access-key", config.MINIO_ROOT_PASSWORD) \
            .config("spark.sql.catalog.demo.client.region", "us-east-1") \
            \
            .config("spark.sql.defaultCatalog", "demo")
        
        if self.job_type == "streaming":
            builder = builder \
                .config("spark.python.worker.reuse", "true") \
                .config("spark.network.timeout", "600s") \
                .config("spark.executor.heartbeatInterval", "60s")
            
        return builder.getOrCreate()