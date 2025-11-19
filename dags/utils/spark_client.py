import os
import sys
import logging
from pyspark.sql import SparkSession
from spark_jobs.config_2 import Config_2

config = Config_2()

class SparkClient:
    def __init__(self, app_name: str, job_type: str = "batch"):
        """
        app_name
        job_type: 'streaming' -> Tốn tài nguyên
                  'bathc' -> Tiết kiệm tài nguyên
        """
        self.app_name = app_name
        self.job_type = job_type
        self.logger = logging.getLogger(__name__)

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark_home = os.environ.get('SPARK_HOME')
        self.jars_dir = os.path.join(self.spark_home, "jars")

        self.required_jars = [
            "hadoop-aws-3.3.4.jar",
            "iceberg-spark-runtime-3.3_2.12-1.4.3.jar",
            "iceberg-aws-bundle-1.4.3.jar",
            "aws-java-sdk-bundle-1.12.262.jar",
            "spark-sql-kafka-0-10_2.12-3.3.4.jar",
            "kafka-clients-2.8.1.jar",
            "commons-pool2-2.11.1.jar",
            "spark-token-provider-kafka-0-10_2.12-3.3.4.jar"
        ]

    def _get_jars_conf(self):

        valid_jars = []

        for jar_name in self.required_jars:
            jar_path = os.path.join(self.jars_dir, jar_name)

            if os.path.exists(jar_path):
                valid_jars.append(jar_path)

            else:
                self.logger.warning(f"-----> [WARNING] Thiếu JAR: {jar_path}")

        return ",".join(valid_jars)
    
    def get_session(self):
        
        self.logger.info(f"-----> [SPARK_CLIENT] [{self.app_name}] Khởi tạo Spark Session ({self.job_type})...")    

        jars_conf = self._get_jars_conf()

        master_conf = "local[*]" if self.job_type == "streaming" else "local[2]"

        builder= SparkSession.builder \
            .appName(self.app_name) \
            .master(master_conf) \
            .config("spark.jars", jars_conf) \
            \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ROOT_USER) \
            .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_ROOT_PASSWORD) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
            \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "rest") \
            .config("spark.sql.catalog.demo.uri", "http://localhost:8181") \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.demo.warehouse", config.ICEBERG_WAREHOUSE_PATH) \
            .config("spark.sql.catalog.demo.s3.endpoint", "http://localhost:9000") \
            .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
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