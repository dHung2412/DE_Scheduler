"""
Kafka (Binary) → UDF Decode → Array[JSON] → Explode → Parse JSON → Iceberg Bronze
"""
import sys
from pathlib import Path
from typing import List
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, current_timestamp, to_date
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, BooleanType
import fastavro
import io
import json
import logging
import os
import site
from dotenv import load_dotenv
from config_2 import Config_2

load_dotenv()

config2 = Config_2()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def create_spark_session():

    try:
        sc = SparkContext.getOrCreate()
        if sc._jsc:
            logger.warning("-----> [BRONZE] Đang dừng Spark Context cũ...")
            sc.stop()

    except Exception as e:
        logger.info(f"-----> [BRONZE] Không có Spark Context nào đang chạy: {e}")
    
    logger.info("-----> [BRONZE] Đang khởi tạo Spark Session...")
    
    spark_home = os.environ.get('SPARK_HOME', r"D:\config\spark-3.3.4-bin-hadoop3")

    jars_dir = os.path.join(spark_home, "jars")
    
    required_jars = [
        "spark-sql-kafka-0-10_2.12-3.3.4.jar",
        "kafka-clients-2.8.1.jar",
        "commons-pool2-2.11.1.jar",
        "spark-token-provider-kafka-0-10_2.12-3.3.4.jar",
        "iceberg-spark-runtime-3.3_2.12-1.4.3.jar",
        "iceberg-aws-bundle-1.4.3.jar",
        "hadoop-aws-3.3.4.jar",
        "aws-java-sdk-bundle-1.12.262.jar"
    ]

    valid_jars = []
    missing_jars = []
    
    for jar_name in required_jars:

        jar_path = os.path.join(jars_dir, jar_name)

        if os.path.exists(jar_path):
            valid_jars.append(jar_path)
            logger.debug(f"-----> [BRONZE] Tìm thấy JAR: {jar_name}")

        else:
            missing_jars.append(jar_name)
            logger.warning(f"-----> [BRONZE] Không tìm thấy JAR: {jar_name}")
    
    if missing_jars:
        logger.error(f"-----> [BRONZE] Thiếu {len(missing_jars)} JARs: {missing_jars}")
        logger.error("-----> [BRONZE] Vui lòng download các JARs còn thiếu từ Maven Central")
    
    jars_conf = ",".join(valid_jars)
    
    venv_python = r"D:\Project\Data_Engineering\DE_Scheduler\.venv\Scripts\python.exe"
    os.environ['PYSPARK_PYTHON'] = venv_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python    

    if not os.path.exists(venv_python):
        logger.warning(f"-----> [BRONZE] Python venv không tồn tại: {venv_python}")
        venv_python = sys.executable
    
    spark = SparkSession.builder \
        .appName("Kafka-to-Bronze-Iceberg") \
        .master("local[*]") \
        .config("spark.jars", jars_conf) \
        .config("spark.pyspark.python", venv_python) \
        .config("spark.pyspark.driver.python", venv_python) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.ui.port", "4040") \
        \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", config2.MINIO_ROOT_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", config2.MINIO_ROOT_PASSWORD) \
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
        .config("spark.sql.catalog.demo.warehouse", config2.ICEBERG_WAREHOUSE_PATH) \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://localhost:9000") \
        .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
        .config("spark.sql.catalog.demo.s3.access-key-id", config2.MINIO_ROOT_USER) \
        .config("spark.sql.catalog.demo.s3.secret-access-key", config2.MINIO_ROOT_PASSWORD) \
        .config("spark.sql.catalog.demo.client.region", "us-east-1") \
        \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("-----> [BRONZE] Spark Session đã được khởi tạo thành công.")
    return spark

def load_avro_schema(schema_path: str):
    try:
        schema_file = Path(schema_path)
        
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_dict = json.load(f)

        parsed_schema = fastavro.parse_schema(schema_dict)
        logger.info(f"-----> [BRONZE] Đã load avro schema từ {schema_path}.")
        return parsed_schema
    
    except Exception as e:
        logger.error(f"-----> [BRONZE] Lỗi khi load Avro schema: {e}.")
        raise

# __________________________ SCHEMA __________________________
def get_nested_schemas():
    actor_schema = StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ])
    repo_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ])
    org_schema = StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ])

    return actor_schema, repo_schema, org_schema

def get_output_schema():
    actor_schema, repo_schema, org_schema = get_nested_schemas()

    return StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", actor_schema, True),
        StructField("repo", repo_schema, True),   
        StructField("payload", StringType(), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("org", org_schema, True) 
    ])

# __________________________ UDF DECODER __________________________
def create_avro_decoder_udf(avro_schema, spark_schema):
    """
    Tạo UDF để giải mã binary Avro thành array of Structs
    """

    def decode_avro_batch(binary_data: bytes) -> List[dict]:
        """
        Decode một batch Avro binary thành list of dicts
        """
        if binary_data is None or len(binary_data) == 0:
            return []
        
        try:
            bytes_reader = io.BytesIO(binary_data)
            records = []
            avro_reader = fastavro.reader(bytes_reader, reader_schema=avro_schema)

            for record in avro_reader:
                if isinstance(record.get('payload'), dict):
                    record['payload'] = json.dumps(record['payload'])
                records.append(record)

            logger.debug(f"-----> [BRONZE] Decoded {len(records)} records from Avro batch")
            return records
        
        except Exception as e:
            logger.error(f"-----> [BRONZE] Lỗi khi decode avro: {e}.")
            return []
    
    return udf(decode_avro_batch, ArrayType(spark_schema))


# __________________________ BRONZE TABLE __________________________
def create_bronze_table_if_not_exists(spark, catalog, namespace, table_name):
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    
    try: 
        # 1. Tạo namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        logger.info(f"-----> [BRONZE] Đảm bảo namespace {catalog}.{namespace} tồn tại.")
        
        # 2. Xóa bảng cũ (nếu cần)
        logger.warning(f"-----> [BRONZE] Đang xóa bảng cũ {full_table_name} để cập nhật lại.")
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        
        # 3. Tạo table
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id STRING,
                type STRING,
                actor STRUCT<
                    id: BIGINT,
                    login: STRING,
                    gravatar_id: STRING,
                    url: STRING,
                    avatar_url: STRING
                >,
                repo STRUCT<
                    id: BIGINT,
                    name: STRING,
                    url: STRING
                >,
                payload STRING,
                public BOOLEAN,
                created_at STRING,
                org STRUCT<
                    id: BIGINT,
                    login: STRING,
                    gravatar_id: STRING, 
                    url: STRING,
                    avatar_url: STRING
                >,
                ingestion_timestamp TIMESTAMP,
                ingestion_date DATE,
                kafka_partition INT,
                kafka_offset BIGINT
            )
            USING iceberg
            PARTITIONED BY (ingestion_date)
            """
        
        spark.sql(create_table_sql)
        logger.info(f"-----> [BRONZE] Đảm bảo table {full_table_name} đã tồn tại.")
    
    except Exception as e:
        logger.error(f"-----> [BRONZE] Lỗi khi tạo table: {e}")
        raise

# __________________________ MAIN STREAMING PIPELINE __________________________
def run_kafka_to_bronze_pipeline(spark, avro_schema):
    """
    Main streaming pipeline:
    Kafka Binary → Decode Avro → Explode → Parse → Iceberg Bronze
    """

    # 1. Tạo bronze table
    create_bronze_table_if_not_exists(
        spark,
        config2.ICEBERG_CATALOG,
        config2.ICEBERG_NAMESPACE,
        config2.ICEBERG_TABLE
    )

    # 2. Đọc stream từ Kafka (binary mode)
    logger.info(f"-----> [BRONZE] Bắt đầu đọc từ Kafka topic: {config2.KAFKA_TOPIC}.")
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config2.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config2.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info(f"-----> [BRONZE] Đã kết nối thành công với Kafka.")

    # 3. Lấy spark schema
    spark_schema = get_output_schema()

    # 4. Tạo UDF decoder
    decode_avro_udf = create_avro_decoder_udf(avro_schema=avro_schema,
                                              spark_schema=spark_schema)
    
    # 5. Decode Avro binary + Array of Structs
    decoded_df = kafka_df.select(
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        decode_avro_udf(col("value")).alias("records_array")
    )

    # 6. Explode array thành individual rows
    exploded_df = decoded_df.select(
        col("kafka_partition"),
        col("kafka_offset"),
        explode(col("records_array")).alias("data")
    )
    
    # 7. Thêm timestamp
    with_ts_df = exploded_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", to_date(col("ingestion_timestamp")))
    
    # 8. Làm phẳng Struct
    parsed_df = with_ts_df.select(
        col("data.id"),
        col("data.type"),
        col("data.actor"),
        col("data.repo"),
        col("data.payload"),
        col("data.public"),
        col("data.created_at"),
        col("data.org"),
        col("ingestion_timestamp"),
        col("ingestion_date"),
        col("kafka_partition"),
        col("kafka_offset")
    )

    # 9. Ghi vào Iceberg Bronze table
    full_table_name = f"{config2.ICEBERG_CATALOG}.{config2.ICEBERG_NAMESPACE}.{config2.ICEBERG_TABLE}"

    logger.info(f"-----> [BRONZE] Bắt đầu ghi dữ liệu vào {full_table_name}")

    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config2.CHECKPOINT_LOCATION) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="10 seconds") \
        .toTable(full_table_name)
    
    logger.info(f"-----> [BRONZE] Streaming query đã bắt đầu. Đang chờ dữ liệu")

    # logger.info(f"-----> [DEBUG MODE] Đang ghi dữ liệu ra CONSOLE để kiểm tra...")
    # query = parsed_df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .option("truncate", "false") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()
    
    return query

def main():
    try:
       # 1. Khởi tạo Spark
        logger.info("-----> [BRONZE] Đang khởi tạo Spark Session...")
        spark = create_spark_session()
        
        # 2. Load Avro schema
        logger.info("-----> [BRONZE] Đang load Avro schema...")
        avro_schema = load_avro_schema(config2.AVRO_SCHEMA_PATH)
        
        # 3. Chạy streaming pipeline
        logger.info("-----> [BRONZE] Đang khởi động streaming pipeline...")
        query = run_kafka_to_bronze_pipeline(spark, avro_schema)
        
        # 4. Chờ terminate
        logger.info("-----> [BRONZE] Pipeline đang chạy. Nhấn Ctrl+C để dừng.")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info(f"-----> [BRONZE] Nhận tín hiệu dừng từ người dùng.")
    
    except Exception as e:
        logger.error(f"-----> [BRONZE] Lỗi trong pipeline: {e}", exc_info=True)
        raise

    finally:
        logger.info(f"-----> [BRONZE] Pipeline đã dừng")

if __name__ == "__main__":
    main()