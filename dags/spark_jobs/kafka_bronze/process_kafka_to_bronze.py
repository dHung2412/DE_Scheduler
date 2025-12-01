"""
Kafka to Bronze Streaming Pipeline
====================================
Flow: Kafka (Avro Binary) → UDF Decode → Explode → Flatten → Iceberg Bronze

Features:
- Proper null handling for nested structs (actor, repo)
- Flattened schema for easy querying (no nested STRUCTs)
- Error logging in UDF for debugging
- Partitioned by ingestion_date
- Optimized for streaming with fanout write
"""
import logging
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from pathlib import Path
from typing import List
import fastavro
import io
import json
from dotenv import load_dotenv

from config_2 import Config_2
from spark_client import SparkClient

from pyspark.sql.functions import udf, col, explode, current_timestamp, to_date
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, BooleanType
from pyspark.sql import SparkSession


load_dotenv()

config = Config_2()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# __________________________ SCHEMA __________________________
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

    return actor_schema, repo_schema


def get_output_schema():
    actor_schema, repo_schema = get_nested_schemas()
    return StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", actor_schema, True),
        StructField("repo", repo_schema, True),   
        StructField("payload", StringType(), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
    ])


# __________________________ UDF DECODER (FIXED) __________________________
def create_avro_decoder_udf(avro_schema, spark_schema):

    def decode_avro_batch(binary_data: bytes) -> List[dict]:
        if binary_data is None or len(binary_data) == 0:
            return []
        
        try:
            bytes_reader = io.BytesIO(binary_data)
            records = []
            
            avro_reader = fastavro.reader(bytes_reader, reader_schema=avro_schema)

            for record in avro_reader:
                payload_val = record.get("payload")

                if isinstance(payload_val, (dict, list)):
                    record['payload'] = json.dumps(payload_val)

                elif payload_val is None:
                    record['payload'] = None
                
                # Đảm bảo các trường actor và repo có giá trị
                for field in ['actor', 'repo']:

                    if field in record:
                        val = record[field]

                        if val is None or (isinstance(val, dict) and not val):
                            record[field] = None

                        elif isinstance(val, dict):
                            if field in ['actor']:
                                record[field] = {
                                    'id': val.get('id'),
                                    'login': val.get('login'),
                                    'gravatar_id': val.get('gravatar_id'),
                                    'url': val.get('url'),
                                    'avatar_url': val.get('avatar_url')
                                }
                            elif field == 'repo':
                                record[field] = {
                                    'id': val.get('id'),
                                    'name': val.get('name'),
                                    'url': val.get('url')
                                }

                records.append(record)

            logger.debug(f"-----> [BRONZE] Decoded {len(records)} records from Avro batch")
            return records
        
        except Exception as e:
            logger.error(f"-----> [BRONZE] Error decoding Avro batch: {e}", exc_info=True)
            return []
    
    return udf(decode_avro_batch, ArrayType(spark_schema))


# __________________________ BRONZE TABLE __________________________
def load_sql_from_file(file_path: str, **kwargs) -> str:
    if not os.path.exists(file_path):     
        raise FileNotFoundError(f"-----> [BRONZE] Không tìm thấy file SQL tại: {file_path}")
    with open(file_path, 'r', encoding='utf-8')  as f:
        sql_content = f.read()

    return sql_content.format(**kwargs)

def create_bronze_table_if_not_exists(spark: SparkSession, catalog: str, namespace: str, table_name: str):
    catalog = config.ICEBERG_CATALOG
    namespace = config.BRONZE_NAMESPACE
    table_name = config.BRONZE_TABLE
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "bronze_table.sql")
    
    try: 
        # 1. Tạo namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        logger.info(f"-----> [BRONZE] Đảm bảo namespace {catalog}.{namespace} tồn tại.")
        
        # 2. Kiểm tra bảng đã tồn tại chưa
        if spark.catalog.tableExists(full_table_name):
            logger.info(f"-----> [BRONZE] Bảng {full_table_name} đã tồn tại.")
            return
        logger.info(f"-----> [BRONZE] Bảng {full_table_name} chưa tồn tại.")
        
        # 3. Tạo table theo schema
        create_table_sql = load_sql_from_file(
            str(sql_file_path),
            full_table_name = full_table_name
        )
        
        spark.sql(create_table_sql)
        logger.info(f"-----> [BRONZE] Đã tạo table {full_table_name}.")
    
    except Exception as e:
        logger.error(f"-----> [BRONZE] Lỗi khi tạo table: {e}")
        raise
    

# __________________________ MAIN STREAMING PIPELINE __________________________
def run_kafka_to_bronze_pipeline(spark, avro_schema):
    """
    Flow: Kafka (Avro Binary) → UDF Decode → Explode → Flatten → Iceberg Bronze
    """
    catalog = config.ICEBERG_CATALOG
    namespace = config.BRONZE_NAMESPACE
    table_name = config.BRONZE_TABLE
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    
    # 1. Tạo bronze table
    create_bronze_table_if_not_exists(
        spark,
        catalog,
        namespace,
        table_name
    )

    # 2. Đọc stream từ Kafka (binary mode)
    logger.info(f"-----> [BRONZE] Bắt đầu đọc từ Kafka topic: {config.KAFKA_TOPIC}.")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info(f"-----> [BRONZE] Đã kết nối thành công với Kafka.")

    # 3. Lấy spark schema
    spark_schema = get_output_schema()

    # 4. Tạo UDF decoder
    decode_avro_udf = create_avro_decoder_udf(avro_schema=avro_schema, spark_schema=spark_schema)
    
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
    
    # Flatten actor struct
    col("data.actor.id").alias("actor_id"),
    col("data.actor.login").alias("actor_login"),
    col("data.actor.gravatar_id").alias("actor_gravatar_id"),
    col("data.actor.url").alias("actor_url"),
    col("data.actor.avatar_url").alias("actor_avatar_url"),
    
    # Flatten repo struct
    col("data.repo.id").alias("repo_id"),
    col("data.repo.name").alias("repo_name"),
    col("data.repo.url").alias("repo_url"),
    
    col("data.payload"),
    col("data.public"),
    col("data.created_at"),
    col("ingestion_timestamp"),
    col("ingestion_date"),
    col("kafka_partition"),
    col("kafka_offset")
)

    # 9. Ghi vào Iceberg Bronze table
    full_table_name = f"{config.ICEBERG_CATALOG}.{config.BRONZE_NAMESPACE}.{config.BRONZE_TABLE}"
    logger.info(f"-----> [BRONZE] Bắt đầu ghi dữ liệu vào {full_table_name}")

    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config.BRONZE_CHECKPOINT_LOCATION) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="30 seconds") \
        .toTable(full_table_name)
    logger.info(f"-----> [BRONZE] Streaming query đã bắt đầu. Đang chờ dữ liệu")

    return query

def main():
    try:
       # 1. Khởi tạo Spark
        logger.info("-----> [BRONZE] Đang khởi tạo Spark Session...")
        spark_client = SparkClient(app_name="Kafka-to-Bronze-Fixed", job_type="streaming")
        spark = spark_client.get_session()
        
        # 2. Load Avro schema
        logger.info("-----> [BRONZE] Đang load Avro schema...")
        avro_schema = load_avro_schema(config.AVRO_SCHEMA_PATH)
        
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
