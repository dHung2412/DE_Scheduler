"""
Kafka (Binary) → UDF Decode → Array[JSON] → Explode → Parse JSON → Iceberg Bronze
"""
from pathlib import Path
from typing import List
from pyspark.sql.functions import udf, col, explode, current_timestamp, to_date
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, BooleanType
import fastavro
import io
import json
import logging
from dotenv import load_dotenv
from config_2 import Config_2
from utils.spark_client import SparkClient

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
        config.ICEBERG_CATALOG,
        config.ICEBERG_NAMESPACE,
        config.ICEBERG_TABLE
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
    full_table_name = f"{config.ICEBERG_CATALOG}.{config.ICEBERG_NAMESPACE}.{config.ICEBERG_TABLE}"

    logger.info(f"-----> [BRONZE] Bắt đầu ghi dữ liệu vào {full_table_name}")

    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config.CHECKPOINT_LOCATION) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="3 minute") \
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
        spark_client = SparkClient(app_name="Kafka-to-Bronze", job_type="streaming")
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