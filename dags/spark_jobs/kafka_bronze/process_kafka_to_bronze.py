"""
Kafka to Bronze Streaming Pipeline
==================================
Pipeline xử lý dữ liệu Streaming từ Kafka, giải mã Avro và ghi vào Iceberg Table (Bronze Layer).
Flow:
    Kafka (Avro Binary) 
    -> UDF Decode (chuyển đổi Binary -> List of Dicts)
    -> Explode (List -> Rows)
    -> Transform (Flatten Structs, ép kiểu)
    -> Iceberg Bronze Table (Partitioned by ingestion_date)
"""

import json
import io
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import fastavro
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, explode, current_timestamp, to_date, collect_set
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, BooleanType

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config_2 import Config_2
from spark_client import SparkClient
from utils.helper.load_sql import load_sql_from_file

load_dotenv()
config = Config_2()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


# =================== SCHEMA ===================

def load_avro_schema(schema_path: str) -> dict:
    try:
        schema_file = Path(schema_path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_dict = json.load(f)
        
        parsed_schema = fastavro.parse_schema(schema_dict)
        logger.info(f"-----> Loaded Avro schema from {schema_path}")
        return parsed_schema

    except Exception as e:
        logger.error(f"-----> Failed to load Avro schema: {e}")
        raise

def get_spark_schema() -> StructType:
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

    return StructType([
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("actor", actor_schema, True),
        StructField("repo", repo_schema, True),
        StructField("payload", StringType(), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
    ])

# =================== Transformation logic ===================

def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    # 1. Handle payload
    payload_val = record.get("payload")
    if isinstance(payload_val, (dict, list)):
        record['payload'] = json.dumps(payload_val)
    elif payload_val is None:
        record['payload'] = None

    # 2. Handle nested structs
    for field in ['actor', 'repo']:
        if field not in record:
            continue
        
        val = record[field]
        if val is None or (isinstance(val, dict) and not val):
            record[field] = None
            continue

        if isinstance(val, dict):
            if field == 'actor':
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
        
    return record

def create_avro_decoder_udf(avro_schema: dict, spark_schema: StructType):

    def decode_avro_batch(binary_data: Optional[bytes]) -> List[Dict[str, Any]]:
        if not binary_data: 
            return []

        try: 
            bytes_reader = io.BytesIO(binary_data)
            records = []

            avro_reader = fastavro.reader(bytes_reader, reader_schema=avro_schema)

            for record in avro_reader:
                cleaned_record = _normalize_record(record)
                records.append(cleaned_record)
            
            logger.info(f"-----> Decoded {len(records)} records")
            return records

        except Exception as e:
            logger.error(f"-----> Failed to decoding batch: {e}", exc_info=True)
            return []

    return udf(decode_avro_batch, ArrayType(spark_schema))

# =================== Table ===================

def create_bronze_table_if_not_exists(spark: SparkSession, catalog_name: str, namespace_name: str, full_table_path: str):
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "bronze_table.sql")
    
    try: 
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace_name}")
        
        if spark.catalog.tableExists(full_table_path):
            logger.info(f"-----> Table {full_table_path} already exists.")
            return
        
        logger.info(f"-----> Reading DDL from: {sql_file_path}.")
        create_table_sql = load_sql_from_file(
            str(sql_file_path),
            full_table_name = full_table_path
        )
        
        spark.sql(create_table_sql)
        logger.info(f"-----> Create Table: {full_table_path}.")
    
    except Exception as e:
        logger.error(f"-----> Failed create Table: {e}.")
        raise

# =================== Streaming Logic ===================

def write_batch_to_bronze(batch_df: DataFrame, batch_id: int, bronze_table: str):
    try:
        batch_df.cache()
        record_count = batch_df.count()

        if record_count == 0:
            logger.info(f"-----> Batch {batch_id} dont have new data")
            return
        
        logger.info(f"-----> Batch {batch_id}: Processing {record_count} records")

        try:
            kafka_partitions_row = batch_df.select(collect_set("kafka_partition").alias("partitions")).first()
            if kafka_partitions_row and kafka_partitions_row.partitions:
                parts = sorted(kafka_partitions_row.partitions)
                logger.info(f"-----> Batch {batch_id} --- Partitions: {parts}")
        
        except Exception as e:
            logger.warning(f"-----> Cound not inspect partitions in Batch {batch_id} : {e}")

        logger.info(f"-----> Batch {batch_id} -> writing -> {bronze_table}")
        batch_df.writeTo(bronze_table) \
            .option("fanout-enabled", "True") \
            .append()
        
        logger.info(f"-----> Batch {batch_id}: Write successful")
    
    except Exception as e:
        logger.error(f"-----> Batch {batch_id}: Write failed {e}", exc_info=True)
        raise
    finally:
        try:
            batch_df.unpersist()
        except Exception:
            pass

def run_process_kafka_to_bronze(spark: SparkSession, avro_schema: dict) -> StreamingQuery:
    catalog = config.ICEBERG_CATALOG
    namespace = config.BRONZE_NAMESPACE
    table_name = config.BRONZE_TABLE
    bronze_table_full = f"{catalog}.{namespace}.{table_name}"

    # 1. Initialize table
    create_bronze_table_if_not_exists(spark, catalog, namespace, bronze_table_full)

    # 2. Read stream from kafka
    logger.info(f"Reading from Kafka topic : {config.KAFKA_TOPIC}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # 3. Create Decoder UDF
    spark_schema = get_spark_schema()
    decode_avro_udf = create_avro_decoder_udf(avro_schema, spark_schema)

    # 4. Transformations
    processed_df = (
        kafka_df
        .select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            decode_avro_udf(col("value")).alias("records_array")
        )
        .select(
            col("kafka_partition"),
            col("kafka_offset"),
            explode("records_array").alias("data")
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", to_date("ingestion_timestamp"))
        .select(
            col("data.id"),
            col("data.type"),
            col("data.actor.id").cast("long").alias("actor_id"),
            col("data.actor.login").alias("actor_login"),
            col("data.actor.gravatar_id").alias("actor_gravatar_id"),
            col("data.actor.url").alias("actor_url"),
            col("data.actor.avatar_url").alias("actor_avatar_url"),
            col("data.repo.id").cast("long").alias("repo_id"),
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
    )

    # 5. Write stream
    logger.info(f"Starting stream write to {bronze_table_full}")
    query = processed_df.writeStream \
        .foreachBatch(lambda df, batch_id: write_batch_to_bronze(df, batch_id, bronze_table_full)) \
        .option("checkpointLocation", config.BRONZE_CHECKPOINT_LOCATION) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

# =================== MAIN ===================
def main():
    spark = None
    query = None
    try:
        logger.info("[BRONZE] --------- Initialize Spark Session")
        spark_client = SparkClient(app_name="kafka-to-bronze", job_type="streaming")
        spark = spark_client.get_session()

        logger.info("[BRONZE] --------- Loading avro schema")
        avro_schema = load_avro_schema(config.AVRO_SCHEMA_PATH)

        logger.info("[BRONZE] --------- Starting Pipeline")
        query = run_process_kafka_to_bronze(spark, avro_schema)

        logger.info("[BRONZE] --------- Pipeline is running. Ctrl + C to stop")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("[BRONZE] --------- Interrupt Pipeline")
    
    except Exception:
        logger.error("[BRONZE] --------- Critical error in Pipeline", exc_info=True)
        raise

    finally:
        logger.info("Shutting down")
        if query and query.isActive:
            try:
                query.stop()
                logger.info("[BRONZE] --------- Streaming Query stopped")
            except Exception as e:
                logger.error(f"[BRONZE] --------- Error stoping query: {e}")
        
        if spark:
            try:
                spark.stop()
                logger.info("[BRONZE] --------- Spark Session stopped")
            except Exception as e:
                logger.error(f"[BRONZE] --------- Error stopping Spark: {e}")

        logger.info("[BRONZE] --------- Shutdown complete")

if __name__== "__main__":
    main()