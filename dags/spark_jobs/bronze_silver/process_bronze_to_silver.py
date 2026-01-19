"""
Bronze to Silver Layer Processing
=================================
Pipeline xử lý dữ liệu từ tầng Bronze (Iceberg) sang tầng Silver (Iceberg).
Flow:
    Bronze Iceberg Table
    -> Incremental Load (Filter by ingestion_timestamp)
    -> Deduplication (Drop duplicates by ID)
    -> Transformation (Parse JSON payload, casting types)
    -> Silver Iceberg Table (Merge: Insert on not matched)
"""

import logging
import os
import sys
from typing import List
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, current_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config_2 import Config_2
from spark_client import SparkClient
from utils.helper.load_sql import load_sql_from_file

config = Config_2()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# =================== Schema ===================

def get_unified_payload_schema() -> StructType:
    return StructType([
        StructField("action", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("head", StringType(), True),
        
        StructField("pull_request", StructType([
            StructField("id", LongType(), True),
            StructField("state", StringType(), True),
            StructField("title", StringType(), True),
            StructField("merged", BooleanType(), True),
            StructField("merged_at", StringType(), True),
        ]), True),
    
        StructField("issue", StructType([
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("state", StringType(), True),
        ]), True)
    ])

# =================== Table ===================

def create_silver_table_if_not_exists(spark: SparkSession, catalog_name: str, namespace_name: str, full_table_path: str):
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "silver_table.sql")

    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog_name}.{namespace_name}")

        if spark.catalog.tableExists(full_table_path):
            logger.info(f"Table {full_table_path} already exists.")
            return
        
        logger.info(f"-----> Reading DDL from: {sql_file_path}.")
        create_sql = load_sql_from_file(
            str(sql_file_path),
            silver_table=full_table_path
        )

        spark.sql(create_sql)
        logger.info(f"-----> Create Table: {full_table_path}.")

    except Exception as e:
        logger.error(f"-----> Failed create Table: {e}.")
        raise

def get_last_processed_timestamp(spark: SparkSession, full_table_path: str) -> str:
    default_timestamp = "1990-01-01 00:00:00"

    try:
        if not spark.catalog.tableExists(full_table_path):
            logger.info(f"-----> Table {full_table_path} not found. Full load triggered")
            return default_timestamp

        row = spark.sql(f"SELECT MAX(processed_at) as max_ts FROM {full_table_path}").first()

        if row and row['max_ts']:
            max_ts = row['max_ts']
            logger.info(f"-----> Last processed timestamp: {max_ts}")
            return str(max_ts)
        else:
            logger.info(f"-----> Silver table {full_table_path} is empty. Full load triggered.")
            return default_timestamp
    
    except Exception as e:
        logger.warning(f"-----> Error reading watermark: {e}. Defaulting to full load.")
        return default_timestamp

# =================== Transformation logic ===================

def _get_column_expression() -> List[Column]:
    p = col("payload_parsed")

    cols = [
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        col("created_at_ts").alias("created_at"),
        col("public"),
        col("actor_id"),
        col("actor_login"),
        col("actor_url"),
        col("actor_avatar_url"),
        col("repo_id"),
        col("repo_name"),
        col("repo_url"),
        col("ingestion_date"),
        current_timestamp().alias("processed_at")
    ]

    cols.extend([
        p.getItem("action").alias("payload_action"),
        p.getItem("ref").alias("payload_ref"),
        p.getItem("ref_type").alias("payload_ref_type"),
        p.getItem("size").alias("push_size"),
        p.getItem("distinct_size").alias("push_distinct_size"),
        p.getItem("head").alias("push_head_sha"),

        p.getItem("pull_request").getItem("id").alias("pull_request_id"),
        p.getItem("pull_request").getItem("number").alias("pull_request_number"),
        p.getItem("pull_request").getItem("state").alias("pull_request_state"),
        p.getItem("pull_request").getItem("title").alias("pull_request_title"),
        p.getItem("pull_request").getItem("merged").alias("pull_request_merge"),
        
        when(p.getItem("pull_request").getItem("merged_at").isNotNull(),
        to_timestamp(p.getItem("pull_request").getItem("merged_at"))).alias("pull_request_merge_at"),

        p.getItem("issue").getItem("number").alias("issue_number"),
        p.getItem("issue").getItem("title").alias("issue_title"),
        p.getItem("issue").getItem("state").alias("issue_state")
    ])

    return cols

def run_process_bronze_to_silver(spark: SparkSession):
    catalog = config.ICEBERG_CATALOG
    bronze_table_full = f"{catalog}.{config.BRONZE_NAMESPACE}.{config.BRONZE_TABLE}"
    silver_table_full = f"{catalog}.{config.SILVER_NAMESPACE}.{config.SILVER_TABLE}"

    logger.info(f"-----> Start processing {bronze_table_full} -> {silver_table_full}")
    # 1. Initialize table
    create_silver_table_if_not_exists(spark, catalog, config.SILVER_NAMESPACE, silver_table_full)

    # 2. Get watermark
    last_ts = get_last_processed_timestamp(spark, silver_table_full)

    # 3. Read Incremental Data
    logger.info(f"-----> Reading changes since {last_ts}")
    bronze_df = spark.table(bronze_table_full) \
        .filter(col("ingestion_timestamp") > to_timestamp(lit(last_ts)))

    # Deduplication
    bronze_df = bronze_df.dropDuplicates(["id"])
    count = bronze_df.count()

    if count == 0:
        logger.info("-----> No new data found. Skipping.")
        return
    
    # 4. Transformations
    logger.info("-----> Applying Transformation & parsing JSON.")
    transformed_df = (
        bronze_df
        .withColumn("created_at_ts", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("payload_parsed", from_json(col("payload"), get_unified_payload_schema()))
        .select(*_get_column_expression()) # Unpacking Operator
    )

    # 5. Merge
    logger.info(f"-----> Merging data into {silver_table_full}")
    transformed_df.createOrReplaceTempView("batch_source_data")

    merge_sql = f"""
    MERGE INTO {silver_table_full} AS target
    USING batch_source_data AS source 
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT *
    """
    spark.sql(merge_sql)

    logger.info(f"-----> Pipeline Finished.")

def main():
    spark = None
    try:
        logger.info("[SILVER] --------- Initialize Spark Session.")
        spark_client = SparkClient(app_name="bronze-to-silver", job_type="batch")
        spark_client.get_session()

        logger.info("[SILVER] --------- Starting Batch Pipeline.")
        run_process_bronze_to_silver(spark)

        logger.info("[SILVER] --------- Pipeline is runnign. Ctrl + C to stop.")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("[SILVER] --------- Interrupt Pipeline.")

    except Exception as e:
        logger.error("[SILVER] --------- Critical error in Pipeline.", exc_info=True)
        raise

    finally:
        if spark:
            try:
                spark.stop()
                logger.info("[SILVER] --------- Spark Session stopped.")
            except Exception as e:
                logger.error(f"[SILVER] --------- Error stopping Spark: {e}.")

        logger.info("[SILVER] --------- Shutdown complete.")

if __name__ == "__main__":
    main()