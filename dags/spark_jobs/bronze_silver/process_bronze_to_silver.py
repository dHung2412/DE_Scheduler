"""
Bronze to Silver Layer Processing
====================================
Flow: Bronze Iceberg → Parse JSON → Deduplication → Incremental Load → Silver Iceberg

Schema-driven approach:
- Sử dụng SQL schema từ utils/sql/silver_table.sql
- Parse JSON payload với from_json (fast parsing)
- Deduplication để tránh duplicate records
- Incremental loading dựa vào ingestion_timestamp

"""
import logging
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config_2 import Config_2
from spark_client import SparkClient
from utils.helper.load_sql import load_sql_from_file

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, to_timestamp, to_date, when, lit, coalesce, current_timestamp, get_json_object)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType, BooleanType, TimestampType)

config = Config_2()


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

PAYLOAD_MAPPING = {    
    "payload_action": "action",
    "payload_ref": "ref",
    "payload_ref_type": "ref_type",
    "push_size": "size",
    "push_distinct_size": "distinct_size",
    "push_head_sha": "head",
    "pr_number": "number",
    "issue_number": "number",
    "pr_id": "pull_request.id",
    "pr_state": "pull_request.state",
    "pr_title": "pull_request.title",
    "pr_merged": "pull_request.merged",
    "pr_merged_at": "pull_request.merged_at", 
    "issue_title": "issue.title",
    "issue_state": "issue.state"
}

# __________________________ SCHEMA __________________________
def get_unified_payload_schema():
    return StructType([
        StructField("action", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("head", StringType(), True),
        StructField("number", IntegerType(), True),
        
        StructField("pull_request", StructType([
            StructField("id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("state", StringType(), True),
            StructField("merged", BooleanType(), True),
            StructField("merged_at", StringType(), True),
        ]), True),
        
        StructField("issue", StructType([
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("state", StringType(), True),
        ]), True)
    ])

# __________________________ SILVER TABLE __________________________
def create_silver_table_if_not_exists(spark: SparkSession, catalog, namespace, silver_table_name):
    silver_table = f"{catalog}.{namespace}.{silver_table_name}"
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "silver_table.sql")

    try:
        # 1. Tạo namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        logger.info(f"-----> [SILVER] Đảm bảo namespace {catalog}.{namespace} tồn tại.")
        
        # 2. Check if table exists
        if spark.catalog.tableExists(silver_table):
            logger.info(f"-----> [SILVER] Table {silver_table} đã tồn tại.")
            return
        
        # 3.Tạo table theo schema
        logger.info(f"-----> [SILVER] Reading DDL from: {sql_file_path}.")        
        create_sql = load_sql_from_file(
            str(sql_file_path),
            silver_table=silver_table
        )

        spark.sql(create_sql)
        logger.info(f"-----> [SILVER] Đã tạo table {silver_table}.")
        
    except Exception as e:
        logger.error(f"-----> [SILVER] Lỗi khi tạo table: {e}.")
        raise

def get_last_processed_timestamp(spark: SparkSession, catalog, namespace, silver_table_name) -> str:
    silver_table = f"{catalog}.{namespace}.{silver_table_name}"
    default_timestamp = "1970-01-01 00:00:00"
    
    try:
        if not spark.catalog.tableExists(silver_table):
            logger.info(f"-----> [SILVER] Table {silver_table} chưa tồn tại, sẽ load toàn bộ từ Bronze.")
            return default_timestamp
        
        row = spark.sql(f"SELECT MAX(processed_at) as max_ts FROM {silver_table}").first()
        if row and row['max_ts']:
            max_ts = row['max_ts']
            logger.info(f"-----> [SILVER] Last processed timestamp: {max_ts}.")
            return str(max_ts)

        else:
            logger.info(f"-----> [SILVER] Table {silver_table} trống, sẽ load toàn bộ từ Bronze.")
            return default_timestamp
            
    except Exception as e:
        logger.warning(f"-----> [SILVER] Không thể lấy last timestamp: {e}.")
        return default_timestamp

# __________________________ MAIN PROCESSING __________________________
def run_process_bronze_to_silver(spark: SparkSession):
    """
    Flow: Bronze Iceberg → Incremental Load → Deduplication → Parse JSON → Silver Iceberg
    """
    catalog = config.ICEBERG_CATALOG
    
    bronze_namespace = config.BRONZE_NAMESPACE
    bronze_table_name = config.BRONZE_TABLE
    bronze_table = f"{catalog}.{bronze_namespace}.{bronze_table_name}"

    silver_namespace = config.SILVER_NAMESPACE
    silver_table_name = config.SILVER_TABLE
    silver_table = f"{catalog}.{silver_namespace}.{silver_table_name}"
    
    logger.info(f"-----> [SILVER] Bắt đầu process: {bronze_table} -> {silver_table}")
    
    # 1. Tạo Silver table
    create_silver_table_if_not_exists(spark, catalog, silver_namespace, silver_table_name)
    
    # 2. Lấy last processed timestamp
    last_ts = get_last_processed_timestamp(spark, catalog, silver_namespace, silver_table_name)
    
    # 3. Incremental load từ Bronze
    logger.info(f"-----> [SILVER] Đọc dữ liệu Bronze từ ingestion_timestamp > {last_ts}.")
    
    bronze_df = spark.table(bronze_table) \
        .filter(col("ingestion_timestamp") > to_timestamp(lit(last_ts)))

    bronze_df = bronze_df.dropDuplicates(["id"])
    dedup_count = bronze_df.count()

    if dedup_count == 0:
        logger.info("-----> [SILVER] Không có dữ liệu mới.")
    else:
        logger.info(f"-----> [SILVER] Số records mới cần xử lý: {dedup_count}.")
    
    # 4. Parse created_at timestamp
    logger.info(f"-----> [SILVER] Parse created_at timestamp.")
    df = bronze_df.withColumn(
        "created_at_ts",
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    
    # 5. Parse JSON payloads
    logger.info(f"-----> [SILVER] Parsing JSON payloads with Unified Schema.")
    df = df.withColumn(
        "payload_parsed",
        from_json(col("payload"), get_unified_payload_schema())
    )

    select_expressions = [
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

    for target_col, source_col in PAYLOAD_MAPPING.items():
        if target_col == "pr_merged_at":
            expr = when(col(f"payload_parsed.{source_col}").isNotNull(), 
                        to_timestamp(col(f"payload_parsed.{source_col}"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        else:
            expr = col(f"payload_parsed.{source_col}")

        select_expressions.append(expr.alias(target_col))

    # 6. Final: Select and Write
    silver_df = df.select(*select_expressions)

    logger.info(f"-----> [SILVER] Writing data to {silver_table}")

    silver_df.createOrReplaceTempView("batch_source_data")
    
    merge_sql = f"""
    MERGE INTO {silver_table} AS target
    USING batch_source_data AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT *
    """
    
    spark.sql(merge_sql)

    logger.info(f"-----> [SILVER] Pipeline hoàn tất!")
    
def main():
    spark = None
    query = None
    try:
        logger.info(f"[SILVER] Khởi tạo Spark Session...")
        spark_client = SparkClient(app_name="Bronze-to-Silver", job_type="batch")
        spark = spark_client.get_session()

        logger.info(f"[SILVER] Bắt đầu Batch Pipeline...")
        query = run_process_bronze_to_silver(spark)

    except KeyboardInterrupt:
        logger.info("-----> [SILVER] Pipeline bị ngắt bởi người dùng.")

    except Exception as e:
        logger.error("-----> [SILVER] Lỗi trong Pipeline", exc_info=True)
        raise

    finally:
        logger.info("-----> [SILVER] Bắt đầu quy trình Shutdown...")

        if query is not None:
            try:
                if query.isActive:
                    logger.info("-----> [SILVER] Đang dừng Streaming Query...")
                    query.stop()
                    logger.info("-----> [SILVER] Streaming Query đã dừng.")
            except Exception as e:
                logger.warning(
                    "-----> [SILVER] Không thể stop Streaming Query",
                    exc_info=True
                )

        if spark is not None:
            try:
                logger.info("-----> [SILVER] Đang đóng Spark Session...")
                spark.stop()
                logger.info("-----> [SILVER] Spark Session đã đóng.")
            except Exception as e:
                logger.warning(
                    "-----> [SILVER] Lỗi khi đóng Spark Session",
                    exc_info=True
                )

        logger.info("-----> [SILVER] Pipeline đã tắt hoàn toàn (Graceful Shutdown).")

if __name__ == "__main__":
    main()
