"""
Bronze to Silver Layer Processing
Đọc dữ liệu từ Bronze Iceberg table, parse JSON payload, và ghi vào Silver table

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


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

config = Config_2()

PAYLOAD_MAPPING = {    
    "payload_action": "action",
    "payload_ref": "ref",
    "payload_ref_type": "ref_type",
    
    "push_size": "size",
    "push_distinct_size": "distinct_size",
    "push_head_sha": "head",
    
    "pr_number": "number",
    "issue_number": "number", # Issue và PR dùng chung field 'number' trong top level
    
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
    """
    Master Schema chứa tất cả các trường có thể xuất hiện trong payload.
    Các trường không có trong event hiện tại sẽ tự động null.
    """
    return StructType([
        # Top level simple fields
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
def create_silver_table_if_not_exists(spark: SparkSession):
    catalog = config.ICEBERG_CATALOG
    namespace = config.SILVER_NAMESPACE
    table_name = config.SILVER_TABLE
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "silver_table.sql")

    try:
        # 1. Tạo namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        logger.info(f"-----> [SILVER] Đảm bảo namespace {catalog}.{namespace} tồn tại")
        
        # 2. Check if table exists (Handle corrupted metadata)
        table_exists = False
        try:
            if spark.catalog.tableExists(full_table_name):
                table_exists = True
        except Exception as e:
            logger.warning(f"-----> [SILVER] Metadata lỗi hoặc không đồng bộ: {e}")
            logger.warning(f"-----> [SILVER] Tiến hành DROP table {full_table_name} để tạo lại.")
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name} PURGE")
            table_exists = False

        if table_exists:
            logger.info(f"-----> [SILVER] Table {full_table_name} đã tồn tại.")
            return
        
        # 3.Tạo table theo schema
        logger.info(f"-----> [SILVER] Reading DDL from: {sql_file_path}")        
        create_sql = load_sql_from_file(
            str(sql_file_path),
            full_table_name=full_table_name
        )

        spark.sql(create_sql)
        logger.info(f"-----> [SILVER] Đã tạo table {full_table_name}")
        
    except Exception as e:
        logger.error(f"-----> [SILVER] Lỗi khi tạo table: {e}")
        raise


def get_last_processed_timestamp(spark: SparkSession) -> str:
    """
    Lấy ingestion_timestamp cuối cùng đã xử lý
    Dùng cho incremental loading
    """
    catalog = config.ICEBERG_CATALOG
    namespace = config.SILVER_NAMESPACE
    table_name = config.SILVER_TABLE
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    default_timestamp = "1970-01-01 00:00:00"
    
    try:
        if not spark.catalog.tableExists(full_table_name):
            logger.info(f"-----> [SILVER] Table {full_table_name} chưa tồn tại, sẽ load toàn bộ từ Bronze")
            return default_timestamp
        
        # Lấy max processed_at từ Silver (là processed_at của lần chạy trước)
        row = spark.sql(f"SELECT MAX(processed_at) as max_ts FROM {full_table_name}").first()
        
        if row and row['max_ts']:
            max_ts = row['max_ts']
            logger.info(f"-----> [SILVER] Last processed timestamp: {max_ts}")
            return str(max_ts)
        else:
            logger.info(f"-----> [SILVER] Table {full_table_name} trống, sẽ load toàn bộ từ Bronze")
            return default_timestamp
            
    except Exception as e:
        logger.warning(f"-----> [SILVER] Không thể lấy last timestamp: {e}")
        return default_timestamp


# __________________________ MAIN PROCESSING __________________________
def run_process_bronze_to_silver(spark: SparkSession):
    """
    Main processing logic:
    1. Incremental load từ Bronze (filter by ingestion_timestamp)
    2. Deduplication (dropDuplicates on event_id)
    3. Parse JSON payload với from_json
    4. Transform và flatten data
    5. Write vào Silver table
    """
    bronze_table = f"{config.ICEBERG_CATALOG}.{config.BRONZE_NAMESPACE}.{config.BRONZE_TABLE}"
    silver_table = f"{config.ICEBERG_CATALOG}.{config.SILVER_NAMESPACE}.{config.SILVER_TABLE}"
    
    logger.info(f"-----> [SILVER] Bắt đầu process: {bronze_table} -> {silver_table}")
    
    # 1. Tạo Silver table nếu chưa có
    create_silver_table_if_not_exists(spark)
    last_ts = get_last_processed_timestamp(spark)
    
    # 2. Incremental load từ Bronze
    logger.info(f"-----> [SILVER] Đọc dữ liệu Bronze từ ingestion_timestamp > {last_ts}")
    
    bronze_df = spark.table(bronze_table) \
        .filter(col("ingestion_timestamp") > to_timestamp(lit(last_ts)))

    bronze_df = bronze_df.dropDuplicates(["id"])
    dedup_count = bronze_df.count()

    if dedup_count == 0:
        logger.info("-----> [SILVER] Không có dữ liệu mới")
        return
    
    logger.info(f"-----> [SILVER] Số records mới cần xử lý: {dedup_count}")
    
    # 3. Parse created_at timestamp
    df = bronze_df.withColumn(
        "created_at_ts",
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    
    logger.info(f"-----> [SILVER] Parsing JSON payloads with Unified Schema ...")
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
        
        # --- Ingestion Meta ---
        col("ingestion_date"),
        current_timestamp().alias("processed_at")
    ]

    # Tự động map các cột payload từ Dict Config
    for target_col, source_col in PAYLOAD_MAPPING.items():
        # Xử lý đặc biệt cho timestamp string cần convert
        if target_col == "pr_merged_at":
            expr = when(col(f"payload_parsed.{source_col}").isNotNull(), 
                        to_timestamp(col(f"payload_parsed.{source_col}"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        else:
            expr = col(f"payload_parsed.{source_col}")

        select_expressions.append(expr.alias(target_col))

    # 4. Final: Select and Write
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

if __name__ == "__main__":
    try:
        spark_client = SparkClient(app_name="Bronze-to-Silver", job_type="batch")
        spark = spark_client.get_session()
        run_process_bronze_to_silver(spark)
    except Exception as e:
        logger.error(f"-----> [SILVER] Lỗi trong Pipeline: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("-----> [SILVER] Đã dừng Spark session")
