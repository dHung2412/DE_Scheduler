"""
Bronze Table Maintenance Job - Weekly
- Expire snapshots cũ (> 7 ngày)
- Remove orphan files (> 3 ngày)
- Rewrite position delete files
"""

import logging
import os
import sys
from datetime import datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config_2 import Config_2
from spark_client import SparkClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

config = Config_2()

def expire_snapshots(spark, full_table_name, catalog, days=7, retain_last=5):
    try:
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Bắt đầu expire snapshots cũ hơn {days} ngày...")
        
        cutoff_timestamp = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        expire_sql = f"""
            CALL {catalog}.system.expire_snapshots(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{cutoff_timestamp}',
                retain_last => {retain_last}
            )
        """
        
        result = spark.sql(expire_sql)
        result.show(truncate=False)
        
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Expire snapshots hoàn tất!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Lỗi khi expire snapshots: {e}")
        raise

def remove_orphan_files(spark, full_table_name, catalog, days=3):
    try:
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Bắt đầu remove orphan files cũ hơn {days} ngày...")
        
        cutoff_timestamp = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        orphan_sql = f"""
            CALL {catalog}.system.remove_orphan_files(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{cutoff_timestamp}'
            )
        """
        
        result = spark.sql(orphan_sql)
        result.show(truncate=False)
        
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Remove orphan files hoàn tất!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Lỗi khi remove orphan files: {e}")
        raise

def rewrite_position_deletes(spark, full_table_name, catalog):
    try:
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Bắt đầu rewrite position delete files...")
        
        rewrite_sql = f"""
            CALL {catalog}.system.rewrite_position_delete_files(
                table => '{full_table_name}',
                options => map('rewrite-all', 'false')
            )
        """
        
        result = spark.sql(rewrite_sql)
        result.show(truncate=False)
        
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Rewrite position deletes hoàn tất!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Lỗi khi rewrite position deletes: {e}")
        # Không raise vì có thể bảng không có delete files
        logger.warning(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Skip rewrite position deletes - có thể table không có delete files")

def run_maintenance_bronze_by_week():
    logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Bắt đầu maintenance bronze by week")

    spark = None

    try:
        logger.info("=" * 80)

        # 1. Khởi tạo Spark
        logger.info("-----> [MAINTENANCE-BRONZE-WEEKLY] Đang khởi tạo Spark Session...")
        spark_client = SparkClient(app_name="Bronze-Maintenance-Weekly", job_type="batch")
        spark = spark_client.get_session()
        
        # 2. Setup table name
        catalog = config.ICEBERG_CATALOG
        namespace = config.BRONZE_NAMESPACE
        table_name = config.BRONZE_TABLE
        full_table_name = f"{catalog}.{namespace}.{table_name}"
        
        logger.info(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Bắt đầu maintenance cho bảng {full_table_name}")
        
        # 3. Expire snapshots (giữ 7 ngày, luôn giữ 5 snapshots gần nhất)
        expire_snapshots(spark, full_table_name, catalog, days=7, retain_last=5)
        
        # 4. Remove orphan files (chỉ xóa files > 3 ngày)
        remove_orphan_files(spark, full_table_name, catalog, days=3)
        
        # 5. Rewrite position delete files
        rewrite_position_deletes(spark, full_table_name, catalog)
        
        logger.info("-----> [MAINTENANCE-BRONZE-WEEKLY] Tất cả maintenance tasks hoàn tất thành công!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-BRONZE-WEEKLY] Lỗi trong quá trình maintenance: {e}", exc_info=True)
        raise
    
    finally:
        logger.info("=" * 80)
        if spark is not None:  # Only stop if spark was initialized
            spark.stop()
            logger.info("-----> [MAINTENANCE-BRONZE-WEEKLY] Spark session đã đóng")

if __name__ == "__main__":
    run_maintenance_bronze_by_week()
