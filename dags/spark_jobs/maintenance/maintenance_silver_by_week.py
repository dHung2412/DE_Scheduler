"""
Silver Table Maintenance Job - Weeklu
- Xóa snapshot cũ hơn 7 ngày
- Dọn rác (Orphan files)
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

def expire_snapshots(spark, silver_table, catalog, days=7, retain_last=5):
    try:
        logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Bắt đầu expire snapshots cũ hơn {days} ngày...")
        cutoff_timestamp = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        expire_sql = f"""
            CALL {catalog}.system.expire_snapshots(
                table => '{silver_table}',
                older_than => TIMESTAMP '{cutoff_timestamp}',
                retain_last => {retain_last}
            )
        """
        
        result = spark.sql(expire_sql)
        result.show(truncate=False) 
        logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Expire snapshots hoàn tất!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-SILVER-WEEKLY] Lỗi khi expire snapshots: {e}")
        raise


def remove_orphan_files(spark, silver_table, catalog, days=3):
    try:
        logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Bắt đầu remove orphan files cũ hơn {days} ngày...")
        cutoff_timestamp = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        orphan_sql = f"""
            CALL {catalog}.system.remove_orphan_files(
                table => '{silver_table}',
                older_than => TIMESTAMP '{cutoff_timestamp}'
            )
        """
        
        result = spark.sql(orphan_sql)
        result.show(truncate=False)
        logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Remove orphan files hoàn tất!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-SILVER-WEEKLY] Lỗi khi remove orphan files: {e}")
        raise

def run_maintenance_silver_by_week():
    logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Bắt đầu maintenance silver by week")
    spark = None

    try:
        logger.info("=" * 80)

        logger.info("-----> [MAINTENANCE-SILVER-WEEKLY] Khởi tạo Spark Session...")
        spark_client = SparkClient(app_name="Silver-Maintenance-Weekly", job_type="batch")
        spark = spark_client.get_session()
        
        catalog = config.ICEBERG_CATALOG
        namespace = config.SILVER_NAMESPACE
        table_name = config.SILVER_TABLE
        silver_table = f"{catalog}.{namespace}.{table_name}"
        
        logger.info(f"-----> [MAINTENANCE-SILVER-WEEKLY] Bắt đầu maintenance cho bảng {silver_table}")
        expire_snapshots(spark, silver_table, catalog, days=7, retain_last=5)
        remove_orphan_files(spark, silver_table, catalog, days=3)
        
        logger.info("-----> [MAINTENANCE-SILVER-WEEKLY] Tất cả maintenance tasks hoàn tất thành công!")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-SILVER-WEEKLY] Lỗi trong quá trình maintenance: {e}", exc_info=True)
        raise
    finally:
        logger.info("=" * 80)
        if spark is not None:
            spark.stop()
            logger.info("-----> [MAINTENANCE-SILVER-WEEKLY] Spark session đã đóng")

if __name__ == "__main__":
    run_maintenance_silver_by_week()
