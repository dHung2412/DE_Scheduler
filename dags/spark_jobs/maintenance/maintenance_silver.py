"""
Maintenance job for Silver Iceberg table
Performs compaction, snapshot expiration, and orphan file cleanup
"""
import logging
from config_2 import Config_2
from spark_client import SparkClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

config = Config_2()

def perform_silver_maintenance():
    """Perform maintenance on Silver table"""
    
    spark_client = SparkClient(app_name="Maintenance-Silver-Job", job_type="batch")
    spark = spark_client.get_session()
    
    full_table_name = f"{config.ICEBERG_CATALOG}.{config.SILVER_NAMESPACE}.{config.SILVER_TABLE}"
    
    try:
        # 1. COMPACTION (Gộp file nhỏ)
        logger.info(f"-----> [MAINTENANCE_SILVER] Bắt đầu Compact bảng {full_table_name}...")
        spark.sql(f"""
            CALL {config.ICEBERG_CATALOG}.system.rewrite_data_files(
                table => '{full_table_name}',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
        logger.info(f"-----> [MAINTENANCE_SILVER] Compact hoàn tất")
        
        # 2. EXPIRE SNAPSHOTS (Xóa snapshots cũ hơn 7 ngày)
        logger.info(f"-----> [MAINTENANCE_SILVER] Bắt đầu Expire Snapshots...")
        spark.sql(f"""
            CALL {config.ICEBERG_CATALOG}.system.expire_snapshots(
                table => '{full_table_name}',
                older_than => DATE_SUB(current_date(), 7)
            )
        """)
        logger.info(f"-----> [MAINTENANCE_SILVER] Expire Snapshots hoàn tất")
        
        # 3. REMOVE ORPHAN FILES (Dọn rác)
        logger.info(f"-----> [MAINTENANCE_SILVER] Bắt đầu Remove Orphan Files...")
        spark.sql(f"""
            CALL {config.ICEBERG_CATALOG}.system.remove_orphan_files(
                table => '{full_table_name}'
            )
        """)
        logger.info(f"-----> [MAINTENANCE_SILVER] Remove Orphan Files hoàn tất")
        
        logger.info("-----> [MAINTENANCE_SILVER] Hoàn tất bảo trì Silver table")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE_SILVER] Lỗi: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    perform_silver_maintenance()
