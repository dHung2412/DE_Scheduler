import logging
from config_2 import config
from utils.spark_client import SparkClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def perform_maintenance():

    spark_client = SparkClient(app_name="Maintenance-Job", job_type="batchh")
    
    spark = spark_client.get_session()
    
    full_table_name = f"{config.ICEBERG_CATALOG}.{config.ICEBERG_NAMESPACE}.{config.ICEBERG_TABLE}"
    
    try:
        # 1. COMPACTION (Gộp file nhỏ)
        # Rewrite data files để tối ưu kích thước (mục tiêu 128MB/file)
        logger.info(f"-----> [MAINTENANCE] Bắt đầu Compact bảng {full_table_name}...")
        spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.rewrite_data_files(table => '{full_table_name}')")
        
        # 2. EXPIRE SNAPSHOTS (Xóa dữ liệu cũ)
        logger.info(f"-----> [MAINTENANCE] Bắt đầu Expire Snapshots...")
        spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.expire_snapshots(table => '{full_table_name}', older_than => DATE_SUB(current_date(), 7))")
        
        # 3. REMOVE ORPHAN FILES (Dọn rác)
        # Xóa các file không còn được tham chiếu bởi metadata nào (do lỗi crash)
        logger.info(f"-----> [MAINTENANCE] Bắt đầu Remove Orphan Files...")
        spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.remove_orphan_files(table => '{full_table_name}')")
        
        logger.info("-----> [MAINTENANCE] Hoàn tất bảo trì.")
        
    except Exception as e:
        logger.error(f"-----> [MAINTENANCE] Lỗi: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    perform_maintenance()