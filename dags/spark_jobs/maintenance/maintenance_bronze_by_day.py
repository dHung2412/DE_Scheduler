"""
Bronze Table Maintenance Job - Daily
- Nén các file vào target 20,000 KB
- Chỉ compact nếu có ít nhất 5 file nhỏ
- Chỉ áp dụng cho 7 ngày gần nhất
Strategy: binpack - chỉ merge files, không sort
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

def run_maintenace_bronze_by_day():
    logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Bắt đầu maintenance bronze by day")

    spark = None
    
    try:
        logger.info("=" * 80)

        logger.info("-----> [MAINTENANCE-BRONZE-DAY] Đang khởi tạo Spark Session")
        spark_client = SparkClient(app_name="Bronze-Maintenance-Daily", job_type="batch")
        spark = spark_client.get_session()
        
        logger.info("-----> [MAINTENANCE-BRONZE-DAY] Spark Session đã khởi tạo thành công.")

        catalog = config.ICEBERG_CATALOG
        namespace = config.BRONZE_NAMESPACE
        table_name = config.BRONZE_TABLE
        full_table_name = f"{catalog}.{namespace}.{table_name}"

        logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Bắt đầu compact bảng {full_table_name}")
        
        cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Chỉ compact dữ liệu từ {cutoff_date} trở lại.")

        compaction_sql = f"""
            CALL {catalog}.system.rewrite_data_files(
                table => '{full_table_name}',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes', '20480000',
                    'min-input-files', '5'
                ),
                where => "ingestion_date >= DATE '{cutoff_date}'"
            )
        """ 

        logger.info("-----> [MAINTENANCE-BRONZE-DAY] Executing compaction")
        result = spark.sql(compaction_sql)

        result.show(truncate=False)

        rows = result.collect()
        if rows:
            for row in rows:
                logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Rewritten files: {row.rewritten_data_files_count}")
                logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Added data files: {row.added_data_files_count}")
                logger.info(f"-----> [MAINTENANCE-BRONZE-DAY] Rewritten bytes: {row.rewritten_bytes_count:,}")

        logger.info("-----> [MAINTENANCE-BRONZE-DAY] Compaction hoàn tất thành công!")

    except Exception as e:
        logger.error(f"-----> [MAINTENANCE-BRONZE-DAY] Lỗi khi chạy compaction: {e}", exc_info=True)
        raise
    
    finally:
        logger.info("=" * 80)
        if spark is not None:  # Only stop if spark was initialized
            spark.stop()
            logger.info("-----> [MAINTENANCE-BRONZE-DAY] Spark session đã đóng")

if __name__ == "__main__":
    run_maintenace_bronze_by_day()
