import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config_2:
    # __________________________ KAFKA __________________________
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_metrics_avro")
    KAFKA_RETRY_MAX = int(os.getenv("KAFKA_RETRY_MAX", 3)) 
    KAFKA_RETRY_BACKOFF_BASE_S = float(os.getenv("KAFKA_RETRY_BACKOFF_BASE_S", 0.5))
    KAFKA_FALLBACK_DIR = Path(os.getenv("KAFKA_FALLBACK_DIR", "./tmp/metric_fallbacks"))

    # __________________________ MINIO __________________________
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "warehouse")

    # _________________________ ICEBERG _________________________
    # Bronze Layer
    ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "demo")
    BRONZE_NAMESPACE = os.getenv("BRONZE_NAMESPACE", "bronze")
    BRONZE_TABLE = os.getenv("BRONZE_TABLE", "github_events")
    
    # Silver Layer
    SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
    SILVER_TABLE = os.getenv("SILVER_TABLE", "github_events_parsed")
    
    # Gold Layer  
    GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")
    
    # Lưu ý: S3A dùng cho Spark Checkpoint, S3 dùng cho Iceberg FileIO
    ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://warehouse/")
    BRONZE_CHECKPOINT_LOCATION = os.getenv("BRONZE_CHECKPOINT_LOCATION", "s3a://warehouse/_checkpoints/kafka_to_bronze")
    SILVER_CHECKPOINT_LOCATION = os.getenv("SILVER_CHECKPOINT_LOCATION", "s3a://warehouse/_checkpoints/bronze_to_silver")

    # __________________________ PATHS __________________________
    _schema_path_str = os.getenv("AVRO_SCHEMA_PATH")

    if _schema_path_str:
        AVRO_SCHEMA_PATH = Path(_schema_path_str.strip('"').strip("'"))
    else:
        AVRO_SCHEMA_PATH = None

config_2 = Config_2()