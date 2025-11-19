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
    ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "demo")
    ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "bronze")
    ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "github_events")
    
    # Lưu ý: S3A dùng cho Spark Checkpoint, S3 dùng cho Iceberg FileIO
    ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://warehouse/")
    CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://warehouse/_checkpoints/kafka_to_bronze")

    # __________________________ PATHS __________________________
    _schema_path_str = os.getenv("AVRO_SCHEMA_PATH")

    if _schema_path_str:
        AVRO_SCHEMA_PATH = Path(_schema_path_str.strip('"').strip("'"))

    else:
        AVRO_SCHEMA_PATH = None

config_2 = Config_2()