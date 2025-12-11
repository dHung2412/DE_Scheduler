import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config_1:
    # __________________________ KAFKA __________________________
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_metrics_avro")
    KAFKA_RETRY_MAX = int(os.getenv("KAFKA_RETRY_MAX", 3))
    KAFKA_RETRY_BACKOFF_BASE_S = float(os.getenv("KAFKA_RETRY_BACKOFF_BASE_S", 0.5))
    KAFKA_FALLBACK_DIR = Path(os.getenv("KAFKA_FALLBACK_DIR", "./tmp/metric_fallbacks"))

    # ______________________ METRIC COLLECTOR ______________________
    BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", 1000))
    BATCH_MAX_TIME_S = float(os.getenv("BATCH_MAX_TIME_S", 5.0))

    QUEUE_PRESSURE_LOG_EVERY = int(os.getenv("QUEUE_PRESSURE_LOG_EVERY", 5))
    QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE", 100000))

    _threshold_env = os.getenv("QUEUE_WARNING_THRESHOLD")

    if _threshold_env:
        QUEUE_WARNING_THRESHOLD = int(_threshold_env)
    else:
        QUEUE_WARNING_THRESHOLD = int(QUEUE_MAX_SIZE * 0.8)
        
    # __________________________ PATHS __________________________
    _schema_path_str = os.getenv("AVRO_SCHEMA_PATH")

    if _schema_path_str:
        AVRO_SCHEMA_PATH = Path(_schema_path_str.strip('"').strip("'"))
    else:
        AVRO_SCHEMA_PATH = None

config_1 = Config_1()