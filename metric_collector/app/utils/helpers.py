import io
import json
import logging
from typing import List
import fastavro
from pathlib import Path

from .config_1 import config_1

def _load_and_parse_schema():
    schema_path = config_1.AVRO_SCHEMA_PATH
    logging.info(f"-----> [HELPERS] Đang load Avro schema từ {schema_path}")

    if not schema_path.exists():
        raise FileNotFoundError(f"-----> [HELPERS] Không tìm thấy schema file tại: {schema_path}")
    
    with open(schema_path, "r", encoding='utf-8') as f:
        schema_dict = json.load(f)
   
    return fastavro.parse_schema(schema_dict)

PARSED_SCHEMA = _load_and_parse_schema()

def serialize_batch_avro(batch: list[dict]) -> bytes:
    for metric in batch:
        payload = metric.get("payload")
        if payload is not None and not isinstance(payload, str):
            try:
                metric["payload"] = json.dumps(payload)

            except Exception as e:
                logging.exception(f"[HELPERS] Error when convert payload to JSON: {e}")
                metric["payload"] = "{}"

    try:
        with io.BytesIO() as fo:
            fastavro.writer(fo, PARSED_SCHEMA, batch)
            return fo.getvalue()
            
    except Exception as e:
        logging.exception(f"[HELPERS] Error when serialize Avro: {e}")
        raise

def write_file_binary(filename: str, data : bytes):
    with open(filename, "wb") as f:
        f.write(data)