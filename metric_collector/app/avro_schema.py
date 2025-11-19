import fastavro
import json
from pathlib import Path
from .config_1 import Config_1

config1 = Config_1()

SCHEMA_FILE_PATH = Path(config1.AVRO_SCHEMA_PATH)
 
def load_schema_from_file(schema_file_path: Path) -> dict:
    print(f"----->[SCHEMA] Loading Avro schema from {schema_file_path}")

    if not schema_file_path.exists():
        raise FileNotFoundError(f"----->[SCHEMA] Schema file not found: {schema_file_path}")

    with open(schema_file_path, "r", encoding='utf-8') as schema_file:
        return json.load(schema_file)
    
SCHEMA_DICT = load_schema_from_file(SCHEMA_FILE_PATH)
PARSED_SCHEMA = fastavro.parse_schema(SCHEMA_DICT)