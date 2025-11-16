import fastavro
import json
from pathlib import Path

SCHEMA_FILE_PATH = Path(__file__).parent.joinpath("schema/github_event.avsc")

def load_schema_from_file(schema_file_path: Path) -> dict:
    print(f"Loading Avro schema from {schema_file_path}")

    if not schema_file_path.exists():
        raise FileNotFoundError(f"----->[SCHEMA] Schema file not found: {schema_file_path}")

    with open(schema_file_path, "r", encoding='utf-8') as schema_file:
        return json.load(schema_file)
    
SCHEMA_DICT = load_schema_from_file(SCHEMA_FILE_PATH)
PARSED_SCHEMA = fastavro.parse_schema(SCHEMA_DICT)