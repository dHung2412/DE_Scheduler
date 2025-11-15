import fastavro

METRIC_SCHEMA_DICT = {
    "type": "record",
    "name": "MetricEvent",
    "namespace": "com.platform.metrics",
    "doc": "Unified schema for all metric events emitted by services.",
    "fields": [
        {
            "name": "event_id",
            "type": "string",
            "doc": "UUID for idempotency + traceability"
        },
        {
            "name": "service_name",
            "type": "string",
            "doc": "Name of the service emitting the metric"
        },
        {
            "name": "metric_type",
            "type": "string",
            "doc": "Type/category of metric (cpu_usage, db_size, job_duration...)"
        },
        {
            "name": "value",
            "type": "double",
            "doc": "Numeric value of metric"
        },
        {
            "name": "unit",
            "type": "string",
            "default": "none",
            "doc": "Unit of metric"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            },
            "doc": "Epoch timestamp in milliseconds"
        },
        {
            "name": "env",
            "type": ["null", "string"],
            "default": None,
            "doc": "Environment (dev/stg/prod)"
        },
        {
            "name": "tags",
            "type": {
                "type": "map",
                "values": "string"
            },
            "default": {},
            "doc": "Additional key-value metadata"
        },
        {
            "name": "metadata",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "MetricMetadata",
                    "fields": [
                        {
                            "name": "source_ip",
                            "type": ["null", "string"],
                            "default": None
                        },
                        {
                            "name": "collector_version",
                            "type": ["null", "string"],
                            "default": None
                        },
                        {
                            "name": "notes",
                            "type": ["null", "string"],
                            "default": None
                        }
                    ]
                }
            ],
            "default": None,
            "doc": "Optional metadata"
        }
    ]
}

PARSED_SCHEMA = fastavro.parse_schema(METRIC_SCHEMA_DICT)
