"""
Prometheus Metrics Module
==========================
Định nghĩa và quản lý các metrics cho Metric Collector Service.
"""
from prometheus_client import Counter, Gauge, Histogram, Info

# ======================== INFO METRICS ========================
app_info = Info('metric_collector_app', 'Application information')
app_info.info({
    'version': '1.0.0',
    'service': 'metric_collector'
})

# ======================== QUEUE METRICS ========================
queue_size = Gauge(
    'metric_queue_size',
    'Current size of the metric queue'
)

queue_capacity = Gauge(
    'metric_queue_capacity',
    'Maximum capacity of the metric queue'
)

queue_utilization = Gauge(
    'metric_queue_utilization_percent',
    'Queue utilization percentage (0-100)'
)

# ======================== API METRICS ========================
api_requests_total = Counter(
    'api_requests_total',
    'Total number of API requests',
    ['endpoint', 'status']
)

api_request_duration = Histogram(
    'api_request_duration_seconds',
    'API request duration in seconds',
    ['endpoint'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

metrics_received_total = Counter(
    'metrics_received_total',
    'Total number of metrics received'
)

metrics_rejected_total = Counter(
    'metrics_rejected_total',
    'Total number of metrics rejected',
    ['reason']
)

# ======================== KAFKA METRICS ========================
kafka_batches_sent_total = Counter(
    'kafka_batches_sent_total',
    'Total number of batches sent to Kafka'
)

kafka_batches_failed_total = Counter(
    'kafka_batches_failed_total',
    'Total number of batches failed to send to Kafka'
)

kafka_batch_size = Histogram(
    'kafka_batch_size',
    'Size of batches sent to Kafka',
    buckets=(1, 10, 50, 100, 200, 500, 1000, 2000, 5000)
)

kafka_send_duration = Histogram(
    'kafka_send_duration_seconds',
    'Time taken to send batch to Kafka',
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

kafka_retry_count = Counter(
    'kafka_retry_count_total',
    'Total number of Kafka send retries'
)

kafka_fallback_batches = Counter(
    'kafka_fallback_batches_total',
    'Total number of batches saved to fallback storage'
)

# ======================== WORKER METRICS ========================
worker_active = Gauge(
    'kafka_worker_active',
    'Whether Kafka worker is active (1) or not (0)'
)

worker_uptime_seconds = Gauge(
    'kafka_worker_uptime_seconds',
    'Kafka worker uptime in seconds'
)

# ======================== SERIALIZATION METRICS ========================
serialization_errors_total = Counter(
    'serialization_errors_total',
    'Total number of serialization errors'
)

serialization_duration = Histogram(
    'serialization_duration_seconds',
    'Time taken to serialize batch',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5)
)
