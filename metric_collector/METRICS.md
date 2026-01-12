# Prometheus Metrics Integration Guide

## Overview
Prometheus metrics đã được tích hợp vào Metric Collector Service để theo dõi hiệu suất và trạng thái hệ thống.

## Endpoints

### `/metrics`
Endpoint Prometheus metrics (format: Prometheus text exposition)

**Example:**
```bash
curl http://localhost:8000/metrics
```

### `/health`
Health check endpoint với thông tin queue

**Example Response:**
```json
{
  "status": "healthy",
  "queue_size": 150,
  "max_queue_size": 100000,
  "queue_pressure_percent": "0.15%"
}
```

## Available Metrics

### Queue Metrics
- `metric_queue_size` - Current queue size
- `metric_queue_capacity` - Maximum queue capacity
- `metric_queue_utilization_percent` - Queue utilization (0-100%)

### API Metrics
- `api_requests_total{endpoint, status}` - Total API requests by endpoint and status
- `api_request_duration_seconds{endpoint}` - Request duration histogram
- `metrics_received_total` - Total metrics received
- `metrics_rejected_total{reason}` - Total metrics rejected (reasons: invalid_json, queue_full, internal_error)

### Kafka Metrics
- `kafka_batches_sent_total` - Total batches successfully sent
- `kafka_batches_failed_total` - Total batches failed
- `kafka_batch_size` - Batch size histogram
- `kafka_send_duration_seconds` - Kafka send duration histogram
- `kafka_retry_count_total` - Total retry attempts
- `kafka_fallback_batches_total` - Total batches saved to fallback

### Worker Metrics
- `kafka_worker_active` - Worker status (1=active, 0=inactive)
- `kafka_worker_uptime_seconds` - Worker uptime in seconds

### Serialization Metrics
- `serialization_errors_total` - Total serialization errors
- `serialization_duration_seconds` - Serialization duration histogram

## Prometheus Configuration

Add this to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'metric_collector'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
```

## Grafana Dashboard Queries

### Queue Utilization
```promql
metric_queue_utilization_percent
```

### API Request Rate
```promql
rate(api_requests_total[5m])
```

### Kafka Success Rate
```promql
rate(kafka_batches_sent_total[5m]) / (rate(kafka_batches_sent_total[5m]) + rate(kafka_batches_failed_total[5m]))
```

### P95 API Latency
```promql
histogram_quantile(0.95, rate(api_request_duration_seconds_bucket[5m]))
```

### Batch Size Average
```promql
rate(kafka_batch_size_sum[5m]) / rate(kafka_batch_size_count[5m])
```

## Testing

Test metrics endpoint:
```bash
# Check if metrics are available
curl http://localhost:8000/metrics | grep metric_queue_size

# Send test metric
curl -X POST http://localhost:8000/collect \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'

# Check updated metrics
curl http://localhost:8000/metrics | grep metrics_received_total
```
