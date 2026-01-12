from fastapi import FastAPI, Request, HTTPException, Response
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import asyncio
import logging
import uuid
import json
import os
import time

from .utils.config_1 import Config_1
from .kafka_producer import run_kafka_producer_worker
from .shared_queue import shared_queue

config = Config_1()

try:
    from . import metrics
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False
    logging.warning("Prometheus metrics not available")
    
logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

worker_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):

    global worker_task
    logging.info("-----> [LIFESPAN] Ứng dụng đang khởi động...")

    shared_queue.setup_signal_handlers()

    worker_task = asyncio.create_task(run_kafka_producer_worker(shared_queue.shutdown_event)) 

    yield

    logging.warning("-----> [LIFESPAN] Ứng dụng đang tắt...")

    if not shared_queue.shutdown_event.is_set():
        logging.warning("-----> [LIFESPAN] Tắt không do signal, set event thủ công")
        shared_queue.shutdown_event.set()
    
    if worker_task:
        logging.info("-----> [LIFESPAN] Chờ worker Kafka Producer hoàn thành...")
        
        try:
            await worker_task
            logging.info("-----> [LIFESPAN] Worker Kafka Producer đã hoàn thành.")
        except Exception as e:
            logging.error(f"-----> [LIFESPAN] Lỗi khi chờ worker Kafka Producer: {e}")

    logging.info("-----> [LIFESPAN] Ứng dụng đã tắt.")
    
# FASTAPI APP INITIALIZATION
app = FastAPI(
    title="Metric Collector Service",
    description="Nhận metric, đưa vào queue để producer gửi đến Kafka.",
    version="1.0.0",
    lifespan=lifespan
)

# API ENDPOINTS
@app.get("/")
async def root():
    return {"message": "Welcome to Metric Collector Service"}
    
@app.post("/collect")
async def collect_metric(request: Request):
    start_time = time.time()
    status_code = 200
    
    try:
        # 1. Nhận và parse JSON
        metric_data = await request.json()

    except json.JSONDecodeError:
        status_code = 400
        if METRICS_ENABLED:
            metrics.api_requests_total.labels(endpoint='/collect', status='400').inc()
            metrics.metrics_rejected_total.labels(reason='invalid_json').inc()
        logging.warning("-----> [API] Dữ liệu không phải JSON hợp lệ.")
        raise HTTPException(status_code=400, detail="Dữ liệu không phải JSON hợp lệ.")
    
    try:
        # 2. Tạo event_id (trace)
        event_id = str(uuid.uuid4())

        # 3. Gói dữ liệu
        item_to_queue = {
            "event_id": event_id,
            "metric": metric_data}
        
        # 4. Đẩy vào queue (Safety: RAM -> Disk Spillover)
        await shared_queue.enqueue_metric(item_to_queue)

        # 5. Kiểm tra áp lực queue
        shared_queue.check_queue_pressure()
        
        # 6. Track metrics
        if METRICS_ENABLED:
            metrics.metrics_received_total.inc()
            metrics.api_requests_total.labels(endpoint='/collect', status='200').inc()

        return {"status": "success", "event_id": event_id}
    
    except Exception as e:
        status_code = 500
        if METRICS_ENABLED:
            metrics.api_requests_total.labels(endpoint='/collect', status='500').inc()
            metrics.metrics_rejected_total.labels(reason='internal_error').inc()
        logging.exception(f"-----> [API] Lỗi không xác định: {e}")
        raise HTTPException(status_code=500, detail="Lỗi máy chủ nội bộ.")
    
    finally:
        # Track request duration
        if METRICS_ENABLED:
            duration = time.time() - start_time
            metrics.api_request_duration.labels(endpoint='/collect').observe(duration)
    
@app.get("/health")
async def health_check():
    qsize = shared_queue.metric_queue.qsize()
    max_size = config.QUEUE_MAX_SIZE
    
    if METRICS_ENABLED:
        shared_queue.check_queue_pressure()
    
    return {"status": "healthy", 
            "queue_size": qsize, 
            "max_queue_size": max_size,
            "queue_pressure_percent": f"{(qsize / max_size) * 100:.2f}%"
    }

@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint"""
    if not METRICS_ENABLED:
        raise HTTPException(status_code=503, detail="Metrics not available")
    
    shared_queue.check_queue_pressure()
    
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)