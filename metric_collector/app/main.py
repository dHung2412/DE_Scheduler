from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import asyncio
import logging
import uuid
import json
import os

from .config_1 import Config_1
from .kafka_producer import run_kafka_producer_worker
from .shared_queue import shared_queue

config = Config_1()

logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

worker_task = None

#______________________________LIFESPAN HANDLER______________________________
@asynccontextmanager
async def lifespan(app: FastAPI):

    global worker_task
    logging.info("-----> [LIFESPAN] Ứng dụng đang khởi động...")

    # 1. Ctrl + C sẽ kích hoạt shutdown event
    shared_queue.setup_signal_handlers()

    # 2. Khởi động worker Kafka Producer
    worker_task = asyncio.create_task(run_kafka_producer_worker(shared_queue.shutdown_event))

    yield

    ## Shutdown process
    logging.warning("-----> [LIFESPAN] Ứng dụng đang tắt...")

    # 1. Đảm bảo event shutdown được set
    if not shared_queue.shutdown_event.is_set():
        logging.warning("-----> [LIFESPAN] Tắt không do signal, set event thủ công")
        shared_queue.shutdown_event.set()
    
    # 2. Chờ worker Kafka Producer hoàn thành 
    if worker_task:
        logging.info("-----> [LIFESPAN] Chờ worker Kafka Producer hoàn thành...")
        
        try:
            await worker_task
            logging.info("-----> [LIFESPAN] Worker Kafka Producer đã hoàn thành.")
        except Exception as e:
            logging.error(f"-----> [LIFESPAN] Lỗi khi chờ worker Kafka Producer: {e}")

    logging.info("-----> [LIFESPAN] Ứng dụng đã tắt.")
    
#______________________________FASTAPI APP INITIALIZATION______________________________
app = FastAPI(
    title="Metric Collector Service",
    description="Nhận metric, đưa vào queue để producer gửi đến Kafka.",
    version="1.0.0",
    lifespan=lifespan
)

#______________________________API ENDPOINTS______________________________
@app.post("/collect")
async def collect_metric(request: Request):
    """Chỉ nhận metric và validate JSON schema, sau đó đưa vào queue."""
    try:
        # 1. Nhận và parse JSON
        metric_data = await request.json()

    except json.JSONDecodeError:
        logging.warning("-----> [API] Dữ liệu không phải JSON hợp lệ.")
        raise HTTPException(status_code=400, detail="Dữ liệu không phải JSON hợp lệ.")
    
    try:
        # 2. Tạo event_id (trace)
        event_id = str(uuid.uuid4())

        # 3. Gói dữ liệu
        item_to_queue = {
            "event_id": event_id,
            "metric": metric_data}
        
        # 4. Đẩy vào queue (non-blocking)
        shared_queue.metric_queue.put_nowait(item_to_queue)

        # 5. Kiểm tra áp lực queue
        shared_queue.check_queue_pressure()

        return {"status": "success", "event_id": event_id}
    
    except asyncio.QueueFull:
        logging.error("-----> [API] Queue đầy, không thể nhận metric mới.")
        raise HTTPException(status_code=503, detail="Dịch vụ quá tải, vui lòng thử lại sau.")
    
    except Exception as e:
        logging.exception(f"-----> [API] Lỗi không xác định: {e}")
        raise HTTPException(status_code=500, detail="Lỗi máy chủ nội bộ.")
    
@app.get("/health")
async def health_check():
    qsize = shared_queue.metric_queue.qsize()
    max_size = int(config.QUEUE_MAX_SIZE, 100000)
    return {"status": "healthy", 
            "queue_size": qsize, 
            "max_queue_size": max_size,
            "queue_pressure_percent": f"{(qsize / max_size) * 100:.2f}%"
    }