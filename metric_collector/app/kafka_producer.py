import asyncio
import time
import uuid
import logging
from pathlib import Path
from typing import List, Optional
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .shared_queue import shared_queue
from .utils.config_1 import Config_1
from .utils.s3_handler import S3Handler
from .utils.helpers import serialize_batch_avro, write_file_binary

logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

try:
    from . import metrics
    METRICS_ENABLED=True
except ImportError:
    METRICS_ENABLED=False
    logging.warning("Prometheus metrics not available")


class KafkaProducerService:
    def __init__(self):
        self.config = Config_1()
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            compression_type="snappy",
            linger_ms=0
        )
        self.s3_handler = S3Handler()
        self.worker_start_time = None
    
    async def run_worker(self, shutdown_event: Optional[asyncio.Event] = None):
        shutdown_event = shutdown_event or asyncio.Event()

        await self.producer.start()
        logging.info("-----> Kafka Producer started.")

        self.worker_start_time = time.time()

        if METRICS_ENABLED:
            metrics.worker_active.set(1)

        pressure_task = asyncio.create_task(self._queue_pressure_logger(shutdown_event))

        try:
            while not shutdown_event.is_set():
                if METRICS_ENABLED:
                    uptime = time.time() - self.worker_start_time
                    metrics.worker_uptime_seconds.set(uptime)
                
                batch_items = await self._collect_batch()

                if not batch_items:
                    continue

                await self._handle_batch(batch_items)
        
        except asyncio.CancelledError:
            logging.info("-----> Kafka Producer worker cancelled.")

        except Exception as e:
            logging.exception(f"-----> Lỗi không mong muốn trong Kafka Producer worker.")
        
        finally:
            if METRICS_ENABLED:
                metrics.worker_active.set(0)
            
            shutdown_event.set()
            pressure_task.cancel()

            try:
                await pressure_task
            except asyncio.CancelledError:
                pass

            await self._drain_and_persist_remaining()
            await self.producer.stop()
            logging.info("-----> Kafka Producer stopped.")

    # ============ CORE LOGIC =============

    async def _collect_batch(self) -> List[dict]:
        batch_items = []

        try:
            item = await asyncio.wait_for(shared_queue.metric_queue.get(), timeout=1.0)
            batch_items.append(item)
        except (asyncio.TimeoutError, Exception):
            return []
        
        batch_start_time = asyncio.get_running_loop().time()

        while len(batch_items) < self.config.BATCH_MAX_SIZE:
            elapsed = asyncio.get_running_loop().time() - batch_start_time
            remaining = self.config.BATCH_MAX_TIME_S - elapsed

            if remaining < 0:
                break

            try:
                item = shared_queue.metric_queue.get_nowait()
                batch_items.append(item)
            except asyncio.QueueEmpty:
                try:
                    item = await asyncio.wait_for(shared_queue.metric_queue.get(), timeout=remaining)
                    batch_items.append(item)
                except (asyncio.TimeoutError, Exception):
                    break
        
        return batch_items

    async def _handle_batch(self, batch_items: List[dict]):
        batch_id = str(uuid.uuid4())
        metric_list = [item["metric"] for item in batch_items]
        batch_ids = [item.get("event_id", "unknown") or "unknown" for item in batch_items]

        logging.info(f"-----> Batch {batch_id}: Thu thập {len(metric_list)} metrics.")

        if METRICS_ENABLED:
            metrics.kafka_batch_size.observe(len(metric_list))
        
        serialized_batch = None

        try:
            loop = asyncio.get_running_loop()
            serialize_start = time.time()
            serialized_batch = await loop.run_in_executor(None, serialize_batch_avro, metric_list)

            if METRICS_ENABLED:
                metrics.serialization_duration.observe(time.time() - serialize_start)

            # Send with Retry
            metadata = await self._send_with_retries(self.config.KAFKA_TOPIC, serialized_batch, batch_id)
            
            if metadata is None:
                if METRICS_ENABLED:
                    metrics.kafka_fallback_batches.inc()
    
                await self._handle_fallback(serialized_batch, batch_id)

        except Exception as e:
            logging.exception(f"-----> Lỗi xử lý batch {batch_id}: {e}.")

            if METRICS_ENABLED:
                metrics.serialization_errors_total.inc()
                metrics.kafka_fallback_batches.inc()
            
            try:
                save_data = serialized_batch or json.dumps(metric_list).encode()
                await self._handle_fallback(save_data, f"FAULT_{batch_id}")
            except Exception as e:
                logging.error(f"-----> Không thể lưu Fallback cho batch id {batch_id}: {e}.")

        finally:
            for _ in batch_items:
                shared_queue.metric_queue.task_done()
    
    async def _send_with_retries(self, topic: str, serialized_batch: bytes, batch_id: str ):
        attempt = 0
        backoff = self.config.KAFKA_RETRY_BACKOFF_BASE_S
        send_start_time = time.time()

        while attempt < self.config.KAFKA_RETRY_MAX:
            attempt += 1
            try: 
                metadata = await self.producer.send_and_wait(topic, value=serialized_batch)

                if METRICS_ENABLED:
                    metrics.kafka_send_duration.observe(time.time() - send_start_time)
                    metrics.kafka_batches_sent_total.inc()

                logging.info(f"-----> Batch {batch_id} đã gửi thành công.")

                return {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset}
            
            except KafkaError as e:
                logging.error(f"-----> [SEND RETRY] {attempt}/{self.config.KAFKA_RETRY_MAX} failed for batch {batch_id}: {e}.")
            
                if METRICS_ENABLED:
                    metrics.kafka_retry_count.inc()
            
                if attempt >= self.config.KAFKA_RETRY_MAX:
                    if METRICS_ENABLED:
                        metrics.kafka_batches_failed_total.inc()
                    return None
                
                await asyncio.sleep(backoff)
                backoff *= 2

            except Exception as e:
                logging.exception(f"-----> Lỗi không mong muốn khi gửi lại batch: {batch_id} : {e}.")
                return None
    
    # ============ FALL BACK & RECOVERY =============
    async def _handle_fallback(self, batch_bytes: bytes, batch_id: str):
        success = await asyncio.to_thread(
            self.s3_handler.upload_data,
            batch_bytes, 
            f"batch_{batch_id}.avro"
        )

        if success:
            logging.info(f"-----> Failover batch {batch_id} to MinIO Succcess.")
        else:
            logging.warning(f"-----> Failover over {batch_id} to MinIO Failed.")
            logging.info(f"-----> Spill {batch_id} to Local disk")
            await self._persist_to_local_disk(batch_bytes, batch_id)

    async def _persist_to_local_disk(self, batch_bytes: bytes, batch_id: str):
        os.makedirs(self.config.KAFKA_FALLBACK_DIR, exist_ok = True)
        
        file_name = os.path.join(
            self.config.KAFKA_FALLBACK_DIR,
            f"failed_batch_{batch_id}_{int(time.time())}.avro"
        )
        
        await asyncio.to_thread(write_file_binary, file_name, batch_bytes)
        logging.warning(f"-----> Saved Fallback to local disk: {file_name}.")

    async def _drain_and_persist_remaining(self):
        remaining = []
        
        while not shared_queue.metric_queue.empty():
            try:
                item = shared_queue.metric_queue.get_nowait()
                remaining.append(item["metric"])
                shared_queue.metric_queue.task_done()

            except asyncio.QueueEmpty:
                break
        
        if remaining:
            batch_id = f"drain_{int(time.time())}"
            logging.info(f"-----> Persisting {len(remaining)} metrics before shutdown.")

            try:
                serialize_batch = serialize_batch_avro(remaining)
                await self._handle_fallback(serialize_batch, batch_id)
            
            except Exception as e:
                logging.exception(f"-----> Failed to persist remaining metrics: {e}")

    # ============ UTILS =============
    async def _queue_pressure_logger(self, shutdown_event: asyncio.Event):
        last_log = 0
        
        while not shutdown_event.is_set():
            now = time.time()
            if now - last_log >= self.config.QUEUE_PRESSURE_LOG_EVERY:
                qsize = shared_queue.metric_queue.qsize()
                logging.info(f"-----> Size: {qsize}")
                last_log = now
            
            await asyncio.sleep(0.5)

kafka_producer = KafkaProducerService()
run_kafka_producer_worker = kafka_producer.run_worker