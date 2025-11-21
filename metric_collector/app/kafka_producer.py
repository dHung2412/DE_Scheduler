import asyncio
import io
from typing import List, Optional
import uuid
import fastavro
import logging
import os
import time
from pathlib import Path
import json 

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .shared_queue import metric_queue
from .avro_schema import PARSED_SCHEMA

from .config_1 import Config_1

config1 = Config_1()
SCHEMA_FILE_PATH = Path(config1.AVRO_SCHEMA_PATH)

producer = AIOKafkaProducer(
    bootstrap_servers=config1.KAFKA_BOOTSTRAP_SERVERS,
    compression_type="snappy",
    linger_ms=0,
)

def serialize_batch_avro(batch: List[dict]) -> bytes:
    with io.BytesIO() as fo:
        fastavro.writer(fo, PARSED_SCHEMA, batch)
        return fo.getvalue()


async def persist_fallback(batch_bytes: bytes, batch_id: int):
    """Lưu batch dữ liệu vào thư mục fallback khi không gửi được đến Kafka."""
    os.makedirs(config1.KAFKA_FALLBACK_DIR, exist_ok=True)
    filename = os.path.join(config1.KAFKA_FALLBACK_DIR, f"failed_batch_{batch_id}_{int(time.time())}.avro")
    await asyncio.to_thread(_write_file, filename, batch_bytes)
    logging.warning(f"-----> [KAFKA] Đã lưu batch thất bại vào {filename}")


def _write_file(filename: str, data: bytes):
    with open(filename, "wb") as f:
        f.write(data)

#__________________________________________________________________________________________
async def send_with_retries(topic: str,value: bytes, batch_id: int) -> Optional[dict]:
    attempt = 0
    backoff = config1.KAFKA_RETRY_BACKOFF_BASE_S

    while attempt < config1.KAFKA_RETRY_MAX:
        attempt += 1
        try:
            metadata = await producer.send_and_wait(topic, value=value)
            
            result = {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset
            }
            
            logging.info(f"-----> [KAFKA] Batch {batch_id} gửi thành công đến Kafka: {result}")
            return result
        
        except KafkaError as e:
            logging.error(f"-----> [KAFKA] Lỗi gửi batch {batch_id} đến Kafka (lần {attempt}): {e}")
            if attempt >= config1.KAFKA_RETRY_MAX:
                logging.error(f"-----> [KAFKA] Đã vượt quá số lần thử gửi batch {batch_id}. Lưu vào fallback.")
                return None
            await asyncio.sleep(backoff)
            backoff *= 2

        except Exception as e:
            logging.exception(f"-----> [KAFKA] Ngoại lệ không mong muốn khi gửi batch {batch_id}: {e}")
            if attempt > config1.KAFKA_RETRY_MAX:
                return None
            await asyncio.sleep(backoff)
            backoff *= 2

# __________________________________________________________________________________________
async def run_kafka_producer_worker(shutdown_event: Optional[asyncio.Event] = None):
    """"
    Task nền liên tục:
    - 1. Batching dữ liệu từ metric_queue
    - 2. Serialize batch theo Avro
    - 3. Gửi batch đến Kafka với cơ chế retry
    - 4. Lưu batch thất bại vào thư mục fallback nếu không gửi được
    Chấp nhận shutdown_envent để dừng.
    """

    if shutdown_event is None:
        shutdown_event = asyncio.Event()

    await producer.start()
    logging.info("-----> [KAFKA] Kafka producer started.")

    pressure_task = asyncio.create_task(_queue_pressure_logger(shutdown_event))

    try:
        while not shutdown_event.is_set():
            batch = []
            batch_ids = []
            batch_start = asyncio.get_event_loop().time()   

            # Collect batch
            while True:
                remaining = config1.BATCH_MAX_TIME_S - (asyncio.get_event_loop().time() - batch_start)
                if remaining <= 0 or len(batch) >= config1.BATCH_MAX_SIZE:
                    break

                try:
                    item = await asyncio.wait_for(metric_queue.get(), timeout=remaining)
                    metric = item["metric"]

                    if "payload" in metric and metric["payload"] is not None:
                        if not isinstance(metric["payload"], str):
                            try:
                                metric["payload"] = json.dumps(metric["payload"])
                            except Exception as e:
                                logging.exception(f"-----> [KAFKA] Lỗi khi chuyển đổi payload thành JSON: {e}")
                                # metric["payload"] = str(metric["payload"])
                                metric["payload"] = "{}"

                    batch.append(metric)
                    batch_ids.append(item.get("event_id", "unknown") or "unknown")

                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    logging.exception(f"-----> [KAFKA] Lỗi khi lấy metric từ queue: {e}")
                    break

            if not batch:
                await asyncio.sleep(0.1)
                continue

            batch_id = str(uuid.uuid4())
            logging.info(f"-----> [KAFKA] Thu thập batch {batch_id} với {len(batch)} metrics, IDs: {batch_ids[:3]}")

            # Serialize batch
            try:
                seriallized_batch = serialize_batch_avro(batch)
            except Exception as e:
                logging.exception(f"-----> [KAFKA] Lỗi khi serialize batch {batch_id}: {e}")

                try:
                    await persist_fallback(b"SERIALIZE_ERROR\n", batch_id)
                except Exception:
                    logging.exception(f"-----> [KAFKA] Không thể lưu batch serialize lỗi {batch_id} vào fallback.")

                # Mark items as done  => Queue không bị tắc
                for _ in batch:
                    metric_queue.task_done()
                continue

            # Send with retries
            metadata = await send_with_retries(config1.KAFKA_TOPIC, seriallized_batch, batch_id)

            if metadata is None:
                try:
                    await persist_fallback(seriallized_batch, batch_id)
                except Exception:
                    logging.exception(f"-----> [KAFKA] Không thể lưu batch thất bại {batch_id} vào fallback.")

                # Mark items as done
                for _ in batch:
                    metric_queue.task_done()

            else:
                # Success, mark items as done
                for _ in batch:
                    metric_queue.task_done()

    except asyncio.CancelledError:
        logging.info("-----> [KAFKA] Kafka producer worker cancelled.")
    except Exception as e:
        logging.exception(f"-----> [KAFKA] Lỗi không mong muốn trong Kafka producer worker: {e}")
    finally:
        shutdown_event.set()
        pressure_task.cancel()
        try:
            await pressure_task
        except asyncio.CancelledError:
            pass
        await _drain_and_presist_remaining()
        await producer.stop()
        logging.info("-----> [KAFKA] Kafka producer stopped.")


#______________________________Helpers: pressure reporter & draining______________________________
async def _queue_pressure_logger(shutdown_event: asyncio.Event):
    """Ghi log định kỳ về áp lực của queue."""
    last_log = 0

    while not shutdown_event.is_set():
        now = time.time()
        if now - last_log >= config1.QUEUE_PRESSURE_LOG_EVERY:
            qsize = metric_queue.qsize()
            logging.info(f"-----> [QUEUE] Kích thước hiện tại của metric_queue: {qsize}")
            last_log = now
        await asyncio.sleep(0.5)

async def _drain_and_presist_remaining():
    """Đưa các mục còn lại vào một tệp avro trước khi tắt."""
    remaining = []
    while not metric_queue.empty():
        try:
            item = await metric_queue.get()
            metric = item["metric"]

            if "payload" in metric and metric["payload"] is not None:
                if not isinstance(metric["payload"], str):
                    metric["payload"] = json.dumps(metric["payload"])

            remaining.append(metric)
            metric_queue.task_done()

        except asyncio.QueueEmpty:
            break
        except Exception:
            break

    if remaining:
        batch_id = f"drain_{int(time.time())}"
        logging.info(f"----->[DRAIN] Đang xử lý {len(remaining)} metrics còn lại trước khi tắt.")
        try:
            seriallized_batch = serialize_batch_avro(remaining)
            await persist_fallback(seriallized_batch, batch_id)
            logging.info(f"----->[DRAIN] Đã lưu {len(remaining)} metrics còn lại vào fallback trước khi tắt.")
        except Exception as e:
            logging.exception(f"----->[DRAIN] Lỗi khi xử lý metrics còn lại: {e}")