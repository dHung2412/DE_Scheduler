"""
Module xử lý và gửi dữ liệu metrics đến Kafka.

Module này cung cấp lớp `KafkaProducerService` nhằm quản lý vòng đời của 
Kafka Producer (dùng thư viện aiokafka) và xử lý việc batching, serialize (Avro), 
gửi dữ liệu, và cơ chế fallback khi gặp lỗi.
"""
import asyncio
import io
import uuid
import logging
import os
import time
import json
from typing import List, Optional
from pathlib import Path

import fastavro
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .shared_queue import shared_queue
from .avro_schema import PARSED_SCHEMA
from .config_1 import Config_1


class KafkaProducerService:
    """Lớp quản lý Kafka Producer và luồng xử lý dữ liệu background.

    Lớp này chịu trách nhiệm:
    1. Quản lý kết nối tới Kafka (start/stop).
    2. Chạy background task (`run_worker`) để tiêu thụ dữ liệu từ queue.
    3. Serialize dữ liệu sang định dạng Avro.
    4. Gửi dữ liệu theo lô (batch) với cơ chế retry.
    5. Lưu dữ liệu vào file (fallback) nếu gửi thất bại.
    
    Attributes:
        config (Config_1): Cấu hình hệ thống.
        producer (AIOKafkaProducer): Đối tượng producer của thư viện aiokafka.
    """
    def __init__(self):
        """Khởi tạo KafkaProducerService với cấu hình và producer."""
        self.config = Config_1()
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            compression_type="snappy",
            linger_ms=0,
        )

    def _serialize_batch_avro(self, batch: List[dict]) -> bytes:
        """Xử lý JSON payload và Encode batch thành Avro.
        
        Phương thức này thực hiện:
        1. Chuyển đổi trường 'payload' trong metric thành chuỗi JSON (nếu cần).
        2. Serialize toàn bộ batch thành bytes sử dụng schema Avro đã parse.

        Args:
            batch (List[dict]): Danh sách các metric records.

        Returns:
            bytes: Dữ liệu đã được serialize.
        """
        # 1. Pre-process payload (JSON dumps)
        for metric in batch:
            payload = metric.get("payload")
            if payload is not None and not isinstance(payload, str):
                try:
                    metric["payload"] = json.dumps(payload)
                except Exception as e:
                    logging.exception(f"-----> [KAFKA] Lỗi khi chuyển đổi payload thành JSON: {e}")
                    metric["payload"] = "{}"

        # 2. Serialize Avro
        with io.BytesIO() as fo:
            fastavro.writer(fo, PARSED_SCHEMA, batch)
            return fo.getvalue()

    async def _persist_fallback(self, batch_bytes: bytes, batch_id: str):
        """Lưu batch dữ liệu vào thư mục fallback khi không gửi được đến Kafka.
        
        Args:
            batch_bytes (bytes): Dữ liệu batch đã serialize (hoặc thông tin lỗi).
            batch_id (str): Định danh của batch.
        """
        os.makedirs(self.config.KAFKA_FALLBACK_DIR, exist_ok=True)
        filename = os.path.join(
            self.config.KAFKA_FALLBACK_DIR, 
            f"failed_batch_{batch_id}_{int(time.time())}.avro"
        )
        await asyncio.to_thread(self._write_file, filename, batch_bytes)
        logging.warning(f"-----> [KAFKA] Đã lưu batch thất bại vào {filename}")

    def _write_file(self, filename: str, data: bytes):
        """Helper function (chạy trong thread) để ghi file."""
        with open(filename, "wb") as f:
            f.write(data)

    async def _send_with_retries(self, topic: str, value: bytes, batch_id: str) -> Optional[dict]:
        """Gửi batch đến Kafka với cơ chế retry (Exponential Backoff).

        Args:
            topic (str): Kafka topic mục tiêu.
            value (bytes): Dữ liệu batch đã serialize.
            batch_id (str): Định danh batch để logging.

        Returns:
            Optional[dict]: Metadata của msg Kafka nếu thành công, None nếu thất bại sau các lần thử.
        """
        attempt = 0
        backoff = self.config.KAFKA_RETRY_BACKOFF_BASE_S

        while attempt < self.config.KAFKA_RETRY_MAX:
            attempt += 1
            try:
                metadata = await self.producer.send_and_wait(topic, value=value)
                
                result = {
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset
                }

                logging.info(f"-----> [KAFKA] Batch {batch_id} gửi thành công đến Kafka: {result}")
                return result
            
            except KafkaError as e:
                logging.error(f"-----> [KAFKA] Lỗi gửi batch {batch_id} đến Kafka (lần {attempt}): {e}")
                if attempt >= self.config.KAFKA_RETRY_MAX:
                    logging.error(f"-----> [KAFKA] Đã vượt quá số lần thử gửi batch {batch_id}. Lưu vào fallback.")
                    return None

                await asyncio.sleep(backoff)
                backoff *= 2

            except Exception as e:
                logging.exception(f"-----> [KAFKA] Ngoại lệ không mong muốn khi gửi batch {batch_id}: {e}")
                if attempt > self.config.KAFKA_RETRY_MAX:
                    return None
                await asyncio.sleep(backoff)
                backoff *= 2

    async def _queue_pressure_logger(self, shutdown_event: asyncio.Event):
        """Ghi log định kỳ về kích thước của queue (Metric/Monitoring)."""
        last_log = 0
        while not shutdown_event.is_set():
            now = time.time()
            if now - last_log >= self.config.QUEUE_PRESSURE_LOG_EVERY:
                qsize = shared_queue.metric_queue.qsize()
                logging.info(f"-----> [QUEUE] Kích thước hiện tại của shared_queue.metric_queue: {qsize}")
                last_log = now
            await asyncio.sleep(0.5)

    async def _drain_and_presist_remaining(self):
        """Xử lý (drain) các metrics còn sót lại trong queue khi shutdown."""
        remaining = []

        while not shared_queue.metric_queue.empty():
            try:
                item = await shared_queue.metric_queue.get()
                metric = item["metric"]
                remaining.append(metric)
                shared_queue.metric_queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception:
                break

        if remaining:
            batch_id = f"drain_{int(time.time())}"
            logging.info(f"----->[DRAIN] Đang xử lý {len(remaining)} metrics còn lại trước khi tắt.")
            try:
                serialized_batch = self._serialize_batch_avro(remaining)
                await self._persist_fallback(serialized_batch, batch_id)
                logging.info(f"----->[DRAIN] Đã lưu {len(remaining)} metrics còn lại vào fallback trước khi tắt.")
            except Exception as e:
                logging.exception(f"----->[DRAIN] Lỗi khi xử lý metrics còn lại: {e}")

    async def run_worker(self, shutdown_event: Optional[asyncio.Event] = None):
        """Vòng lặp chính của Kafka Producer Worker.
        
        Quy trình:
        1. Khởi động Kafka Producer.
        2. Liên tục lấy metrics từ queue theo lô (batch).
        3. Serialize và gửi đến Kafka.
        4. Xử lý shutdown và drain queue khi cần thiết.

        Args:
            shutdown_event (Optional[asyncio.Event]): Event để nhận tín hiệu tắt. 
                                                      Nếu None, sẽ tự tạo mới.
        """
        if shutdown_event is None:
            shutdown_event = asyncio.Event()

        await self.producer.start()
        logging.info("-----> [KAFKA] Kafka producer started.")

        pressure_task = asyncio.create_task(self._queue_pressure_logger(shutdown_event))
        loop = asyncio.get_running_loop()

        try:
            while not shutdown_event.is_set():
                batch = []
                batch_ids = []
                
                # --- Phase 1: Wait for first item (Avoid busy loop) ---
                try:
                    # Wait 1s, then check shutdown_event loop
                    item = await asyncio.wait_for(shared_queue.metric_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break

                # Add first item
                batch.append(item["metric"])
                batch_ids.append(item.get("event_id", "unknown") or "unknown")
                batch_start = loop.time() 

                # --- Phase 2: Collect subsequent items ---
                while True:
                    now = loop.time()
                    remaining = self.config.BATCH_MAX_TIME_S - (now - batch_start)
                    
                    if remaining <= 0 or len(batch) >= self.config.BATCH_MAX_SIZE:
                        break

                    try:
                        # Try get_nowait first to avoid Task overhead
                        try:
                            item = shared_queue.metric_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            item = await asyncio.wait_for(shared_queue.metric_queue.get(), timeout=remaining)
                        
                        batch.append(item["metric"])
                        batch_ids.append(item.get("event_id", "unknown") or "unknown")

                    except asyncio.TimeoutError:
                        break
                    except Exception as e:
                        logging.exception(f"-----> [KAFKA] Lỗi khi lấy metric từ queue: {e}")
                        break

                batch_id = str(uuid.uuid4())
                logging.info(f"-----> [KAFKA] Thu thập batch {batch_id} với {len(batch)} metrics, IDs: {batch_ids[:3]}")

                # --- Phase 3: Serialize batch (Offload CPU work) ---
                try:
                    # Run CPU-bound serialization in a separate thread
                    serialized_batch = await loop.run_in_executor(None, self._serialize_batch_avro, batch)
                except Exception as e:
                    logging.exception(f"-----> [KAFKA] Lỗi khi serialize batch {batch_id}: {e}")

                    try:
                        await self._persist_fallback(b"SERIALIZE_ERROR\n", batch_id)
                    except Exception:
                        logging.exception(f"-----> [KAFKA] Không thể lưu batch serialize lỗi {batch_id} vào fallback.")

                    for _ in batch:
                        shared_queue.metric_queue.task_done()
                    continue

                # --- Phase 4: Send with retries ---
                metadata = await self._send_with_retries(self.config.KAFKA_TOPIC, serialized_batch, batch_id)

                if metadata is None:
                    try:
                        await self._persist_fallback(serialized_batch, batch_id)
                    except Exception:
                        logging.exception(f"-----> [KAFKA] Không thể lưu batch thất bại {batch_id} vào fallback.")

                    for _ in batch:
                        shared_queue.metric_queue.task_done()
                else:
                    for _ in batch:
                        shared_queue.metric_queue.task_done()

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
            
            await self._drain_and_presist_remaining()
            await self.producer.stop()
            logging.info("-----> [KAFKA] Kafka producer stopped.")

kafka_service = KafkaProducerService()

run_kafka_producer_worker = kafka_service.run_worker
