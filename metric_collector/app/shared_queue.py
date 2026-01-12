import asyncio
import logging
import signal
import platform
import os
import json
import time
from pathlib import Path
from .utils.config_1 import Config_1

try:
    from . import metrics
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False
    logging.warning("Prometheus metrics not available")


class SharedQueue:
    def __init__(self):
        self.config = Config_1()
        self.metric_queue = asyncio.Queue(maxsize=self.config.QUEUE_MAX_SIZE)
        self.shutdown_event = asyncio.Event()
        self.overflow_dir = Path("data/queue_overflow")
        self.overflow_dir.mkdir(parents=True, exist_ok=True)
        
        if METRICS_ENABLED:
            metrics.queue_capacity.set(self.config.QUEUE_MAX_SIZE)

    async def enqueue_metric(self, item: dict) -> bool:
        try:
            self.metric_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            await self._spill_to_disk(item)
            return False

    def check_queue_pressure(self):
        size = self.metric_queue.qsize()
        
        if METRICS_ENABLED:
            metrics.queue_size.set(size)
            metrics.queue_utilization.set((size / self.config.QUEUE_MAX_SIZE) * 100)
        
        if size >= self.config.QUEUE_WARNING_THRESHOLD:
            logging.warning(
                f"-----> [QUEUE ALERT] High pressure: {size}/{self.config.QUEUE_MAX_SIZE} "
                f"({size / self.config.QUEUE_MAX_SIZE:.2%})"
            )

    # ========================== SIGNAL HANDLING ==========================
    def setup_signal_handlers(self):
        """Cài đặt hứng tín hiệu tắt (SIGINT/SIGTERM)."""
        if platform.system() == "Windows":
            logging.warning("-----> [SHUTDOWN] Bỏ qua signal handlers (SIGINT/SIGTERM) trên Windows.")
            return

        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)
            loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)
            logging.info("-----> [SHUTDOWN] Signal handlers (SIGINT/SIGTERM) are set up.")
        except Exception as e:
            logging.error(f"-----> [SHUTDOWN] Không thể cài đặt signal handlers: {e}")

    def _handle_shutdown(self):
        logging.warning("-----> Shutdown signal received.")
        self.shutdown_event.set()

    # ========================== INTERNAL HELPERS ==========================
    async def _spill_to_disk(self, item: dict):
        """Ghi dữ liệu ra đĩa khi hàng đợi đầy (Spillover)."""
        filename = self.overflow_dir / f"overflow_{time.strftime('%Y%m%d')}.jsonl"
        
        try:
            record = json.dumps(item, ensure_ascii=False)
            await asyncio.to_thread(self._write_append_sync, filename, record)
            logging.warning(f"-----> [QUEUE ALARM] Queue Full! Spilled EventID {item.get('event_id')} to {filename}")
        except Exception as e:
            logging.error(f"-----> [CRITICAL] Spill-to-disk failed: {e}")

    def _write_append_sync(self, filename, content):
        """Hàm ghi file đồng bộ (chạy trong thread riêng)."""
        with open(filename, mode='a', encoding='utf-8') as f:
            f.write(content + "\n")

shared_queue = SharedQueue()