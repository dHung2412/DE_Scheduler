import asyncio
import logging
import signal
import platform
from .config_1 import Config_1

class SharedQueue:
    """Lớp quản lý trung tâm cho Metric Queue và Graceful Shutdown.
    
    Attributes:
        config (Config_1): Đối tượng cấu hình hệ thống.
        metric_queue (asyncio.Queue): Hàng đợi FIFO bất đồng bộ lưu trữ metrics chờ xử lý.
        shutdown_event (asyncio.Event): Sự kiện báo hiệu yêu cầu tắt ứng dụng (từ signal hoặc lỗi).
    """
    def __init__(self):
        self.config = Config_1()
        self.metric_queue = asyncio.Queue(maxsize=self.config.QUEUE_MAX_SIZE)
        self.shutdown_event = asyncio.Event()

    def check_queue_pressure(self):
        """Kiểm tra và cảnh báo nếu kích thước hàng đợi vượt quá ngưỡng."""
        size = self.metric_queue.qsize()
        if size >= self.config.QUEUE_WARNING_THRESHOLD:
            logging.warning(
                f"metric_queue size high: {size}/{self.config.QUEUE_MAX_SIZE} "
                f"({size / self.config.QUEUE_MAX_SIZE:.2%})"
            )

    def _handle_shutdown(self):
        """Internal callback khi nhận signal."""
        logging.warning("-----> Shutdown signal received.")
        self.shutdown_event.set()

    def setup_signal_handlers(self):
        """Đăng ký các hàm xử lý tín hiệu hệ thống (Signal Handlers)."""    
        if platform.system() == "Windows":
            logging.warning("-----> [SHUTDOWN] Bỏ qua signal handlers (SIGINT/SIGTERM) trên Windows.")
            return

        try:
            loop = asyncio.get_running_loop() # Dùng running loop chuẩn hơn
            loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)
            loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)
            logging.info("-----> [SHUTDOWN] Signal handlers (SIGINT/SIGTERM) are set up.")
        except Exception as e:
            logging.error(f"-----> [SHUTDOWN] Không thể cài đặt signal handlers: {e}")

shared_queue = SharedQueue()