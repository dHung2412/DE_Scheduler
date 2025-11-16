
import asyncio
import logging
import signal
import platform
# Hàng đợi này là bộ đệm (buffer) trung tâm
# Collector dùng metric_queue.put()
# Producer dùng metric_queue.get()
"""
Nó tạo một hàng đợi bất đồng bộ (async queue) để:
Collector push metrics vào
Producer worker lấy metrics ra để serialize Avro và gửi Kafka
Vai trò: giảm áp lực IO, chống nghẽn, và decouple Collector ↔ Producer.
"""

QUEUE_MAX_SIZE = 100000
QUEUE_WARNING_THRESHOLD = int(QUEUE_MAX_SIZE * 0.8)

metric_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

async def safe_put(item, timeout = 0.5):
    """"Đưa item vào hàng đợi với timeout để tránh deadlock."""
    try:
        await asyncio.wait_for(metric_queue.put(item), timeout=timeout)
        check_queue_pressure()

    except asyncio.TimeoutError:
        logging.error("-----> metric_queue full, dropping metric")

async def safe_get(timeout = 1.0):
    """"Lấy item từ hàng đợi với timeout để tránh deadlock."""
    try:
        return await asyncio.wait_for(metric_queue.get(), timeout=timeout)
    
    except asyncio.TimeoutError:
        logging.debug("-----> metric_queue empty on get()")
        return None
    
def check_queue_pressure():
    size = metric_queue.qsize()
    if size >= QUEUE_WARNING_THRESHOLD:
        logging.warning(
            f"metric_queue size high: {size}/{QUEUE_MAX_SIZE}",
            f"({size / QUEUE_MAX_SIZE:.2%})"
            )

async def monitor_queue(interval=2):
    """Theo dõi kích thước hàng đợi định kỳ."""
    while True:
        size = metric_queue.qsize()
        logging.info(f"metric_queue size: {size}/{QUEUE_MAX_SIZE} ({size / QUEUE_MAX_SIZE:.2%})")
        await asyncio.sleep(interval)
        
# GRACEFUL SHUTDOWN
_shutdown_event = asyncio.Event()

def _handle_shutdown():
    logging.warning("-----> Shutdown signal received.")
    _shutdown_event.set()

async def drain_queue():
    """Xử lý hết các mục trong hàng đợi trước khi tắt."""
    logging.warning("-----> Draining metric_queue before shutdown...")
    
    drained = 0
    while not metric_queue.empty():
        item = await metric_queue.get()
        if item is not None:
            # Xử lý item ở đây (ví dụ: gửi đến Kafka)
            drained += 1
    logging.warning(f"-----> Drained {drained} items from metric_queue.")

async def wait_for_shutdown():
    """Chặn cho đến khi nhận được tín hiệu tắt rồi xả hàng đợi."""
    await _shutdown_event.wait()
    await drain_queue()

def setup_signal_handlers():
    """
    Cài đặt xử lý tín hiệu cho Graceful Shutdown.
    Chỉ thực hiện trên các hệ thống hỗ trợ (non-Windows).
    """
    
    # Kiểm tra xem có phải Windows không
    if platform.system() == "Windows":
        logging.warning("-----> [SHUTDOWN] Bỏ qua signal handlers (SIGINT/SIGTERM) trên Windows.")
        # Chúng ta dựa vào cơ chế 'lifespan' của Uvicorn để bắt lệnh tắt
        return
        
    # Nếu là Linux/macOS, sử dụng cơ chế asyncio chuẩn
    try:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, _handle_shutdown)
        loop.add_signal_handler(signal.SIGTERM, _handle_shutdown)
        logging.info("-----> [SHUTDOWN] Signal handlers (SIGINT/SIGTERM) are set up.")
    except Exception as e:
        logging.error(f"-----> [SHUTDOWN] Không thể cài đặt signal handlers: {e}")

async def start_queue_management():
    """Khởi động quản lý hàng đợi và xử lý tắt."""
    setup_signal_handlers()
    asyncio.create_task(monitor_queue())
    await wait_for_shutdown()