import httpx
import asyncio
import json
import logging
import os
import time

API_ENDPOINT = "http://localhost:8000/collect"
INPUT_FILE_PATH = r"D:\Project\Data_Engineering\DE_Scheduler\data\2015-03-01-17.json" 
CONCURRENCY_LIMIT = 50
# --- Logging setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------------------------------------------------------
async def send_metric(client: httpx.AsyncClient, metric: dict, index: int):
    """Gửi một bản ghi metric đến API và xử lý phản hồi."""
    
    # Kiểm tra xem metric có phải là dict không (đã được parse)
    if not isinstance(metric, dict):
        logging.error(f"Metric #{index}: Dữ liệu không phải là dict.")
        return False

    try:
        response = await client.post(
            API_ENDPOINT,
            json=metric,
            timeout=5.0
        )
        
        if response.status_code == 200:
            logging.debug(f"Metric #{index} (ID: {response.json().get('event_id', 'N/A')}) gửi thành công (200 OK).")
            return True
        
        elif response.status_code == 503:
            # Xử lý Backpressure: Service quá tải (Queue đầy)
            logging.warning(f"Metric #{index} bị từ chối (503 Service Unavailable). Queue đang đầy.")
            return False
            
        else:
            logging.error(f"Metric #{index} gửi thất bại. Status: {response.status_code}, Response: {response.text[:100]}")
            return False

    except httpx.RequestError as e:
        logging.error(f"Metric #{index} gửi thất bại do lỗi kết nối: {e}")
        return False

# ------------------------------------------------------------------------------
async def main_injector():
    """Đọc file và khởi động các tác vụ bất đồng bộ."""
    
    if not os.path.exists(INPUT_FILE_PATH):
        logging.error(f"Không tìm thấy tệp đầu vào tại: {INPUT_FILE_PATH}. Vui lòng kiểm tra lại đường dẫn.")
        return
    
    # 1. Đọc dữ liệu từ file JSON (mỗi dòng là một bản ghi)
    logging.info(f"Bắt đầu đọc dữ liệu từ tệp: {INPUT_FILE_PATH}")
    metrics_to_send = []
    
    try:
        with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    metrics_to_send.append(json.loads(line))
    except json.JSONDecodeError as e:
        logging.error(f"Lỗi cú pháp JSON trong tệp: {e}. Đảm bảo mỗi dòng là một JSON object hợp lệ.")
        return
    
    total_metrics = len(metrics_to_send)
    logging.info(f"Đã đọc thành công {total_metrics} bản ghi. Bắt đầu bắn dữ liệu...")

    # 2. Thiết lập giới hạn chạy đồng thời
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = []
    
    # 3. Sử dụng httpx.AsyncClient trong async context
    async with httpx.AsyncClient() as client:
        start_time = time.time()
        
        for index, metric in enumerate(metrics_to_send, 1):
            async def run_with_semaphore(idx, m):
                async with semaphore:
                    await send_metric(client, m, idx)
            
            tasks.append(asyncio.create_task(run_with_semaphore(index, metric)))

        # 4. Chờ tất cả các tác vụ hoàn thành
        await asyncio.gather(*tasks)

        end_time = time.time()
        
    logging.info(f"Quá trình bắn dữ liệu hoàn tất.")
    logging.info(f"Tổng số bản ghi: {total_metrics}")
    logging.info(f"Thời gian thực thi: {end_time - start_time:.2f} giây")
    logging.info(f"Tốc độ trung bình: {total_metrics / (end_time - start_time):.2f} bản ghi/giây")


if __name__ == "__main__":
    asyncio.run(main_injector())