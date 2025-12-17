# Data Pipeline for GitHub Events 

Dự án này triển khai một Data Pipeline toàn diện (End-to-End), được thiết kể để xử lý dữ liệu sự kiện thời gian thực từ GitHub, sử dụng kiến trúc Lakehouse hiện đại.

## 🛠 Tech Stack
*   **Ingestion**: FastAPI & Kafka
*   **Orchestration**: [Apache Airflow](https://airflow.apache.org/)
*   **Processing Engine**: [Apache Spark](https://spark.apache.org/) (Streaming & Batch)
*   **Table Format**: [Apache Iceberg](https://iceberg.apache.org/)
*   **Transformation**: [dbt](https://www.getdbt.com/)
*   **Storage**: [MinIO](https://min.io/) (S3 Compatible)
*   **Infrastructure**: Docker & Docker Compose

## 📂 Project Structure
- `airflow/`: Cấu hình Docker build và Plugins cho Airflow.
- `dags/`: Chứa các pipeline điều phối (DAGs) và code Spark.
- `dbt_project/`: Project dbt quản lý logic transform dữ liệu (Silver -> Gold).
- `metric_collector/`: Service API nhận dữ liệu và Kafka Producer.
- `docker-compose.yaml`: Định nghĩa toàn bộ hạ tầng (Infrastructure as Code).

## 🏗 Kiến trúc hệ thống (Architecture)
Hệ thống sử dụng kiến trúc **Lambda Architecture**, kết hợp giữa Streaming (Ingestion) và Batch (Processing).

![Pipeline Architecture](./utils/pipeline.png)

### Detailed Pipeline Flow

**1. Ingestion Layer: API & Kafka (High Throughput & Reliability)**
*   **API Gateway**: Thiết kế theo hướng **Non-blocking I/O** & **Fail-Fast**.
    *   Nhận payload JSON -> Gán Trace ID (`event_id`) -> Đẩy vào Memory Queue (`put_nowait`).
    *   Phản hồi client tức thì với độ trễ cực thấp (<10ms).
    *   Cơ chế **Backpressure**: Trả về `503 Service Unavailable` khi hàng đợi đầy để bảo vệ tài nguyên server.
*   **Producer Worker**:
    *   **Hybrid Batching Strategy**: Tự động chuyển đổi giữa 'Polling' (High Load) và 'Waiting' (Low Latency) để tối ưu throughput.
    *   **Non-blocking Serialization**: Dùng `run_in_executor` để đẩy tác vụ nén Avro (CPU-bound) sang thread riêng, giữ cho Event Loop luôn mượt mà.
    *   **Data Durability**: Cơ chế Retry kết hợp **Local Fallback** giúp đảm bảo không mất mát dữ liệu (Zero Data Loss) kể cả khi Kafka gặp sự cố.

**2. Bronze Layer: Streaming Ingestion (Kafka -> Iceberg)**
*   Spark Structured Streaming đọc liên tục từ Kafka topic.
*   Sử dụng **UDF Decoder** giải mã Avro binary ngay trong Spark.
*   **Flattening**: Làm phẳng cấu trúc JSON lồng nhau.
*   Ghi vào bảng Iceberg `demo.bronze.github_events` với chế độ Fanout Writer.

**3. Silver Layer (Part 1): Raw to Structured (Spark Batch)**
*   Trigger bởi Airflow định kỳ (Hourly).
*   **Incremental Load**: Chỉ đọc dữ liệu mới từ Bronze dựa vào watermark `ingestion_timestamp`.
*   **Parsing**: Parse cột `payload` JSON thành các cột quan trọng (PR details, Issue state...).
*   **Append Only**: Chiến lược ghi nhận sự kiện lịch sử, không update/delete để tối ưu performace (No MoR overhead).

**4. Silver Layer (Part 2): Structured to Enriched (dbt)**
*   Làm sạch dữ liệu, chuẩn hóa định dạng chuỗi.
*   **Event Categorization**: Phân loại sự kiện (Code Change, Social, Management...).
*   Tính toán Activity Score cho từng event.

**5. Gold Layer: Aggregation & Business Insights (dbt)**
*   **Daily Aggregation**: Tổng hợp hoạt động người dùng theo ngày.
*   **User Profiling**: Phân loại User (Developer, Reviewer, etc.) dựa trên hành vi đóng góp.
*   Tính toán các chỉ số xu hướng (Rolling Average).

**6. Orchestration**
*   **Airflow DAG** chạy định kỳ mỗi giờ:
    1.  `Spark Job`: Bronze -> Silver Parsed.
    2.  `dbt run`: Silver Enriched update.
    3.  `dbt run`: Gold User Activity update.
    4.  `dbt test`: Kiểm tra chất lượng dữ liệu (Unique ID, Not Null...).

**7. Maintenance Layer (Iceberg Table Optimization)**
Hệ thống Iceberg cần được bảo trì định kỳ để giải quyết vấn đề "Small Files" (do Streaming) và "Metadata Bloat" (do time travel history).

*   **Daily Maintenance**: (`maintenance_{bronze|silver}_by_day.py`)
    *   **Mục tiêu**: Tối ưu hiệu suất ĐỌC và LƯU TRỮ.
    *   **Compaction**: Gom các file nhỏ (do streaming/batch nhỏ sinh ra) thành file chuẩn (20MB).
    *   **Data Layout Optimization (Silver only)**: Sử dụng chiến thuật **Sort (Z-Order)** theo `event_type` & `created_at`. Giúp Query Engine bỏ qua (Skip) dữ liệu không cần thiết khi lọc, tăng tốc độ truy vấn đáng kể.
    *   **Cost Efficiency**: Sử dụng ràng buộc `min-input-files` để tránh chạy job lãng phí khi dữ liệu ít, và `cutoff` date để chỉ xử lý dữ liệu mới.

*   **Weekly Maintenance**: (`maintenance_{bronze|silver}_by_week.py`)
    *   **Mục tiêu**: Dọn dẹp rác hệ thống & Giải phóng dung lượng.
    *   **Expire Snapshots**: Xóa bỏ các metadata check-points cũ (> 7 ngày). Giữ lại tối thiểu 5 snapshots gần nhất để đảm bảo an toàn cho Time Travel.
    *   **Remove Orphan Files**: Xóa dứt điểm các file vật lý trôi nổi không còn được tham chiếu bởi bất kỳ snapshot nào.
    *   **Rewrite Position Deletes**: Không áp dụng (Vì hệ thống thiết kế dạng Append-Only).

## 🚀 Getting Started

### 1. Prerequisites
- Docker & Docker Compose installed.
- RAM tối thiểu: 6-8GB.

### 2. Setup Environment
Tạo file `.env` tại thư mục gốc và cấu hình các thông số kết nối (tham khảo file `docker-compose.yaml`):
