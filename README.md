# DE_SCHEDULER
### *End-to-End Data Engineering Pipeline with Medallion Architecture*

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/dHung2412/DE_Scheduler)
[![Version](https://img.shields.io/badge/version-1.0.0-blue)](https://github.com/dHung2412/DE_Scheduler)
[![Tech Stack](https://img.shields.io/badge/stack-Airflow%20|%20Spark%20|%20MinIO%20|%20Kafka%20|%20Iceberg%20|%20Prometheus%20|%20Grafana-orange)](https://github.com/dHung2412/DE_Scheduler)

**DE_SCHEDULER** là một Data Pipeline (End-to-End) được thiết kế để thu thập, xử lý và phân tích dữ liệu sự kiện thời gian thực từ GitHub. Dự án áp dụng kiến trúc **Medallion Lakehouse** để tối ưu hóa hiệu suất truy vấn và đảm bảo tính nhất quán của dữ liệu.

---

## 1. Các tính năng chính (Features)

- **Real-time Ingestion**: Nhận dữ liệu sự kiện GitHub từ nguồn bên ngoài qua FastAPI với cơ chế Backpressure và Non-blocking I/O, sau đó đẩy vào Kafka.
- **Robust Architecture**: Thiết kế "Double Safety Net" với cơ chế Spill-to-disk tại Queue và Kafka Producer giúp đảm bảo **Zero Data Loss** khi hệ thống quá tải hoặc mất kết nối.
- **Optimized Batching**: Sử dụng kỹ thuật **Micro-batching** và **Application-level Compression** (đóng gói metrics vào file Avro) giúp giảm tải 99% cho Kafka Broker.
- **Medallion Architecture**: Tổ chức dữ liệu qua 3 lớp chuẩn hóa: **Bronze** (Dữ liệu thô), **Silver** (Dữ liệu đã dọn dẹp & làm phẳng), **Gold** (Dữ liệu tổng hợp cho Business).
- **ACID Transactions**: Sử dụng **Apache Iceberg** làm format bảng, mang lại khả năng Transactional (Acid), Time Travel và Schema Evolution.
- **Comprehensive Monitoring**: Tích hợp **Prometheus** để giám sát sâu từng chỉ số (Queue Size, Serialization Time, Kafka Latency, etc.).
- **Automated Orchestration**: Toàn bộ quy trình từ ingest đến transform được điều phối tự động bởi **Apache Airflow**.
- **Data Quality & Testing**: Tích hợp **dbt (data build tool)** để thực hiện các bài kiểm tra chất lượng dữ liệu (Uniqueness, Not Null, Referential Integrity).
- **Maintenance Automation**: Tự động hóa việc bảo trì bảng (Compaction, Snapshot Expiration, Z-Order Optimization) để duy trì hiệu suất hệ thống.

---

## 2. Công nghệ sử dụng (Tech Stack)

- **Ngôn ngữ**: Python, SQL.
- **Framework API**: FastAPI, Uvicorn.
- **Điều phối (Orchestration)**: Apache Airflow.
- **Xử lý dữ liệu (Computing Engine)**: Apache Spark (Structured Streaming & Batch).
- **Hàng đợi thông điệp (Messaging)**: Apache Kafka, Zookeeper.
- **Lưu trữ Lakehouse**: Apache Iceberg, MinIO (S3 Compatible).
- **Biến đổi dữ liệu (Transformation)**: dbt.
- **Giám sát & Cảnh báo (Monitoring)**: Prometheus, Grafana (Optional).
- **Cơ sở dữ liệu**: PostgreSQL (Metadata storage).
- **Hạ tầng**: Docker, Docker Compose.

---

## 3. Kiến trúc hệ thống (System Architecture)

Dự án tuân thủ mô hình xử lý dữ liệu hiện đại, kết hợp giữa Streaming và Batch:

1.  **Collection Layer (API)**: FastAPI Service nhận dữ liệu JSON thô. Sử dụng **Shared Queue** làm vùng đệm để tách biệt việc nhận request và xử lý.
2.  **Ingestion Layer (Worker)**:
    -   **Batching**: Worker gom metric từ Queue theo kích thước (1000 items) hoặc thời gian (5s).
    -   **Packing**: Nén cả lô thành 1 file **Avro Binary** duy nhất.
    -   **Reliability**: Gửi file Avro vào Kafka Topic `raw_metrics_avro`. Nếu lỗi, tự động ghi file xuống đĩa (Local Fallback) để Replay sau.
3.  **Bronze Layer**: Spark Structured Streaming đọc từ Kafka -> Giải mã Avro -> Ghi vào Iceberg Bronze tables.
4.  **Silver Layer**: Spark Batch (được Airflow trigger định kỳ) -> Parse JSON payload -> Làm phẳng cấu trúc dữ liệu.
5.  **Gold Layer**: dbt thực hiện các logic Business -> Tính toán metric -> Lưu trữ dữ liệu phân tích cuối cùng.

![Pipeline Architecture](./utils/pipeline.png)

---

## 4. Hướng dẫn cài đặt (Installation & Setup)

### Yêu cầu hệ thống
- Docker & Docker Compose.
- RAM tối thiểu: 8GB (Khuyến nghị 12GB+).

### Các bước cài đặt

1.  **Clone repository**:
    ```bash
    git clone https://github.com/dHung2412/DE_Scheduler.git
    cd DE_Scheduler
    ```

2.  **Cấu hình biến môi trường**:
    Tạo file `.env` từ file mẫu hoặc thay đổi các tham số

3.  **Khởi chạy hệ thống**:
    ```bash
    docker-compose up -d
    ```

4.  **Kiểm tra trạng thái**:
    - Airflow UI: `http://localhost:8080` (admin/admin)
    - Spark UI: `http://localhost:8081`
    - MinIO Console: `http://localhost:9000` (admin/admin123)
    - Grafana UI: `http://localhost:3000` (admin/admin)

---

## 5. Cách sử dụng (Usage)


### Bước 1: Chuẩn bị dữ liệu (Data Ingestion)
1.  **Khởi chạy Collector Service** (FastAPI):
    Dịch vụ này nhận dữ liệu từ nguồn ngoài và đẩy vào Kafka.
    ```bash
    cd metric_collector
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

2.  **Đẩy dữ liệu mẫu (Inject Data)**:
    Mở một terminal mới và chạy script để giả lập việc gửi dữ liệu GitHub tới Collector.
    ```bash
    python -m metric_collector.test.batch_injector
    ```

3.  **Giám sát Metrics (Optional)**:
    -   Truy cập endpoint `/metrics` để xem Prometheus metrics: `http://localhost:8000/metrics`.
    -   Kiểm tra sức khỏe hệ thống: `http://localhost:8000/health`.

4.  **Xử lý dữ liệu từ Kafka sang Bronze**:
    Chạy Spark job để đưa dữ liệu từ Kafka vào bảng Iceberg lớp Bronze.
    ```bash
    python -m dags.spark_jobs.kafka_bronze.process_kafka_to_bronze
    ```

### Bước 2: Kích hoạt Pipeline điều phối (Airflow)
1.  Truy cập Airflow UI tại `http://localhost:8080`.
2.  Bật (Unpause) DAG `github_events_pipeline`.
3.  Kích hoạt (Trigger) DAG để thực hiện luồng: **Bronze -> Silver -> Gold** và thực hiện **dbt test**.

### Bước 3: Bảo trì hệ thống
Các DAG `maintenance_bronze_*` và `maintenance_silver_*` được thiết lập để chạy định kỳ nhằm tối ưu hóa file lưu trữ (Compaction) và dọn dẹp Metadata trên MinIO.

---

## 6. Truy vấn dữ liệu & Phân tích (Analytics)
Bạn có thể sử dụng Jupyter Notebook tích hợp sẵn để kiểm tra kết quả tại mỗi Layer:
- **Địa chỉ**: `http://localhost:8888`
- **Cách dùng**: Sử dụng Spark SQL để query các bảng trong namespaces `demo.bronze`, `demo.silver`, `demo.gold`.

---

## 7. Ảnh chụp màn hình hoặc Demo (Screenshots/Gifs)

*(Hiện tại chưa có ảnh demo)*

---

## 8. Cấu trúc thư mục (Project Structure)

```text
DE_Scheduler/
├── airflow/              # Cấu hình Docker & Plugins cho Airflow
├── dags/                 # Các DAGs điều phối pipeline
│   ├── spark_jobs/       # Chứa code Spark (Python) cho từng Layer
│   └── af_*.py           # Định nghĩa Workflow cho Airflow
├── dbt_project/          # Dự án dbt (Macros, Models, Tests)
├── metric_collector/     # Service thu thập dữ liệu & Kafka Producer
├── monitoring/           # Cấu hình Prometheus
├── dbt_profiles/         # Cấu hình kết nối dbt tới Spark Thrift Server
├── notebooks/            # Jupyter Notebooks phục vụ phân tích (EDA)
├── utils/                # Các scripts bổ trợ và schema định nghĩa
├── docker-compose.yaml   # Định nghĩa toàn bộ hạ tầng dịch vụ
└── .env                  # Biến môi trường cấu hình hệ thống
```

