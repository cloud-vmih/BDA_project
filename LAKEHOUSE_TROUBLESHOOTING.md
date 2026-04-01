# Tổng Hợp Các Lỗi Và Cách Khắc Phục (Lakehouse Data Pipeline)

Tài liệu này tổng hợp chi tiết toàn bộ các lỗi đã gặp phải trong quá trình xây dựng, cấu hình và chạy luồng dữ liệu (pipeline) Lakehouse bằng Airflow, Hadoop, Spark, Hive Metastore, PostgreSQL, và Trino trên hệ thống Docker.

---

## 1. Lỗi Quyền Truy Cập Docker Socket Của Airflow (`/var/run/docker.sock`)
* **Mô tả lỗi:** Các task Airflow dùng `BashOperator` để gọi các lệnh `docker exec` (ví dụ: gửi lệnh sang container Hadoop hoặc Spark) bị thất bại với lỗi `permission denied` khi cố gắng truy cập vào `/var/run/docker.sock`.
* **Nguyên nhân:** Daemon Docker trên máy host chạy dưới quyền `root:docker` (hoặc một Group ID cụ thể). Người dùng `airflow` bên trong container không có cùng GID với group `docker` ở ngoài host nên không có quyền gọi Docker API.
* **Cách khắc phục:** 
  1. Cập nhật `Dockerfile` của Airflow để cài đặt `sudo` và cấp quyền không cần mật khẩu cho user `airflow`.
  2. Viết file khởi động `start-airflow.sh` có logic: Tự động phát hiện GID của `/var/run/docker.sock` lúc runtime, tạo một group tương ứng trong container và gán user `airflow` vào group đó để có quyền native, cho phép thực thi `docker exec` một cách mượt mà và bảo mật.

---

## 2. Lỗi quyền tạo bảng của Trino
* **Mô tả lỗi:** Trino báo lỗi khi tạo bảng Iceberg hoặc truy cập metadata do thiếu quyền trên Hive Metastore/Hadoop.
* **Nguyên nhân:** Trino sử dụng cấu hình catalog Iceberg dựa trên Hive Metastore. Nếu file cấu hình `iceberg.properties` không đúng hoặc nếu account/trust relationship giữa Trino và Hadoop không có đủ quyền, Trino sẽ không thể tạo bảng hoặc ghi metadata.
* **Cách khắc phục:**
  1. Kiểm tra `trino/catalog/iceberg.properties` và đảm bảo `hive.metastore.uri` trỏ chính xác tới `thrift://<container_master_name>:9083`.
  2. Đảm bảo Trino có quyền đọc/ghi tới HDFS warehouse path (`hdfs://.../user/hive/warehouse`).
  3. Nếu cần, cấp quyền HDFS bằng lệnh:

```bash
hadoop fs -chmod -R 777 /user/hive/warehouse
```

## 3. Lỗi tạo topic Kafka và internal topic replication
* **Mô tả lỗi:** Khi tạo topic hoặc khởi chạy Kafka, các internal topic như `__consumer_offsets` bị tạo với `replication.factor=3`, gây lỗi khi cluster chỉ có 1 broker.
* **Nguyên nhân:** Cấu hình Kafka mặc định yêu cầu replication factor cao hơn số broker thực tế.
* **Cách khắc phục:** Thêm các giá trị cấu hình sau vào file `kafka/server.properties` hoặc cấu hình Kafka tương ứng:

```properties
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

## 4. Lỗi Spark 4.1.1 chưa hỗ trợ Iceberg runtime tương thích
* **Mô tả lỗi:** Khi dùng Spark 4.1.1 với Iceberg bundle, Spark không khởi động đúng hoặc job Iceberg bị lỗi do runtime không tương thích.
* **Nguyên nhân:** Phiên bản Spark 4.1.1 mới hơn chưa có Iceberg runtime bản tương thích sẵn. Dự án cần dùng Spark 4.0.x với Iceberg runtime tương ứng.
* **Cách khắc phục:**
  1. Dùng Spark 4.0.2 trong `spark/Dockerfile`.
  2. Cấu hình Iceberg runtime đúng: `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1`.
  3. Đảm bảo `spark/spark-defaults.conf` có dòng:

```properties
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1
```

## TỔNG KẾT
Kết quả của quá trình gỡ lỗi tuần tự như trên là hệ thống hiện tại đã tạo ra được một Pipeline thành công 100%: 
**"Airflow tải dữ liệu → Đẩy vào HDFS → Spark xử lý ETL → Lưu vào Hive Metastore bằng chuẩn Iceberg Open Table Format → Trino SQL Query thành công."**
