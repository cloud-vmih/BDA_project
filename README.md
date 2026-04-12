# Hadoop Cluster Docker

Một dự án Docker Compose để khởi tạo môi trường Big Data tích hợp Hadoop, Hive, Iceberg, Kafka, Spark, Airflow, Trino và Superset.

## Mục tiêu

- Khởi tạo cụm Hadoop gồm master và slave
- Cài Hive và cấu hình Iceberg
- Cài Spark master/worker và cấu hình Iceberg runtime
- Khởi chạy Airflow với các DAG Spark ELT
- Khởi chạy Trino để truy vấn dữ liệu Iceberg
- Mở Superset để trực quan hóa dữ liệu

## Cấu trúc chính

- `docker-compose.yml` - định nghĩa tất cả dịch vụ
- `Dockerfile` - cài cơ sở Hadoop/Hive/Iceberg
- `spark/Dockerfile` - cài Spark master/worker
- `spark/spark-defaults.conf` - cấu hình Spark và Iceberg
- `airflow/Dockerfile` - cài Airflow và cài Python packages từ `airflow/requirements.txt`
- `airflow/dags/` - chứa DAG Airflow và Spark job
- `trino/Dockerfile` - image Trino và cấu hình Iceberg catalog
- `master_config/`, `slave1_config/` - cấu hình Hadoop/Hive/YARN

## Dịch vụ và port

- `master` (Hadoop master)
  - 9870: Namenode UI
  - 8088: YARN ResourceManager UI
  - 9083: Hive Metastore
  - 9000: HDFS NameNode
- `slave1` (Hadoop slave)
- `postgres`
  - 5432: PostgreSQL cho Airflow metadata
- `airflow`
  - 8080: Airflow Web UI
- `trino`
  - 8082: Trino UI/HTTP
- `superset`
  - 8083: Superset UI
- `zookeeper`
  - 2181: Zookeeper
- `kafka`
  - 9092: Kafka broker
- `spark-master`
  - 8081: Spark Master UI
  - 7077: Spark Master port

## Cài đặt và chạy

1. Đảm bảo có Docker và Docker Compose.
2. Giữ file `apache-hive-3.1.3-bin.tar.gz` trong thư mục gốc của dự án.
3. Chạy build và khởi động:

```bash
docker compose up --build
```

hoặc nếu dùng Docker Compose cũ:

```bash
docker-compose up --build
```

### Chạy `init_all_services.sh` và `start_all_services.sh`

```bash
wsl bash ./init_all_services.sh
```

```bash
wsl bash ./start_all_services.sh
```

Script sẽ:
- bật SSH trên master và slave
- xóa data HDFS cũ để đồng bộ cluster IDs
- format NameNode
- khởi động HDFS và YARN
- tạo db metastore trên postgres
- chạy service metastore
- tạo Hive lakehouse

## Lưu ý quan trọng

- `Dockerfile` root tải Hadoop 3.4.1, Hive 3.1.3 và Iceberg runtime `iceberg-spark-runtime-4.0_2.13:1.10.1`.
- `spark/defaults.conf` đã cấu hình `spark.jars.packages` để Spark tự kéo gói Iceberg khi khởi động.
- `airflow/Dockerfile` cài dependencies từ `airflow/requirements.txt`.
- kafka phải có zookeeper, hive & superset & airflow phải có postgres
- khi chạy service cần metastore phải đảm bảo service metastore đã được bật (chạy file start_all_services.sh )

## Các workflow hiện có

- `airflow/dags/airflow_test.py` - kiểm tra pipeline end-to-end: ingestion → Spark ELT → Iceberg → Trino
- `airflow/dags/spark_jobs/spark_elt.py` - job Spark ELT viết dữ liệu vào Iceberg

## Chạy airflow_test.py

- Compose up tất các Container cần thiết cho airflow (postgres, master, slave, spark-master/worker, trino, airflow)

```bash
docker compose up postgres master slave1 spark-master spark-worker trino airflow
```

- Start service hadoop trên master, vào container master

```bash
start-all.sh
```

- Start service metastore (đảm bảo đã init - chạy file start_all_services.sh)

```bash
hive --service metastore
```

- Mở UI localhost:8080, nhập user = admin, password = admin, xem dags hiện có và bấm Trigger Dag 
- Bấm vào từng task để xem log

--Lưu ý:
--Nếu không thấy dags nào, vào terminal
```bash
docker exec -it airflow bash -c '
    source /home/airflow/airflow/venv/bin/activate &&
    airflow dags trigger full_lakehouse_test_pipeline
'
```

## Cấu hình đặc biệt

- `trino/catalog/iceberg.properties` kết nối Trino với Iceberg qua Hive Metastore
- `master_config/hive-site.xml` và `spark/spark-defaults.conf` định nghĩa Iceberg catalog

## Một số lệnh 

- Vào container master Hadoop:

```bash
docker exec -it 23133083thuyvan-master bash
```

- Trước khi format NameNode kiểm tra quyền của thư mục hadoop_data bên master và slave có phải owner là user hay chưa, nếu chưa thực hiện lệnh dưới đây cho cả master và slave

```bash
sudo chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hadoop/hadoop_data
```
- Format lại NameNode nếu cần:

```bash
hdfs namenode -format
```
- Khởi động dịch vụ Hadoop trong container master:

```bash
start-all.sh
jps
```

- Vào container slave:

```bash
docker exec -it 23133083thuyvan-slave1 bash
```

- Vào container khác cũng sử dụng lệnh tương tự như trên, chỉ cần đổi tên container

## Ghi chú

- Nếu container Spark worker cần kết nối đến master, biến môi trường `SPARK_MASTER_URL` đã thiết lập trong `docker-compose.yml`.
- Nếu thay đổi DAG hoặc cấu hình Airflow, cần restart container `airflow`.
- Nếu thay đổi cấu hình Spark, cần rebuild container `spark` và restart dịch vụ `spark-master`/`spark-worker`.
