from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests, os, subprocess, time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Sử dụng context manager cho DAG là cách tốt nhất
with DAG(
    dag_id='kaggle_daily_batch_pipeline',
    default_args=default_args,
    description='Full pipeline: Ingestion → Spark ELT → Iceberg → Trino',
    schedule_interval='@daily',         
    start_date=datetime(2026, 3, 30),
    catchup=False,
) as dag:

    # TASK 1: TẢI FILE TỪ KAGGLE
    def download_kaggle_data():
        USERNAME = "vanthuy1234"
        API_TOKEN = "KGAT_bc501bbd8e8baa5e7c6c48feb9d4e2e6"   

        url = "https://www.kaggle.com/api/v1/datasets/download/nelgiriyewithana/global-weather-repository/GlobalWeatherRepository.csv"
        
        headers = {
            "Authorization": f"Bearer {API_TOKEN}"
        }
        
        print("Đang tải file từ Kaggle bằng API Token...")
        
        try:
            response = requests.get(url, headers=headers, stream=True)
            if response.status_code == 200:
                os.makedirs("/tmp/kaggle_data", exist_ok=True)
                file_path = "/tmp/kaggle_data/GlobalWeatherRepository.csv"
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print(f"Tải thành công! File lưu tại: {file_path}")
                
                # Chờ HDFS sẵn sàng trước khi upload
                hdfs_ready = False
                for attempt in range(5):
                    result = subprocess.run(
                        ["docker", "exec", "23133083thuyvan-master",
                         "hdfs", "dfs", "-ls", "/"],
                        capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        hdfs_ready = True
                        print(f"HDFS sẵn sàng (attempt {attempt + 1})")
                        break
                    print(f"HDFS chưa sẵn sàng, thử lại sau 15s... (attempt {attempt + 1}/5)")
                    time.sleep(15)
                
                if not hdfs_ready:
                    raise Exception("HDFS không sẵn sàng sau 5 lần thử")
                
                start_date = datetime.now()
                # Upload lên HDFS
                subprocess.run([
                    "docker", "exec", "23133083thuyvan-master",
                    "hdfs", "dfs", "-mkdir", "-p", f"/user/hive/lakehouse/bronze/raw_kaggle/{start_date.strftime('%Y-%m-%d')}"
                ], check=True)
                
                # Copy file từ Airflow container sang Hadoop master container
                remote_tmp = "/tmp/GlobalWeatherRepository.csv"
                subprocess.run([
                    "docker", "cp", file_path, f"23133083thuyvan-master:{remote_tmp}"
                ], check=True)
                print(f"Đã copy file vào Hadoop master: {remote_tmp}")
                
                hdfs_path = f"/user/hive/lakehouse/bronze/raw_kaggle/{start_date.strftime('%Y-%m-%d')}/GlobalWeatherRepository.csv"
                
                subprocess.run([
                    "docker", "exec", "23133083thuyvan-master",
                    "hdfs", "dfs", "-put", "-f", remote_tmp, hdfs_path
                ], check=True)
                print(f"Đã upload lên HDFS: {hdfs_path}")
            else:
                print(f"Lỗi tải file: {response.status_code}")
                raise Exception("Không tải được file từ Kaggle")
        except Exception as e:
            print(f"Lỗi: {str(e)}")
            raise

    download_task = PythonOperator(
        task_id='download_kaggle_csv',
        python_callable=download_kaggle_data,
    )

    # TASK 3: SPARK BATCH ELT (Bronze -> Silver)
    spark_etl_task = BashOperator(
        task_id='spark_etl_bronze_to_silver',
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/batch_elt_to_silver.py 
        """,
    )

    # TASK 4: KIỂM TRA BẰNG TRINO 
    check_trino_task = BashOperator(
        task_id='check_data_in_trino',
        bash_command="""
            docker exec trino trino --execute "
                SELECT 
                    count(*) as total_records
                FROM iceberg.air_quality_db.air_quality_silver;
            "
        """,
    )

    # Thiết lập thứ tự chạy
    download_task >> spark_etl_task >> check_trino_task