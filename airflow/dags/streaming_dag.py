from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

from airquality_kafka_producer import call_openmeteo

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def check_if_spark_running():
    """
    Hàm này trả về True nếu không tìm thấy job -> Cho phép chạy tiếp Task Bash
    Trả về False nếu job đang chạy -> Ngắt các task phía sau
    """
    check_cmd = "docker exec spark-master ps -ef"
    keyword = "spark_streaming_elt.py"
    try:
        output = subprocess.check_output(check_cmd, shell=True).decode('utf-8')
        # Kiểm tra xem có tiến trình nào chứa keyword không
        for line in output.splitlines():
            if keyword in line and "ps -ef" not in line:
                print(f"Job '{keyword}' đang chạy. Sẽ skip task khởi động.")
                return False 
        print("Không thấy job. Sẽ kích hoạt khởi động...")
        return True
    except:
        return True

with DAG(
    dag_id='air_quality_streaming_pipeline',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['airquality', 'kafka', 'bash_operator']
) as dag:

    # Task 1: Producer
    task_call_api = PythonOperator(
        task_id='call_api_and_send_kafka',
        python_callable=call_openmeteo,
    )

    # Task 2: Kiểm tra (Nếu True thì mới chạy Task 3)
    task_check_spark = ShortCircuitOperator(
        task_id='check_spark_before_start',
        python_callable=check_if_spark_running,
    )

    # Task 3: Start Spark bằng BashOperator
    task_start_spark = BashOperator(
        task_id='start_spark_streaming_job',
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/spark_streaming_elt.py
        """.strip(),
    )
    

    # Thiết lập phụ thuộc
    task_check_spark >> task_start_spark 
    task_call_api