from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_navigation': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'air_quality_silver_to_gold_pipeline',
    default_args=default_args,
    description='Pipeline ELT từ Silver sang Gold và Predict ML',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 10),
    catchup=False,
    max_active_runs=1, # Chạy 1 lần một để tránh xung đột dữ liệu
    tags=['air_quality', 'elt', 'ml', 'dwh']
) as dag:

    # Task 1: Xử lý Warehouse (Tạo các bảng Dim/Fact ở Gold)
    etl_warehouse = BashOperator(
        task_id='pyspark_elt_warehouse_gold',
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/dag2_elt.py {{ ds }}
        """,
    )

    # Task 2: Chuẩn bị dữ liệu cho ML và thực hiện Predict
    # Bước này sẽ kiểm tra Model, nếu có thì Predict và lưu vào Gold
    elt_ml = BashOperator(
        task_id='pyspark_elt_ml_gold',
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/elt_ml.py {{ ds }}
        """,
    )

    predict_ml = BashOperator(
        task_id='pyspark_predict_ml_gold',  
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/dag1_predict.py {{ ds }}
        """,
    )
    
    etl_warehouse >> elt_ml 