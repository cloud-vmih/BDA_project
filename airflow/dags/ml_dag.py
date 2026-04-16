from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

from datetime import datetime, timedelta

default_args = {
    'owner': "airflow",
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=60),
    'max_active_runs': 1
}

with DAG(
    dag_id="predict_aqi",
    default_args=default_args,
    description='full pipeline: Check Gold → ML → Prediction → Anomaly',
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    # Task 1. Check data ở gold
    wait_for_data = BashSensor(
        task_id="wait_for_gold_data",
        bash_command="""
        count=$(docker exec trino trino --execute "
            SELECT COUNT(*) FROM iceberg.air_quality_ml.air_quality_features;
        " | tail -n 1 | tr -d '"[:space:]')

        echo "Record count: $count"

        if [ "$count" -gt 0 ]; then
            exit 0
        else
            exit 1
        fi
        """,
        poke_interval=30,
        timeout=300
    )

    # task 3. Train Model
    train_model = BashOperator(
        task_id="train_model",
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/train_onnx.py 
        """,
    )

    # task 4. Predict
    predict = BashOperator(
        task_id="predict",
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/predict_onnx.py
        """,
    )

    # task 5. Detect Anomaly
    anomaly = BashOperator(
        task_id="detect_anomaly",
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/anomaly_detection.py
        """,
    )

    wait_for_data >> train_model >> predict >> anomaly