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
    'retry_delay': timedelta(minutes=60)
}

with DAG(
    dag_id="predict_aqi",
    default_args=default_args,
    description='full pipeline: Check → Silver → Gold → ML → Prediction → Anomaly → Save DB',
    start_date=datetime(2026, 4, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    # Task 1. Check data (Silver ready chưa)
    wait_for_data = BashSensor(
        task_id="wait_for_silver_data",
        bash_command="""
        count=$(docker exec trino trino --execute "
            SELECT COUNT(*) FROM iceberg.air_quality_db.air_quality_silver;
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

    # task 2. Silver → Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
            docker exec spark-master spark-submit \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/silver_to_gold.py {{ ds }}
        """,
    )

    # task 3. Train Model
    train_model = BashOperator(
        task_id="train_model",
        bash_command="""
            docker exec spark-master spark-submit \
                --conf spark.pyspark.python=python3 \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/train_model.py {{ ds }}
        """,
    )

    # task 4. Predict
    predict = BashOperator(
        task_id="predict",
        bash_command="""
            docker exec spark-master spark-submit \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/predict.py {{ ds }}
        """,
    )

    # task 5. Detect Anomaly
    anomaly = BashOperator(
        task_id="detect_anomaly",
        bash_command="""
            docker exec spark-master spark-submit \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1 \
                /home/spark/spark/spark_jobs/anomaly.py {{ ds }}
        """,
    )

    wait_for_data >> silver_to_gold >> train_model >> predict >> anomaly