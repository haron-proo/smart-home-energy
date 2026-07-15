from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'smarthome_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 1_smarthome_continuous_streaming
with DAG(
    '1_smarthome_continuous_streaming',
    default_args=default_args,
    description='Ensure Spark Streaming Job starts once and runs 24/7',
    schedule='@once',  # 👈 تم التحديث إلى schedule
    catchup=False,
    max_active_runs=1,
) as dag_stream:

    launch_stream = BashOperator(
        task_id='launch_spark_stream',
        bash_command="""
        if docker exec smarthome-spark-notebook ps aux | grep -v grep | grep -q "spark_stream.ipynb"; then
            echo "✅ Spark Streaming job is already running smooth!";
        else
            echo "⚠️ Restarting...";
            docker exec -d smarthome-spark-notebook papermill /home/jovyan/work/processing/spark_stream.ipynb /home/jovyan/work/processing/output_spark_stream.ipynb
        fi
        """
    )

# 2_smarthome_historical_parquet
with DAG(
    '2_smarthome_historical_parquet',
    default_args=default_args,
    description='Runs historical compaction every 5 minutes',
    schedule='*/5 * * * *',  # 👈 تم التحديث
    catchup=False,
) as dag_historical:

    run_historical = BashOperator(
        task_id='run_historical_parquet',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/historical_parquet.ipynb /home/jovyan/work/processing/output_historical.ipynb'
    )

# 3_smarthome_ai_report_generator
with DAG(
    '3_smarthome_ai_report_generator',
    default_args=default_args,
    description='Runs AI Report Generator every 10 minutes',
    schedule='*/10 * * * *',  # 👈 تم التحديث
    catchup=False,
) as dag_ai_report:

    run_ai_report = BashOperator(
        task_id='run_ai_report_generator',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/ai_report_generator.ipynb /home/jovyan/work/processing/output_ai_report_generator.ipynb'
    )

# 4_smarthome_ml_data_processing
with DAG(
    '4_smarthome_ml_data_processing',
    default_args=default_args,
    description='Runs ML Data Processing Notebook every 10 minutes',
    schedule='*/10 * * * *',  # 👈 تم التحديث
    catchup=False,
) as dag_ml_data:

    run_ml_data = BashOperator(
        task_id='run_ml_data_notebook',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/ml_data.ipynb /home/jovyan/work/processing/output_ml_data.ipynb'
    )

# 5_smarthome_process_parquet_once_daily
with DAG(
    '5_smarthome_process_parquet_once_daily',
    default_args=default_args,
    description='Runs parquet batch process once a day',
    schedule='0 1 * * *',  # 👈 تم التحديث
    catchup=False,
) as dag_process_parquet:

    run_process_parquet = BashOperator(
        task_id='run_process_parquet_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/process_parquet_batch.ipynb /home/jovyan/work/processing/output_process_parquet.ipynb'
    )

# 6_smarthome_spark_dwh_thrice_daily
with DAG(
    '6_smarthome_spark_dwh_thrice_daily',
    default_args=default_args,
    description='Runs Spark DWH batch three times a day',
    schedule='0 4,12,20 * * *',  # 👈 تم التحديث
    catchup=False,
) as dag_dwh:

    run_dwh_batch = BashOperator(
        task_id='run_spark_dwh_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/spark_dwh_batch.ipynb /home/jovyan/work/processing/output_dwh_batch.ipynb'
    )