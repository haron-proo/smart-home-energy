from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1️⃣ الإعدادات الافتراضية لجميع المهام
default_args = {
    'owner': 'smarthome_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ==============================================================================
# 2️⃣ الـ DAG الأول: تشغيل الـ Streaming واستمراريته (مرة واحدة عند التشغيل + فحص مستمر)
# ==============================================================================
with DAG(
    '1_smarthome_continuous_streaming',
    default_args=default_args,
    description='Ensure Spark Streaming Job starts once and runs 24/7',
    schedule_interval='@once',  # يعمل مرة واحدة عند تشغيل الحاوية وتفعيل الـ DAG
    catchup=False,
    max_active_runs=1,
) as dag_stream:

    launch_stream = BashOperator(
        task_id='launch_spark_stream',
        bash_command="""
        if docker exec smarthome-spark-notebook ps aux | grep -v grep | grep -q "spark_stream.ipynb"; then
            echo "✅ Spark Streaming dynamic job is already running smoothly!";
        else
            echo "⚠️ Alert: Streaming job is dead or disconnected! Restarting now...";
            docker exec -d smarthome-spark-notebook papermill /home/jovyan/work/processing/spark_stream.ipynb /home/jovyan/work/processing/output_spark_stream.ipynb
        fi
        """
    )

# ==============================================================================
# 3️⃣ الـ DAG الثاني: معالجة الباركيه التاريخي التزايدي (كل 5 دقائق)
# ==============================================================================
with DAG(
    '2_smarthome_historical_parquet',
    default_args=default_args,
    description='Runs historical compaction every 5 minutes',
    schedule_interval='*/5 * * * *',  # كل 5 دقائق بدقة
    catchup=False,
) as dag_historical:

    run_historical = BashOperator(
        task_id='run_historical_parquet',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/historical_parquet.ipynb /home/jovyan/work/processing/output_historical.ipynb'
    )

# ==============================================================================
# 4️⃣ الـ DAG الثالث: توليد تقارير الذكاء الاصطناعي (كل 10 دقائق)
# ==============================================================================
with DAG(
    '3_smarthome_ai_report_generator',
    default_args=default_args,
    description='Runs AI Report Generator every 10 minutes',
    schedule_interval='*/10 * * * *',  # كل 10 دقائق بدقة
    catchup=False,
) as dag_ai_report:

    run_ai_report = BashOperator(
        task_id='run_ai_report_generator',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/ai_report_generator.ipynb /home/jovyan/work/processing/output_ai_report_generator.ipynb'
    )

# ==============================================================================
# 5️⃣ الـ DAG الرابع: تحضير بيانات تعلم الآلة (كل 10 دقائق)
# ==============================================================================
with DAG(
    '4_smarthome_ml_data_processing',
    default_args=default_args,
    description='Runs ML Data Processing Notebook every 10 minutes',
    schedule_interval='*/10 * * * *',  # كل 10 دقائق بدقة
    catchup=False,
) as dag_ml_data:

    run_ml_data = BashOperator(
        task_id='run_ml_data_notebook',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/ml_data.ipynb /home/jovyan/work/processing/output_ml_data.ipynb'
    )

# ==============================================================================
# 6️⃣ الـ DAG الخامس: معالجة الباركيه اليومي الدفعة (مرة واحدة يومياً بالفجر)
# ==============================================================================
with DAG(
    '5_smarthome_process_parquet_once_daily',
    default_args=default_args,
    description='Runs parquet batch process once a day at 1:00 AM',
    schedule_interval='0 1 * * *',  # الساعة 1:00 صباحاً يومياً
    catchup=False,
) as dag_process_parquet:

    run_process_parquet = BashOperator(
        task_id='run_process_parquet_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/process_parquet_batch.ipynb /home/jovyan/work/processing/output_process_parquet.ipynb'
    )

# ==============================================================================
# 7️⃣ الـ DAG السادس: مستودع البيانات (ثلاث مرات يومياً بالتساوي)
# ==============================================================================
with DAG(
    '6_smarthome_spark_dwh_thrice_daily',
    default_args=default_args,
    description='Runs Spark DWH batch three times a day (4:00 AM, 12:00 PM, 8:00 PM)',
    schedule_interval='0 4,12,20 * * *',  # الساعة 4:00 و 12:00 و 20:00 (8 مساءً)
    catchup=False,
) as dag_dwh:

    run_dwh_batch = BashOperator(
        task_id='run_spark_dwh_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/spark_dwh_batch.ipynb /home/jovyan/work/processing/output_dwh_batch.ipynb'
    )