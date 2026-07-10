from datetime import datetime, timedelta
# 👍 إصلاح أسطر الاستيراد بشكل صريح ونظيف لمنع الـ NameError
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1️⃣ الإعدادات الافتراضية لجميع المهام
default_args = {
    'owner': 'smarthome_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# 2️⃣ الـ DAG الأول المطور: فحص وإطلاق الـ Streaming حياً 24/7 (كل 5 دقائق)
# ==============================================================================
with DAG(
    '1_smarthome_continuous_streaming',
    default_args=default_args,
    description='Check and Keep Spark Streaming Job Running 24/7',
    schedule_interval='*/5 * * * *',  # ⏱️ تم التعديل: يفحص ويشتغل تلقائياً كل 5 دقائق
    catchup=False,
    max_active_runs=1,  # 🛑 حماية: يمنع تداخل أكثر من فحص في نفس الوقت
) as dag_stream:

    launch_stream = BashOperator(
        task_id='launch_spark_stream',
        # 🚀 سكربت ذكي: يفحص إذا كانت عملية papermill للستريم تعمل داخل الحاوية.
        # إذا كانت تعمل (grep) يطبع رسالة وينجح، وإذا انقطعت يشغلها فوراً في الخلفية.
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
    '2_smarthome_historical_parquet_incremental',
    default_args=default_args,
    description='Runs historical compaction every 5 minutes to fetch new data smoothly',
    schedule_interval='*/5 * * * *',  # كل 5 دقائق بدقة
    catchup=False,
) as dag_historical:

    run_historical = BashOperator(
        task_id='run_historical_parquet',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/historical_parquet.ipynb /home/jovyan/work/processing/output_historical.ipynb'
    )

# ==============================================================================
# 4️⃣ الـ DAG الثالث: معالجة الباركيه اليومي (مرة واحدة باليوم في الفجر)
# ==============================================================================
with DAG(
    '3_smarthome_process_parquet_once_daily',
    default_args=default_args,
    description='Runs parquet batch process once a day at 1:00 AM',
    schedule_interval='0 1 * * *',
    catchup=False,
) as dag_process_parquet:

    run_process_parquet = BashOperator(
        task_id='run_process_parquet_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/process_parquet_batch.ipynb /home/jovyan/work/processing/output_process_parquet.ipynb'
    )

# ==============================================================================
# 5️⃣ الـ DAG الرابع: مستودع البيانات (مرة واحدة باليوم بوقت منفصل)
# ==============================================================================
with DAG(
    '4_smarthome_spark_dwh_once_daily',
    default_args=default_args,
    description='Runs Spark DWH batch once a day at 4:00 AM',
    schedule_interval='0 4 * * *',
    catchup=False,
) as dag_dwh:

    run_dwh_batch = BashOperator(
        task_id='run_spark_dwh_batch',
        bash_command='docker exec smarthome-spark-notebook papermill /home/jovyan/work/processing/spark_dwh_batch.ipynb /home/jovyan/work/processing/output_dwh_batch.ipynb'
    )