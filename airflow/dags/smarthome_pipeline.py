from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

# 1️⃣ الإعدادات الافتراضية لجميع المهام
default_args = {
    'owner': 'smarthome_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# دالة ذكية وموحدة لإرسال طلب تشغيل الـ Notebook عبر الـ API الداخلي للشبكة
def trigger_notebook_via_api(notebook_name):
    # نستخدم اسم الخدمة في شبكة دكر 'spark-notebook' والمنفذ 8000 المفتوح حديثاً
    url = "http://spark-notebook:8000/run-notebook"
    payload = {"notebook_name": notebook_name}

    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        print(f"📡 استجابة الـ API بنجاح: {response.json()}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"❌ فشل الاتصال بالـ API لتشغيل {notebook_name}: {e}")


# ==============================================================================
# 2️⃣ الـ DAG الأول: خط نقل البيانات التراكمي (البرونز -> السيلفر) - كل 5 دقائق
# ==============================================================================
with DAG(
        '2_smarthome_bronze_and_silver_ingestion',
        default_args=default_args,
        description='Extract raw data to Bronze, then clean to Silver every 5 minutes',
        schedule_interval='*/5 * * * *',  # كل 5 دقائق بدقة
        catchup=False,
        max_active_runs=1,  # منع تداخل العمليات
) as dag_bronze_silver:
    # أ. سحب البيانات المتسخة كلياً من كافكا وحفظها خام (Bronze Layer)
    run_raw_bronze = PythonOperator(
        task_id='ingest_to_raw_bronze',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'raw_bronze_parquet.ipynb'}
    )

    # ب. تنظيف البيانات الخام وحفظها في الباركيه التاريخي المعقم (Silver Layer)
    run_clean_silver = PythonOperator(
        task_id='clean_to_historical_silver',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'clean_silver_pipeline.ipynb'}
    )

    # التتابع المنطقي: البرونز أولاً ثم السيلفر
    run_raw_bronze >> run_clean_silver

# ==============================================================================
# 3️⃣ الـ DAG الثاني: توليد تقارير الذكاء الاصطناعي (كل 10 دقائق)
# ==============================================================================
with DAG(
        '3_smarthome_ai_report_generator',
        default_args=default_args,
        description='Runs AI Report Generator every 10 minutes',
        schedule_interval='*/10 * * * *',  # كل 10 دقائق بدقة
        catchup=False,
) as dag_ai_report:
    run_ai_report = PythonOperator(
        task_id='run_ai_report_generator',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'ai_report_generator.ipynb'}
    )

# ==============================================================================
# 4️⃣ الـ DAG الثالث: تحضير بيانات تعلم الآلة (كل 10 دقائق)
# ==============================================================================
with DAG(
        '4_smarthome_ml_data_processing',
        default_args=default_args,
        description='Runs ML Data Processing Notebook every 10 minutes',
        schedule_interval='*/10 * * * *',
        catchup=False,
) as dag_ml_data:
    run_ml_data = PythonOperator(
        task_id='run_ml_data_notebook',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'ml_data.ipynb'}
    )

# ==============================================================================
# 5️⃣ الـ DAG الرابع: معالجة الباركيه اليومي الدفعة (مرة واحدة يومياً بالفجر)
# ==============================================================================
with DAG(
        '5_smarthome_process_parquet_once_daily',
        default_args=default_args,
        description='Runs parquet batch process once a day at 1:00 AM',
        schedule_interval='0 1 * * *',
        catchup=False,
) as dag_process_parquet:
    run_process_parquet = PythonOperator(
        task_id='run_process_parquet_batch',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'process_parquet_batch.ipynb'}
    )

# ==============================================================================
# 6️⃣ الـ DAG الخامس: مستودع البيانات (ثلاث مرات يومياً بالتساوي)
# ==============================================================================
with DAG(
        '6_smarthome_spark_dwh_thrice_daily',
        default_args=default_args,
        description='Runs Spark DWH batch three times a day (4:00 AM, 12:00 PM, 8:00 PM)',
        schedule_interval='0 4,12,20 * * *',
        catchup=False,
) as dag_dwh:
    run_dwh_batch = PythonOperator(
        task_id='run_spark_dwh_batch',
        python_callable=trigger_notebook_via_api,
        op_kwargs={'notebook_name': 'spark_dwh_batch.ipynb'}
    )