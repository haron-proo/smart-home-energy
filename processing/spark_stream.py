import os
import sys
import shutil
import time

# --- 1. تجهيز المكتبات الخارجية والمسارات البيئية ---
offline_packages_path = "/home/jovyan/work/storage/packages"
os.makedirs(offline_packages_path, exist_ok=True)

# التأكد من تثبيت مكتبة الاتصال بقاعدة البيانات
if not os.path.exists(os.path.join(offline_packages_path, "psycopg2")):
    print("📦 جاري تثبيت psycopg2 للمرة الأولى...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", f"--target={offline_packages_path}", "psycopg2-binary", "--quiet"])

if offline_packages_path not in sys.path:
    sys.path.insert(0, offline_packages_path)

import psycopg2
print("✅ تم إعداد بيئة المكتبات بنجاح!")

# --- 2. بناء جلسة Spark موحدة ومحسنة السرعة ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, expr, window, avg, sum, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

spark = SparkSession.builder \
    .appName("SmartHome-Energy-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.minBatchesToRetain", "10") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.streaming.fileSink.log.cleanupDelay", "30000") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# إيقاف أي تيارات بث نشطة مسبقاً لمنع التعارض
for q in spark.streams.active:
    try:
        q.stop()
    except Exception:
        pass

print("⚙️ تم تشغيل جلسة Spark الموحدة بنجاح!")

# --- 3. بناء الـ Schema والربط بكافكا ---
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("house_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("is_room_occupied", BooleanType(), True),
    StructField("power_consumption_watts", DoubleType(), True),
    StructField("status", StringType(), True)
])

kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "smarthome-kafka:29092") \
    .option("subscribe", "energy_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("📡 تم الاتصال بـ Kafka وبدء الاستماع...")

# --- 4. معالجة البيانات وتطبيق النوافذ الزمنية وفلاتر الـ 10 دقائق ---
parsed_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# تصفية البيانات بدقة لقراءة آخر 10 دقائق فقط لتجنب استهلاك موارد الذاكرة
ten_minutes_ago = current_timestamp() - expr("INTERVAL 10 MINUTES")
filtered_time_df = parsed_stream_df.filter(col("timestamp") >= ten_minutes_ago)

# وضع حد أقصى للتأخير (Watermark) بـ 15 دقيقة
watermarked_df = filtered_time_df.withWatermark("timestamp", "15 minutes")

context_clean_df = watermarked_df \
    .filter(
        (col("power_consumption_watts").isNotNull()) &
        (~col("status").isin("LOST_SIGNAL", "SENSOR_FAULT", "ERROR")) &
        (col("power_consumption_watts") >= 0) &
        (
            ((col("device_type") == "AC") & (col("power_consumption_watts") <= 6000)) |
            ((col("device_type") == "Oven") & (col("power_consumption_watts") <= 4000)) |
            ((col("device_type") == "Washing_Machine") & (col("power_consumption_watts") <= 3000)) |
            ((col("device_type").isin("Lighting", "Smart_Bulb", "TV")) & (col("power_consumption_watts") <= 500)) |
            (~col("device_type").isin("AC", "Oven", "Washing_Machine", "Lighting", "Smart_Bulb", "TV") & (col("power_consumption_watts") <= 2000))
        )
    ) \
    .dropDuplicates(["timestamp", "device_id"])

advanced_processed_df = context_clean_df \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("zone"),
        col("device_type")
    ) \
    .agg(
        avg("power_consumption_watts").alias("avg_power_watts"),
        sum("power_consumption_watts").alias("total_power_watts")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("zone"),
        col("device_type"),
        col("avg_power_watts"),
        col("total_power_watts")
    )

print("🚀 خط معالجة البيانات مجهز بالفلترة الزمنية الفورية وجاهز للعمل.")

# --- 5. دالة الـ Upsert والتنظيف التلقائي لـ Postgres ---
def write_to_postgres_only(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    def db_upsert_partition(partition_iterator):
        import sys
        offline_path = "/home/jovyan/work/storage/packages"
        if offline_path not in sys.path:
            sys.path.insert(0, offline_path)
        import psycopg2

        try:
            conn = psycopg2.connect(
                host="smarthome-postgres", database="smarthome_energy",
                user="smarthome_user", password="smarthome_password", port="5432"
            )
            cursor = conn.cursor()

            upsert_query = """
                INSERT INTO spark_windowed_energy (zone, device_type, avg_power_watts, total_power_watts, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (zone, device_type, window_end) 
                DO UPDATE SET 
                    avg_power_watts = EXCLUDED.avg_power_watts,
                    total_power_watts = EXCLUDED.total_power_watts,
                    window_start = EXCLUDED.window_start;
            """

            data_to_insert = [
                (r['zone'], r['device_type'], float(r['avg_power_watts']), float(r['total_power_watts']), r['window_start'], r['window_end'])
                for r in partition_iterator if r['zone'] is not None and r['device_type'] is not None and r['window_end'] is not None
            ]

            if data_to_insert:
                cursor.executemany(upsert_query, data_to_insert)
                conn.commit()

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ فشل الضخ في قاعدة البيانات: {e}")

    print(f"📥 [Batch {batch_id}] جاري ضخ البيانات عبر الـ Partitions...")
    batch_df.foreachPartition(db_upsert_partition)

    # التنظيف التلقائي الذكي كل 5 دفعات
    if batch_id % 5 == 0:
        try:
            conn = psycopg2.connect(host="smarthome-postgres", database="smarthome_energy", user="smarthome_user", password="smarthome_password", port="5432")
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM spark_windowed_energy 
                WHERE window_end < (SELECT MAX(window_end) FROM spark_windowed_energy) - INTERVAL '10 minutes';
            """)
            conn.commit()
            cursor.close()
            conn.close()
            print("🧹 [Auto-Purge] تم تنظيف البيانات اللحظية القديمة.")
        except Exception as e:
            print(f"⚠️ تنبيه الحذف التلقائي: {e}")

# --- 6. إدارة وتشغيل خط الأنابيب اللحظي بأمان ---
base_checkpoint_dir = "/home/jovyan/work/storage/check_spark_o"

# تخطي مشاكل أقفال نظام ويندوز للـ Checkpoints
if os.path.exists(base_checkpoint_dir):
    try:
        trash_dir = f"{base_checkpoint_dir}_trash_{int(time.time())}"
        print("🔄 نقل مسار الـ Checkpoint القديم لتجاوز التعليق...")
        os.rename(base_checkpoint_dir, trash_dir)

        # الحذف الفعلي والآمن عبر مكتبة بايثون الأصلية
        shutil.rmtree(trash_dir, ignore_errors=True)
        print("🧹 تم تنظيف المجلدات القديمة بنجاح.")
    except Exception as e:
        # حل احترافي بديل في حال قفل الملفات بالكامل
        unique_suffix = int(time.time())
        base_checkpoint_dir = f"{base_checkpoint_dir}_{unique_suffix}"
        print(f"🚀 [تجاوز تلقائي] تم التحويل لمسار فريد لتجنب بطء الأقفال: {base_checkpoint_dir}")

print("🛰️ إطلاق تيار البث المباشر المسرّع...")

query = (advanced_processed_df.writeStream
    .foreachBatch(write_to_postgres_only)
    .outputMode("update")
    .trigger(processingTime='10 seconds')
    .option("checkpointLocation", base_checkpoint_dir)
    .start())

query.awaitTermination()