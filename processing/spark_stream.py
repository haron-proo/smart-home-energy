#!/usr/bin/env python
# coding: utf-8

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
# 
# print("⚙️ تم تحميل مكتبات Spark Streaming بنجاح داخل الدوكر!")

# In[ ]:


# %% [markdown]
# ### 1. تجهيز المكتبات الخارجية والمسارات البيئية
# %%
import os
import sys
import shutil

offline_packages_path = "/home/jovyan/work/storage/packages"
os.makedirs(offline_packages_path, exist_ok=True)

# تثبيت psycopg2 إذا لم يكن موجوداً قبل بدء الجلسة لضمان الاستقرار
if not os.path.exists(os.path.join(offline_packages_path, "psycopg2")):
    print("📦 جاري تثبيت psycopg2 للمرة الأولى...")
    get_ipython().system('pip install --target={offline_packages_path} psycopg2-binary --quiet')

if offline_packages_path not in sys.path:
    sys.path.insert(0, offline_packages_path)

import psycopg2
print("✅ تم إعداد بيئة المكتبات بنجاح!")

# %% [markdown]
# ### 2. بناء جلسة Spark موحدة وشاملة الإعدادات
# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, expr, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# إنشاء جلسة واحدة فقط تحتوي على الـ Jars والـ Configurations المطلوبة لحماية القرص
spark = SparkSession.builder \
    .appName("SmartHome-Energy-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.minBatchesToRetain", "30") \
    .getOrCreate()

# تنظيف الـ Streams القديمة العالقة في الذاكرة إن وجدت
for q in spark.streams.active:
    q.stop()

print("⚙️ تم تشغيل جلسة Spark الموحدة بنجاح!")

# %% [markdown]
# ### 3. بناء الـ Schema والربط بكافكا
# %%
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
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("📡 تم الاتصال بـ Kafka وبدء الاستماع...")

# %% [markdown]
# ### 4. معالجة البيانات وتطبيق الفلاتر والنوافذ الزمنية
# %%
parsed_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

watermarked_df = parsed_stream_df.withWatermark("timestamp", "20 hours")

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

print("🚀 خط معالجة البيانات جاهز للإطلاق.")

# %% [markdown]
# ### 5. دالة الـ Upsert والتنظيف الدوري لـ Postgres
# %%
def write_to_postgres_only(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df.cache()
    row_count = batch_df.count()
    
    if row_count > 0:
        print(f"📥 [Batch {batch_id}] جاري معالجة وضخ {row_count} سجل...")
        try:
            conn = psycopg2.connect(
                host="smarthome-postgres", database="smarthome_energy", 
                user="smarthome_user", password="smarthome_password", port="5432"
            )
            cursor = conn.cursor()
            records = batch_df.collect()
            
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
                for r in records
            ]
            cursor.executemany(upsert_query, data_to_insert)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"   🔹 [Postgres] تم الـ Upsert بنجاح.")
        except Exception as e:
            print(f"   ❌ فشل في Postgres: {e}")
        
        # التنظيف التلقائي كل 5 دفعات
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
                print("   🧹 [Auto-Purge] تم تنظيف البيانات التاريخية القديمة اللحظية.")
            except Exception as e:
                print(f"   ⚠️ تنبيه الحذف التلقائي: {e}")
                
    batch_df.unpersist()

# %% [markdown]
# ### 6. تشغيل خط الأنابيب اللحظي وإبقاء الاتصال مستقراً
# %%
print("🚀 إطلاق تيار البث اللحظي للبوستغرس...")

query = (advanced_processed_df.writeStream
    .foreachBatch(write_to_postgres_only)
    .outputMode("update")
    .trigger(processingTime='10 seconds') 
    .option("checkpointLocation", "/home/jovyan/work/storage/check_spark_today")
    .start())

# الانتظار حتى انتهاء الاتصال
query.awaitTermination()

