@echo off
chcp 65001 > nul
title 💾 تصدير صور دوكر للمشروع - أوفلاين

cd /d "%~dp0"
echo ==========================================================
echo 📦 جاري إنشاء مجلد لحفظ الصور...
if not exist "docker_images" mkdir docker_images

echo ==========================================================
echo ⏳ جاري تصدير صور دوكر (قد تستغرق هذه العملية دقائق حسب سرعة القرص)...
echo ==========================================================

echo 💾 [1/5] جاري تصدير Apache Airflow...
docker save apache/airflow:2.9.1 -o docker_images\airflow_2.9.1.tar

echo 💾 [2/5] جاري تصدير Metabase...
docker save metabase/metabase:v0.46.6 -o docker_images\metabase_v0.46.6.tar

echo 💾 [3/5] جاري تصدير PySpark Notebook...
docker save jupyter/pyspark-notebook:spark-3.5.0 -o docker_images\spark_notebook_3.5.0.tar

echo 💾 [4/5] جاري تصدير Postgres...
docker save postgres:14-alpine -o docker_images\postgres_14_alpine.tar

echo 💾 [5/5] جاري تصدير Apache Kafka...
docker save apache/kafka:3.7.0 -o docker_images\kafka_3.7.0.tar

echo ==========================================================
echo ✅ [نجاح ساحق] تم تصدير جميع الصور بنجاح داخل مجلد docker_images!
echo ==========================================================
pause