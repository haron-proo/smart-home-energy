@echo off
chcp 65001 > nul
title 📥 استيراد صور دوكر للمشروع - الجهاز الجديد

cd /d "%~dp0"
echo ==========================================================
echo 🚀 جاري فحص واستيراد صور دوكر من المجلد المحمول...
echo ==========================================================

if not exist "docker_images" (
    echo ❌ خطأ: لم يتم العثور على مجلد docker_images! تأكد من وجوده بجانب هذا الملف.
    goto end
)

echo 📥 [1/5] جاري شحن Airflow...
docker load -i docker_images\airflow_2.9.1.tar

echo 📥 [2/5] جاري شحن Metabase...
docker load -i docker_images\metabase_v0.46.6.tar

echo 📥 [3/5] جاري شحن PySpark Notebook...
docker load -i docker_images\spark_notebook_3.5.0.tar

echo 📥 [4/5] جاري شحن Postgres...
docker load -i docker_images\postgres_14_alpine.tar

echo 📥 [5/5] جاري شحن Kafka...
docker load -i docker_images\kafka_3.7.0.tar

echo ==========================================================
echo ✅ [نجاح] تم استيراد كافة الصور في الجهاز الجديد بنجاح!
echo 💡 يمكنك الآن كتابة الأمر التالي لتشغيل مشروعك فوراً أوفلاين:
echo    docker-compose -f docker-compose-clean.yaml up -d
echo ==========================================================

:end
pause