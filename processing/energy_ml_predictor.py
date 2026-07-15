import os
import sys
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime

# 1. إعداد المسارات للمكتبات أوفلاين
offline_packages_path = "/home/jovyan/work/storage/packages"
if offline_packages_path not in sys.path:
    sys.path.insert(0, offline_packages_path)

# الآن نستدعي مكاتب التعلم الآلي بأمان
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
import joblib  # لحفظ النموذج واستدعائه لاحقاً دون الحاجة لإعادة التدريب في كل مرة

MODEL_PATH = "/home/jovyan/work/storage/energy_model.pkl"


def fetch_historical_data():
    """جلب البيانات التاريخية من قاعدة البيانات لتدريب النموذج"""
    print("🔌 جاري الاتصال بقاعدة البيانات لجلب بيانات التدريب...")
    conn = psycopg2.connect(
        host="smarthome-postgres",
        database="smarthome_energy",
        user="smarthome_user",
        password="smarthome_password",
        port="5432"
    )

    # سنقوم بجلب قراءات الطاقة السابقة من جدول spark_windowed_energy أو جدول الإحصائيات
    # لغرض المثال، سنفترض أننا نسحب البيانات التاريخية لمعرفة العلاقة بين (الساعة، المنطقة، ونوع الجهاز) والاستهلاك
    query = """
        SELECT EXTRACT(HOUR FROM window_end) as hour, zone, device_type, avg_power_watts 
        FROM spark_windowed_energy;
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def train_predictive_model():
    """تدريب نموذج تعلم آلي للتنبؤ باستهلاك الطاقة المتوقع"""
    df = fetch_historical_data()

    if df.empty or len(df) < 10:
        print("⚠️ البيانات التاريخية غير كافية حالياً لتدريب النموذج. سيتم استخدام نموذج تقديري مؤقت.")
        return None

    print(f"📊 تم جلب {len(df)} سجل لتدريب النموذج. جاري معالجة البيانات...")

    # تحويل القيم النصية (Zone و Device Type) إلى قيم رقمية (One-Hot Encoding) ليقفلها النموذج
    df_encoded = pd.get_dummies(df, columns=['zone', 'device_type'])

    # تحديد المدخلات (Features) والمخرجات (Target)
    X = df_encoded.drop(columns=['avg_power_watts'])
    y = df_encoded['avg_power_watts']

    # تقسيم البيانات لتدريب واختبار
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # تدريب نموذج شجرة القرار (يمتاز بالسرعة والكفاءة في بيئات الـ IoT)
    model = DecisionTreeRegressor(max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    # قياس دقة النموذج
    score = model.score(X_test, y_test)
    print(f"🎯 تم تدريب النموذج بنجاح! دقة التنبؤ الحالية: {score * 100:.2f}%")

    # حفظ أعمدة التدريب لضمان مطابقتها عند التنبؤ المستقبلي
    model.feature_names_ = list(X.columns)

    # حفظ النموذج على القرص الصلب لتجنب إعادة التدريب في كل مرة
    joblib.dump(model, MODEL_PATH)
    print(f"💾 تم حفظ النموذج بنجاح في: {MODEL_PATH}")
    return model


def predict_energy_anomaly(current_hour, zone, device_type, actual_watts):
    """استدعاء النموذج للتنبؤ بالاستهلاك ومقارنته بالاستهلاك الفعلي لكشف الانحرافات"""
    if not os.path.exists(MODEL_PATH):
        print("🤖 لا يوجد نموذج مدرب مسبقاً، جاري تشغيل التدريب الآن...")
        model = train_predictive_model()
        if model is None:
            return "NORMAL", 0.0  # في حال عدم وجود بيانات كافية
    else:
        model = joblib.load(MODEL_PATH)

    try:
        # تجهيز بيانات الإدخال للتنبؤ بها
        input_data = pd.DataFrame([{
            'hour': current_hour,
            f'zone_{zone}': 1,
            f'device_type_{device_type}': 1
        }])

        # مطابقة الأعمدة مع الأعمدة التي تدرب عليها النموذج لضمان عدم حدوث خطأ
        for col in model.feature_names_:
            if col not in input_data.columns:
                input_data[col] = 0
        input_data = input_data[model.feature_names_]

        # التنبؤ بالاستهلاك المتوقع (Expected/Normal Watts)
        predicted_watts = float(model.predict(input_data)[0])

        # حساب نسبة الانحراف
        deviation_ratio = actual_watts / predicted_watts if predicted_watts > 0 else 1.0

        print(
            f"🔮 التنبؤ الذكي لجهاز {device_type} في {zone}: المتوقع = {predicted_watts:.1f} واط | الفعلي = {actual_watts:.1f} واط")

        # كشف الشذوذ بناءً على الذكاء الاصطناعي (مثلاً إذا تجاوز المتوقع بـ 30%)
        if deviation_ratio > 1.30:
            return "CRITICAL_ANOMALY", predicted_watts
        elif deviation_ratio > 1.15:
            return "WARNING_ANOMALY", predicted_watts
        else:
            return "NORMAL", predicted_watts

    except Exception as e:
        print(f"⚠️ حدث خطأ أثناء التنبؤ بالنموذج: {e}")
        return "NORMAL", 0.0


if __name__ == "__main__":
    # تجربة تشغيل سريعة للنموذج
    train_predictive_model()
    # تجربة اختبار للتنبؤ لجهاز المكيف في الصالة عند الساعة 2 ظهراً باستهلاك فعلي مرتفع جداً
    status, pred = predict_energy_anomaly(current_hour=14, zone="LivingRoom", device_type="AirConditioner",
                                          actual_watts=4500.0)
    print(f"نتيجة الفحص الذكي: {status} (المتوقع كان: {pred:.2f} واط)")