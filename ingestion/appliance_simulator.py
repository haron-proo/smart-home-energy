import os
import json
import time
import random
from datetime import datetime  # ⚡ تم اعتماد datetime المحلي الفعلي
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

BASE_DIR = r"E:\IoT Project"
DB_URL = "postgresql://smarthome_user:smarthome_password@localhost:5433/smarthome_energy"

CACHE_INTERVAL_SECONDS = 60
last_cache_update = 0
cached_data = {
    "devices": [],
    "eco_mode": False
}


# 1. دالة جلب وإدارة مصفوفة الأجهزة والوضع الاقتصادي عبر الـ Cache
def get_all_devices_from_db_cached():
    global last_cache_update, cached_data
    current_time = time.time()
    if current_time - last_cache_update > CACHE_INTERVAL_SECONDS or not cached_data["devices"]:
        try:
            conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
            cursor = conn.cursor()

            # أ) جلب قائمة الأجهزة وحالتها الفورية
            cursor.execute("SELECT device_id, device_type, override_status, base_watts, critical FROM devices_status;")
            cached_data["devices"] = cursor.fetchall()

            # ب) جلب حالة الوضع الاقتصادي العامة للمنزل
            cursor.execute("SELECT eco_mode_enabled FROM user_preferences WHERE id = 1;")
            pref_row = cursor.fetchone()
            cached_data["eco_mode"] = pref_row["eco_mode_enabled"] if pref_row else False

            cursor.close()
            conn.close()
            last_cache_update = current_time
            status_text = "مفعّل" if cached_data["eco_mode"] else "معطّل"
            print(
                f"🔄 [Cache Update] تم تحديث الأجهزة حياً (العدد: {len(cached_data['devices'])}) | الوضع الاقتصادي: {status_text}")
        except Exception as e:
            print(f"⚠️ فشل تحديث الـ Cache، سيتم استخدام البيانات المخزنة مسبقاً في الذاكرة: {e}")
    return cached_data


# 2. الاتصال بـ Kafka وبدء المحاكاة
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("🚀 تم تشغيل محرك محاكاة القصر الذكي المطور (اعتماد كامل على DB + Cache + Eco Mode)!")
except Exception as e:
    print(f"❌ فشل الاتصال بسيرفر كافكا محلياً: {e}")
    exit(1)

try:
    while True:
        # ⚡ إلغاء الـ UTC تماماً واعتماد التوقيت المحلي الفعلي للنظام مباشرة
        now = datetime.now()
        current_hour = now.hour

        current_cache = get_all_devices_from_db_cached()
        live_devices = current_cache["devices"]
        eco_mode_active = current_cache["eco_mode"]

        # تحديد الأنماط الزمنية للمنزل
        is_sleeping_hours = 23 <= current_hour or current_hour <= 6
        is_peak_heat_hours = 11 <= current_hour <= 16
        is_evening_rush = 17 <= current_hour <= 22

        if not live_devices:
            print("ℹ️ لا توجد أجهزة مسجلة في قاعدة البيانات حالياً، في انتظار الإضافة من شاشة FastAPI...")
            time.sleep(5)
            continue

        for device in live_devices:
            dev_id = device["device_id"]
            dev_type = device["device_type"]
            device_override = device["override_status"]
            is_critical = device["critical"]
            db_watts = device["base_watts"] if device["base_watts"] else 100.0

            # تحديد القدرة الكهربائية الأساسية
            if dev_type == "AC":
                base_watts = min(db_watts, 1200.0)
            elif dev_type == "Lighting":
                base_watts = min(db_watts, 15.0)
            elif dev_id == "GR_EV_Charger":
                base_watts = min(db_watts, 2000.0)
            elif dev_type == "Heavy_Appliance":
                base_watts = min(db_watts, 1000.0)
            else:
                base_watts = min(db_watts, 80.0)

            # استنتاج المنطقة (Zone) تلقائياً
            if dev_id.startswith("LV"):
                zone_name = "Living_Room"
                occupancy_weight = 0.8
            elif dev_id.startswith("KT"):
                zone_name = "Kitchen"
                occupancy_weight = 0.6
            elif dev_id.startswith("MB"):
                zone_name = "Master_Bedroom"
                occupancy_weight = 0.7
            elif dev_id.startswith("OD"):
                zone_name = "Outdoor_&_Security"
                occupancy_weight = 0.2
            else:
                zone_name = "Garage_&_Utility"
                occupancy_weight = 0.3

            # محاكاة وجود أشخاص في الغرف
            if is_sleeping_hours:
                room_occupied = True if zone_name == "Master_Bedroom" else (random.random() < 0.05)
            elif is_evening_rush:
                room_occupied = True if zone_name in ["Living_Room", "Kitchen"] else (random.random() < 0.3)
            else:
                room_occupied = random.random() < occupancy_weight

            # حساب حالة التشغيل والقدرة بناءً على التحكم والـ Eco Mode
            state = "OFF"
            power = 0.0

            if device_override == "OFF":
                state = "OFF"
                power = 0.0
            elif device_override == "ON":
                state = "ON"
                power = random.uniform(base_watts * 0.9, base_watts * 1.1)
            else:  # وضع الـ AUTO التلقائي الذكي
                if is_critical or dev_type == "Critical_Appliance":
                    state = "ON"
                    power = random.uniform(base_watts * 0.9, base_watts * 1.1)
                elif dev_type == "AC":
                    if zone_name == "Master_Bedroom" and is_sleeping_hours:
                        state = "ON"
                        factor = 0.5 if eco_mode_active else 0.7
                        power = random.uniform(base_watts * factor, base_watts * (factor + 0.1))
                    elif zone_name == "Living_Room" and is_peak_heat_hours:
                        state = "ON"
                        factor = 0.8 if eco_mode_active else 1.1
                        power = random.uniform(base_watts * (factor - 0.1), base_watts * factor)
                    elif room_occupied and not is_sleeping_hours:
                        state = "ON"
                        factor = 0.7 if eco_mode_active else 0.9
                        power = random.uniform(base_watts * factor, base_watts * (factor + 0.1))
                elif dev_type == "Lighting":
                    if eco_mode_active and zone_name == "Outdoor_&_Security":
                        state = "OFF"
                        power = 0.0
                    elif is_evening_rush or (room_occupied and not is_sleeping_hours):
                        state = "ON"
                        factor = 0.75 if eco_mode_active else 0.9
                        power = random.uniform(base_watts * factor, base_watts)
                    elif zone_name == "Outdoor_&_Security" and (current_hour >= 18 or current_hour <= 5):
                        state = "ON"
                        power = base_watts
                elif dev_id == "GR_EV_Charger":
                    if eco_mode_active and not is_sleeping_hours:
                        state = "OFF"
                        power = 0.0
                    elif is_sleeping_hours and random.random() < 0.4:
                        state = "ON"
                        power = random.uniform(base_watts * 0.8, base_watts * 1.0)
                elif room_occupied and dev_type in ["Heavy_Appliance", "Appliance", "Entertainment"]:
                    if eco_mode_active:
                        state = "OFF"
                        power = 0.0
                    elif random.random() < 0.2:
                        state = "ON"
                        power = random.uniform(base_watts * 0.7, base_watts * 1.1)

            # طبقة حقن مشاكل جودة البيانات
            if state != "OFF":
                dice_roll = random.random()
                if dice_roll < 0.01:
                    power = None
                    state = "LOST_SIGNAL"
                elif dice_roll < 0.015:
                    power = -50.0 if random.random() > 0.5 else 1500.0
                    state = "SENSOR_FAULT"
                else:
                    power = round(power, 2)
            else:
                power = 0.0

            # بناء الرسالة النهائية وضخها في كافكا بالتوقيت المحلي الفعلي المتطابق مع جهازك
            payload = {
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "house_type": "Smart_Luxury_Villa",
                "currency": "YER",
                "zone": zone_name,
                "device_id": dev_id,
                "device_type": dev_type,
                "is_room_occupied": room_occupied,
                "power_consumption_watts": power,
                "status": state
            }
            producer.send('energy_events', value=payload)

            if state in ["SENSOR_FAULT", "LOST_SIGNAL"]:
                print(f"⚠️ [DATA QUALITY ISSUE] {zone_name} -> {dev_id}: {power} ({state})")
            else:
                eco_prefix = "[ECO MODE ON]" if (eco_mode_active and state == "ON") else ""
                print(f"⚡ [Stream YER] {eco_prefix} {zone_name} -> {dev_id}: {power} W ({state})")

            if random.random() < 0.005 and power is not None and state != "OFF":
                producer.send('energy_events', value=payload)

        print(f"--- 🕒 تم بث نبضة القراءات المحدّثة بنجاح في الوقت الفعلي {now.strftime('%H:%M:%S')} ---")
        time.sleep(5)
except KeyboardInterrupt:
    print("\n🛑 تم إيقاف محرك المحاكاة.")
finally:
    producer.close()