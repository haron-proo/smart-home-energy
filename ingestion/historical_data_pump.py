import os
import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = "postgresql://smarthome_user:smarthome_password@localhost:5433/smarthome_energy"


# 1. جلب بيانات الأجهزة والوضع الاقتصادي لمرة واحدة من قاعدة البيانات لتسريع العملية
def get_devices_and_preferences():
    try:
        conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()

        cursor.execute("SELECT device_id, device_type, override_status, base_watts, critical FROM devices_status;")
        devices = cursor.fetchall()

        cursor.execute("SELECT eco_mode_enabled FROM user_preferences WHERE id = 1;")
        pref_row = cursor.fetchone()
        eco_mode = pref_row["eco_mode_enabled"] if pref_row else False

        cursor.close()
        conn.close()
        return devices, eco_mode
    except Exception as e:
        print(f"❌ فشل الاتصال بقاعدة البيانات لجلب الأجهزة: {e}")
        exit(1)


# 2. الاتصال بـ Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # إعدادات لتحسين سرعة الإرسال وضخ كميات ضخمة (Batching)
        linger_ms=10,
        batch_size=16384 * 4
    )
    print("🚀 تم تشغيل مضخة البيانات التاريخية لـ 3 أشهر إلى كافكا...")
except Exception as e:
    print(f"❌ فشل الاتصال بسيرفر كافكا محلياً: {e}")
    exit(1)

# 3. إعداد النطاق الزمني (3 أشهر ماضية)
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=90)

# خطوة التقدم الزمني (كل 10 دقائق لتوليد حجم بيانات منطقي وسريع)
time_step = timedelta(minutes=10)

live_devices, eco_mode_active = get_devices_and_preferences()
if not live_devices:
    print("⚠️ لا توجد أجهزة في قاعدة البيانات لبدء عملية التوليد!")
    exit(1)

print(f"⏳ جاري بدء حقن البيانات من تاريخ: {start_time} إلى {end_time}")
current_sim_time = start_time
total_messages_sent = 0

try:
    while current_sim_time <= end_time:
        # حساب الساعة المحلية للمحاكاة (توقيت مكة/اليمن UTC +3)
        current_hour = (current_sim_time.hour + 3) % 24

        # تحديد الأنماط الزمنية التاريخية للرحلة
        is_sleeping_hours = 23 <= current_hour or current_hour <= 6
        is_peak_heat_hours = 11 <= current_hour <= 16
        is_evening_rush = 17 <= current_hour <= 22

        for device in live_devices:
            dev_id = device["device_id"]
            dev_type = device["device_type"]
            device_override = device["override_status"]
            is_critical = device["critical"]
            db_watts = device["base_watts"] if device["base_watts"] else 100.0

            # تحديد القدرة الأساسية
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

            # تحديد المنطقة (Zone)
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

            # محاكاة الوجود البشري التاريخي
            if is_sleeping_hours:
                room_occupied = True if zone_name == "Master_Bedroom" else (random.random() < 0.05)
            elif is_evening_rush:
                room_occupied = True if zone_name in ["Living_Room", "Kitchen"] else (random.random() < 0.3)
            else:
                room_occupied = random.random() < occupancy_weight

            # حساب القدرة والتشغيل
            state = "OFF"
            power = 0.0

            if device_override == "OFF":
                state = "OFF"
                power = 0.0
            elif device_override == "ON":
                state = "ON"
                power = random.uniform(base_watts * 0.9, base_watts * 1.1)
            else:  # AUTO
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

            # جودة البيانات
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

            # بناء الرسالة بالتوقيت التاريخي التدريجي
            payload = {
                "timestamp": current_sim_time.strftime("%Y-%m-%d %H:%M:%S"),
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
            total_messages_sent += 1

        # طباعة تقدم العملية كل يوم كامل من المحاكاة لمنع ازدحام المخرجات
        if current_sim_time.hour == 0 and current_sim_time.minute == 0:
            print(
                f"📅 تم إرسال بيانات اليوم التاريخي: {current_sim_time.strftime('%Y-%m-%d')} | إجمالي الرسائل: {total_messages_sent}")
            # تفريغ الذاكرة المؤقتة وضمان وصول البيانات لكافكا بشكل فوري
            producer.flush()

        # الانتقال للخطوة التاريخية القادمة
        current_sim_time += time_step

    print(f"🎉 [نجاح ساحق] تم إرسال {total_messages_sent} قراءة تاريخية متكاملة لـ 3 أشهر ماضية إلى كافكا بنجاح!")
except KeyboardInterrupt:
    print("\n🛑 تم إيقاف عملية الحقن.")
finally:
    producer.close()