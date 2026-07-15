from kafka import KafkaConsumer
import json

# 1. إعداد المستهلك لقراءة البيانات اللحظية فقط
consumer = KafkaConsumer(
    'energy_events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    # 👇 القراءة من آخر مؤشر (Latest) لتجاهل القديم وقراءة الجديد فقط
    auto_offset_reset='latest'
)

print("📡 جهاز مراقبة كافكا اللحظي يعمل الآن... في انتظار بيانات جديدة من المحاكي...")

try:
    count = 0
    for message in consumer:
        data = message.value
        count += 1
        # طباعة منسقة وذكية للبيانات الجديدة المستلمة
        print(f"📥 [رسالة جديدة #{count}] {data['timestamp']} | المنطقة: {data['zone']} | الجهاز: {data['device_id']} | الطاقة: {data['power_consumption_watts']} واط | الحالة: {data['status']}")
except KeyboardInterrupt:
    print("\n🛑 تم إيقاف جهاز المراقبة اللحظي.")
finally:
    consumer.close()