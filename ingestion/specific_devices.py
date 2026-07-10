import json
import time
import threading
import psutil
from datetime import datetime
from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

app = Flask(__name__)

# إعداد منتج كافكا (Kafka Producer)
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("🚀 [IoT Protocol Engine] كافكا متصل وجاهز لتوجيه بروتوكولات IoT...")
except Exception as e:
    print(f"❌ فشل كافكا: {e}")
    exit(1)

# 🌐 الجزء أ: استقبال البيانات عبر بروتوكول HTTP API
@app.route('/api/v1/device', methods=['POST'])
def http_api_endpoint():
    try:
        data = request.get_json()
        producer.send('energy_events_specific', value=data)
        print(f"🌐 [HTTP API] استقبال قراءة من الجهاز {data.get('asset_tag')} عبر الشبكة")
        return jsonify({"status": "routed_to_kafka"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_http_server():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# 📡 الجزء ب: استقبال البيانات عبر بروتوكول MQTT Broker
def on_mqtt_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode('utf-8'))
        producer.send('energy_events_specific', value=payload)
        print(f"📡 [MQTT Broker] تم سحب قراءة حية من موضوع: {message.topic}")
    except Exception as e:
        print(f"⚠️ [MQTT Error] خطأ في معالجة رسالة الـ Broker: {e}")

def run_mqtt_client():
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_message = on_mqtt_message
    try:
        mqtt_client.connect("broker.hivemq.com", 1883, 60)
        mqtt_client.subscribe("yemen/smart_home/specific_devices")
        print("🔌 [MQTT Broker] متصل بنجاح بـ broker.hivemq.com ومستمع للحساسات حياً...")
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"❌ فشل اتصال MQTT Broker: {e}")

# 💻 الجزء ج: محاكاة إرسال حزمة بيانات الـ PC عبر بروتوكول HTTP
def run_my_pc_protocol_sender():
    import requests
    time.sleep(3)
    print("🖥️ [PC Node] بدأ بإرسال بيانات الـ Hardware عبر بروتوكول HTTP API...")
    while True:
        cpu_usage = psutil.cpu_percent(interval=1)
        pc_watts = round(35.0 + (150.0 * (cpu_usage / 100.0)), 2)
        payload = {
            "asset_tag": "Specific_Asset_PC",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "measured_watts": pc_watts,
            "health_index": "ONLINE",
            "transport_protocol": "HTTP_REST"
        }
        try:
            requests.post("http://localhost:5000/api/v1/device", json=payload, timeout=2)
        except:
            pass
        time.sleep(6)

# 🏁 تشغيل المحركات الثلاثة بالتوازي (Multi-Threading)
if __name__ == '__main__':
    threading.Thread(target=run_http_server, daemon=True).start()
    threading.Thread(target=run_mqtt_client, daemon=True).start()
    threading.Thread(target=run_my_pc_protocol_sender, daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 تم إيقاف محرك البروتوكولات المطور.")