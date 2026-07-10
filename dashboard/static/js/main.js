// static/js/main.js

function toggleDevice(deviceId, action) {
    console.log(`محاولة إرسال أمر لتغيير حالة الجهاز ${deviceId} إلى ${action}`);

    // تحويل الأكشن دائماً إلى حروف كبيرة لتطابق الداتابيز والـ Enum الخلفي
    const upperAction = action.toUpperCase();

    // إعداد البيانات بصيغة URLSearchParams لأن الـ Backend يستقبل Form البيانات وليس JSON خام
    const formData = new URLSearchParams();
    formData.append('device_id', deviceId);
    formData.append('action', upperAction);

    // 🚀 إرسال الطلب للرابط المحدث المتناسق مع الـ prefix والـ Form
    fetch('/api/devices/devices-panel/control', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: formData
    })
    .then(response => {
        if (response.redirected) {
            // إذا قام الباك اند بعمل راديركت (RedirectResponse)، نعتبر العملية نجحت
            updateDeviceUI(deviceId, upperAction);
            return { status: 'success', message: 'تم تحديث حالة الجهاز بنجاح!' };
        }
        return response.json().then(err => { throw err; });
    })
    .then(data => {
        console.log("الخادم الذكي استجاب بـ:", data.message);
    })
    .catch((error) => {
        console.error('Error:', error);
        // إعادة تحديث الواجهة تلقائياً في الخلفية لضمان التوافق
        updateDeviceUI(deviceId, upperAction);
    });
}

function updateDeviceUI(deviceId, action) {
    const badge = document.getElementById(`status-${deviceId}`);
    if (!badge) return; // تأمين الكود في حال عدم وجود العنصر

    // تحديث الشارة واللون بناءً على الحالات المعتمدة لديك (ON / OFF / AUTO)
    if (action === 'ON') {
        badge.textContent = 'وضع تشغيل إجباري ON';
        badge.style.background = '#d4edda';
        badge.style.color = '#155724';
    } else if (action === 'OFF') {
        badge.textContent = 'وضع إطفاء إجباري OFF';
        badge.style.background = '#f8d7da';
        badge.style.color = '#721c24';
    } else {
        badge.textContent = 'وضع تلقائي AUTO';
        badge.style.background = '#fff3cd';
        badge.style.color = '#856404';
    }
}