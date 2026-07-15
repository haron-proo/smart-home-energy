import os
import sys
import subprocess

# 1. تحديد المسار المحلي المشترك على القرص الصلب لحفظ المكاتب أوفلاين
# (يقابل E:\IoT Project\storage\packages على جهازك الويندوز)
OFFLINE_PACKAGES_PATH = "/home/jovyan/work/storage/packages"

# 2. قائمة مكاتب التعلم الآلي والبيانات التي ترغب في توفيرها أوفلاين
REQUIRED_PACKAGES = [
    "numpy",
    "pandas",
    "scikit-learn",
    "scipy",
    "joblib"
]


def check_and_prepare_offline_packages():
    print("🔍 جاري التحقق من مستودع المكاتب أوفلاين...")

    # إنشاء المجلد إذا لم يكن موجوداً
    if not os.path.exists(OFFLINE_PACKAGES_PATH):
        os.makedirs(OFFLINE_PACKAGES_PATH)
        print(f"📁 تم إنشاء مجلد الحفظ أوفلاين: {OFFLINE_PACKAGES_PATH}")

    # التحقق مما إذا كان المجلد يحتوي على ملفات تثبيت (.whl أو .tar.gz)
    existing_files = [f for f in os.listdir(OFFLINE_PACKAGES_PATH) if f.endswith(('.whl', '.tar.gz'))]

    if not existing_files:
        print("🌐 المجلد فارغ! جاري الاتصال بالإنترنت لتحميل المكاتب وحفظها للعمل أوفلاين لاحقاً...")
        try:
            # أمر تحميل المكاتب كملفات wheel دون تثبيتها داخل المجلد المحدد
            download_cmd = [
                               sys.executable, "-m", "pip", "download",
                               "-d", OFFLINE_PACKAGES_PATH,
                           ] + REQUIRED_PACKAGES

            subprocess.check_call(download_cmd)
            print("✅ تم تحميل جميع حزم التعلم الآلي بنجاح وحفظها في القرص الصلب!")
        except Exception as e:
            print(f"❌ فشل تحميل المكاتب من الإنترنت. تأكد من اتصال الشبكة داخل الحاوية: {e}")
            return False
    else:
        print(f"📦 تم العثور على {len(existing_files)} ملف حزمة جاهز أوفلاين في القرص الصلب.")

    return True


def install_and_import_packages():
    # التحقق أولاً من المجلد وتحميل الحزم إذا لزم الأمر
    if not check_and_prepare_offline_packages():
        print("⚠️ سيتم المحاولة باستخدام المتاح حالياً.")

    # إعداد بيئة بايثون لتقرأ من هذا المجلد كأولوية قصوى
    if OFFLINE_PACKAGES_PATH not in sys.path:
        sys.path.insert(0, OFFLINE_PACKAGES_PATH)
        print("🔗 تم ربط مسار المكاتب المحلي بـ sys.path بنجاح.")

    print("⚡ جاري تثبيت/تحديث المكاتب في البيئة الحالية من المستودع المحلي (Offline Install)...")
    try:
        # التثبيت أوفلاين بالكامل بالاعتماد فقط على الملفات المجهزة بالقرص الصلب
        install_cmd = [
                          sys.executable, "-m", "pip", "install",
                          "--no-index",  # منع البحث في الإنترنت (مهم للعمل أوفلاين)
                          "--find-links", OFFLINE_PACKAGES_PATH,  # القراءة من مجلدنا المحلي
                      ] + REQUIRED_PACKAGES

        subprocess.check_call(install_cmd)
        print("🎉 [نجاح باهر] تم تثبيت مكاتب التعلم الآلي بنجاح أوفلاين!")

        # تجربة الاستدعاء الفعلي للتأكد من تفعيل المكاتب
        import numpy as np
        import pandas as pd
        import sklearn
        print(f"🧠 تم استدعاء المكاتب بنجاح! نسخة Scikit-Learn الحالية: {sklearn.__version__}")

    except Exception as e:
        print(f"❌ فشل عملية التثبيت أوفلاين: {e}")
        print("💡 تلميح: قد تحتاج لربط الحاوية بالإنترنت لمرة واحدة فقط لتنزيل الاعتمادات الناقصة.")


if __name__ == "__main__":
    install_and_import_packages()