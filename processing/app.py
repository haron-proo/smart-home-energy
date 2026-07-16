# processing/app.py
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import subprocess
import os

app = FastAPI(title="SmartHome Notebook Executor API", version="1.0")

# المجلد الأساسي الذي يحتوي على ملفات الـ Notebooks داخل الحاوية
BASE_DIR = "/home/jovyan/work/processing"


class NotebookRequest(BaseModel):
    notebook_name: str  # مثل: ai_report_generator.ipynb


def run_notebook_task(notebook_name: str):
    """تقوم بتشغيل الـ Notebook في الخلفية دون تأخير استجابة إيرفلو"""
    input_path = os.path.join(BASE_DIR, notebook_name)
    output_path = os.path.join(BASE_DIR, f"output_{notebook_name}")

    if not os.path.exists(input_path):
        print(f"❌ الملف غير موجود في المسار: {input_path}")
        return

    print(f"🚀 [بدء التشغيل أوفلاين]: {notebook_name}...")
    try:
        # تشغيل papermill وتنفيذ النوت بوك وحفظ مخرجات جديدة
        result = subprocess.run([
            "papermill",
            input_path,
            output_path
        ], capture_output=True, text=True, check=True)
        print(f"✅ [نجاح]: تم الانتهاء من تشغيل {notebook_name} وحفظ المخرجات.")
    except subprocess.CalledProcessError as e:
        print(f"❌ [فشل]: فشل تشغيل الـ Notebook: {notebook_name}")
        print(f"الأخطاء البرمجية الناتجة:\n{e.stderr}")


@app.post("/run-notebook")
def trigger_notebook(payload: NotebookRequest, background_tasks: BackgroundTasks):
    input_path = os.path.join(BASE_DIR, payload.notebook_name)

    if not os.path.exists(input_path):
        raise HTTPException(
            status_code=404,
            detail=f"ملف الـ Notebook المطلوب غير متوفر: {payload.notebook_name}"
        )

    # وضع العملية في قائمة المهام الخلفية وإرجاع رد سريع لإيرفلو فوراً ليتجنب الـ Timeout
    background_tasks.add_task(run_notebook_task, payload.notebook_name)

    return {
        "status": "queued",
        "message": f"تمت الجدولة وبدء تشغيل '{payload.notebook_name}' بنجاح في الخلفية."
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)