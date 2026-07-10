from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from routers import devices, analytics, preferences
import uvicorn

app = FastAPI(
    title="منصة إدارة طاقة المنزل الذكي",
    description="نظام ذكي لمراقبة استهلاك الطاقة والتحكم في الأجهزة وتضمين تحليلات Metabase",
    version="1.0.0"
)

# ربط الملفات الثابتة (CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ربط المسارات النمطية (Routers)
app.include_router(devices.router, prefix="/api/devices", tags=["Devices Control"])
app.include_router(analytics.router, tags=["Analytics & Views"])
app.include_router(preferences.router)

@app.get("/")
def root():
    # التحويل المباشر إلى لوحة التحكم
    return RedirectResponse(url="/dashboard")

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8066, reload=True)