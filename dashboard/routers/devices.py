from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from config import settings

router = APIRouter(tags=["Devices Management"])
templates = Jinja2Templates(directory="templates")


def get_db_connection():
    return psycopg2.connect(settings.DATABASE_URL)


# 1. شاشة عرض الأجهزة والتحكم بها + نموذج إنشاء جهاز جديد
@router.get("/devices-panel", response_class=HTMLResponse)
async def devices_panel_page(request: Request):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT device_id, device_name, device_type, override_status FROM devices_status ORDER BY device_id;")
    all_devices = cur.fetchall()
    cur.close()
    conn.close()
    return templates.TemplateResponse(
        request=request,
        name="devices_panel.html",
        context={"all_devices": all_devices}
    )


# 2. مسار (API) للتحكم وتعديل حالة جهاز معين
@router.post("/devices-panel/control")
async def control_device(device_id: str = Form(...), action: str = Form(...)):
    if action not in ["AUTO", "ON", "OFF"]:
        raise HTTPException(status_code=400, detail="إجراء غير صالح")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE devices_status SET override_status = %s WHERE device_id = %s;", (action, device_id))
    conn.commit()
    cur.close()
    conn.close()
    return RedirectResponse(url="/devices-panel", status_code=303)


# 3. مسار (API) لإنشاء وإضافة جهاز جديد تماماً
@router.post("/devices-panel/add")
async def add_new_device(
        device_id: str = Form(...),
        device_name: str = Form(...),
        device_type: str = Form(...),
        initial_status: str = Form(...),
        base_watts: float = Form(500.0),
        critical: bool = Form(False)
):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO devices_status (device_id, device_name, device_type, override_status, base_watts, critical)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id) 
            DO UPDATE SET base_watts = EXCLUDED.base_watts, critical = EXCLUDED.critical;
        """, (device_id, device_name, device_type, initial_status, base_watts, critical))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error adding device: {e}")
    finally:
        cur.close()
        conn.close()
    return RedirectResponse(url="/devices-panel", status_code=303)