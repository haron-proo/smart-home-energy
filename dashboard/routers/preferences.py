from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import List
import psycopg2
from config import settings

router = APIRouter(prefix="/preferences", tags=["Preferences"])
templates = Jinja2Templates(directory="templates")

def get_connection():
    return psycopg2.connect(settings.DATABASE_URL)

@router.get("/", response_class=HTMLResponse)
async def get_preferences(request: Request):
    conn = get_connection()
    cur = conn.cursor()

    # 1. جلب التفضيلات العامة للمستخدم
    cur.execute("SELECT target_monthly_budget_usd, eco_mode_enabled FROM user_preferences WHERE id = 1;")
    user_pref = cur.fetchone()

    # 2. جلب تفضيلات الأجهزة المحددة مسبقاً
    cur.execute("SELECT device_name, max_price_limit_usd FROM device_thresholds WHERE user_id = 1;")
    device_prefs = cur.fetchall()

    # 3. جلب قائمة الأجهزة الحقيقية المتوفرة في النظام
    cur.execute("SELECT DISTINCT device_type FROM devices_status ORDER BY device_type;")
    available_devices = cur.fetchall()

    cur.close()
    conn.close()
    return templates.TemplateResponse(
        request=request,
        name="preferences.html",
        context={
            "user_pref": user_pref,
            "device_prefs": device_prefs,
            "available_devices": available_devices
        }
    )

@router.post("/", response_class=RedirectResponse)
async def update_preferences(
    request: Request,
    target_monthly_budget_usd: float = Form(...),
    eco_mode_enabled: bool = Form(False),
    device_name: List[str] = Form([]),
    max_price_limit_usd: List[float] = Form([])
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE user_preferences 
            SET target_monthly_budget_usd = %s, eco_mode_enabled = %s 
            WHERE id = 1;
        """, (target_monthly_budget_usd, eco_mode_enabled))

        # مسح الحدود القديمة لتجنب التكرار
        cur.execute("DELETE FROM device_thresholds WHERE user_id = 1;")

        for name, limit in zip(device_name, max_price_limit_usd):
            if name.strip():
                cur.execute("""
                    INSERT INTO device_thresholds (user_id, device_name, max_price_limit_usd)
                    VALUES (1, %s, %s);
                """, (name, limit))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating preferences: {e}")
    finally:
        cur.close()
        conn.close()
    return RedirectResponse(url="/preferences/", status_code=303)