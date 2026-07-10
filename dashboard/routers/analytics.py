import os
import time
import jwt
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from config import settings

router = APIRouter()
templates = Jinja2Templates(directory="templates")
USD_TO_YER_RATE = 250.0

def get_db_connection():
    return psycopg2.connect(settings.DATABASE_URL, cursor_factory=RealDictCursor)

def get_violated_devices(cursor, conn):
    # جلب أجهزة البث الحي لآخر 10 دقائق من السبارك
    devices_summary = []
    try:
        query = """
            SELECT 
                s.device_type as device_name,
                MAX(t.max_price_limit_usd) as max_10min_limit,
                ROUND((AVG(s.avg_power_watts) * 0.05)::numeric, 2) as current_10min_cost
            FROM spark_windowed_energy s
            LEFT JOIN device_thresholds t ON 
                LOWER(s.device_type) LIKE '%' || LOWER(t.device_name) || '%'
                OR LOWER(t.device_name) LIKE '%' || LOWER(s.device_type) || '%'
            WHERE s.window_end >= (SELECT MAX(window_end) - INTERVAL '10 minutes' FROM spark_windowed_energy)
            GROUP BY s.device_type
            ORDER BY current_10min_cost DESC;
        """
        cursor.execute(query)
        devices_summary = cursor.fetchall()
    except Exception as e:
        print(f"⚠️ فشل استخراج تقرير السبارك: {e}")
        conn.rollback()
    return devices_summary

# 1. لوحة تحكم البث الحي الحالية (Real-Time Live Dashboard)
@router.get("/dashboard", response_class=HTMLResponse)
def get_dashboard_page(request: Request):
    conn = get_db_connection()
    cursor = conn.cursor()
    current_price = 0.0
    devices_list = []
    violated_devices = []
    user_prefs = None
    actual_spending_yer = 0.0
    is_budget_exceeded = False
    budget_status_msg = ""

    # أ) جلب التفضيلات العامة والميزانية باليمني
    try:
        cursor.execute("SELECT target_monthly_budget_usd, eco_mode_enabled FROM user_preferences WHERE id = 1;")
        user_prefs = cursor.fetchone()
        if user_prefs:
            target_budget_yer = float(user_prefs['target_monthly_budget_usd'])
    except Exception as e:
        print(f"⚠️ فشل جلب التفضيلات: {e}")
        conn.rollback()

    # ب) حساب ومقارنة الصرفية الفعلية للشهر الحالي من الـ DWH
    try:
        current_year_month = datetime.now().strftime("%Y-%m")
        query_spending = """
            SELECT COALESCE(SUM(estimated_cost_yer), 0) as total_spent
            FROM dwh_energy_analytics 
            WHERE TO_CHAR(report_date, 'YYYY-MM') = %s;
        """
        cursor.execute(query_spending, (current_year_month,))
        spending_row = cursor.fetchone()
        if spending_row:
            actual_spending_yer = float(spending_row['total_spent'])

        if actual_spending_yer > target_budget_yer:
            is_budget_exceeded = True
            diff_yer = actual_spending_yer - target_budget_yer
            budget_status_msg = f"🚨 تجاوزت الميزانية بمقدار {diff_yer:.2f} ريال! ينصح بتشغيل الـ Eco Mode."
        else:
            diff_yer = target_budget_yer - actual_spending_yer
            budget_status_msg = f"✅ ضمن الحدود المسموحة. المتبقي: {diff_yer:.2f} ريال"
    except Exception as e:
        print(f"⚠️ فشل مقارنة ميزانية الـ DWH: {e}")
        conn.rollback()

    # ج) جلب سعر الكهرباء الحالي بناءً على الساعة
    try:
        current_hour = time.localtime().tm_hour
        cursor.execute("SELECT price_per_kwh FROM electricity_prices WHERE hour = %s;", (current_hour,))
        price_row = cursor.fetchone()
        if price_row:
            current_price = price_row['price_per_kwh']
    except Exception as e:
        print(f"⚠️ فشل جلب سعر الكيلوواط: {e}")
        conn.rollback()

    # د) جلب قائمة وحالة الأجهزة الفورية
    try:
        cursor.execute("SELECT device_id, device_name, override_status, device_type FROM devices_status ORDER BY device_id;")
        devices_list = cursor.fetchall()
    except Exception as e:
        print(f"⚠️ فشل جلب الأجهزة: {e}")
        conn.rollback()

    # هـ) جلب خلاصة السبارك وبناء توكين ميتابيس للحماية
    try:
        violated_devices = get_violated_devices(cursor, conn)
    except Exception as e:
        print(f"⚠️ فشل جلب خلاصة السبارك: {e}")

    cursor.close()
    conn.close()

    try:
        payload = {
            "resource": {"dashboard": settings.METABASE_DASHBOARD_ID},
            "params": {},
            "exp": int(time.time()) + (60 * 15)
        }
        token = jwt.encode(payload, settings.METABASE_SECRET_KEY, algorithm="HS256")
        metabase_iframe_url = f"{settings.METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=false"
    except Exception as jwt_error:
        print(f"💥 خطأ توكين ميتابيس: {jwt_error}")
        metabase_iframe_url = ""

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "iframe_url": metabase_iframe_url,
            "target_budget": target_budget_yer,
            "actual_spending_yer": round(actual_spending_yer, 2),
            "is_budget_exceeded": is_budget_exceeded,
            "budget_status_msg": budget_status_msg,
            "eco_mode": "مفعّل" if (user_prefs and user_prefs['eco_mode_enabled']) else "معطّل",
            "current_price": current_price,
            "devices": devices_list,
            "violated_devices": violated_devices
        }
    )

# 2. مسار تقارير الأرشيف التاريخي المخزن في الكاش (Fast & Cached Layer)
@router.get("/historical-reports", response_class=HTMLResponse)
def historical_reports_page(request: Request):
    report_data = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT zone, device_type, overall_avg_watts, peak_power_watts, total_records_analyzed, last_updated 
            FROM historical_analytics_summary
            ORDER BY zone, overall_avg_watts DESC;
        """)
        rows = cursor.fetchall()

        if rows:
            total_records = sum(row['total_records_analyzed'] for row in rows)
            zones_dict = {}
            for row in rows:
                z = row['zone']
                if z not in zones_dict:
                    zones_dict[z] = {"zone_name": z, "avg_watts": row['overall_avg_watts'], "max_watts": row['peak_power_watts']}

            devices_list = [{"device_name": row['device_type'], "avg_watts": row['overall_avg_watts']} for row in rows]
            report_data = {
                "total_records": total_records,
                "zones": list(zones_dict.values()),
                "devices": devices_list,
                "last_updated": rows[0]['last_updated'].strftime('%Y-%m-%d %H:%M:%S') if rows[0]['last_updated'] else 'N/A'
            }
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ فشل جلب تحليلات الكاش: {e}")

    return templates.TemplateResponse(request=request, name="historical_reports.html", context={"report_data": report_data})

# 3. مسار تحليلات الـ Batch الملخصة من قاعدة البيانات (Batch View Layer)
@router.get("/dashboard/batch-analytics", response_class=HTMLResponse)
def get_batch_analytics_page(request: Request):
    analytics_list = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT zone, device_type, overall_avg_watts, peak_power_watts, total_records_analyzed, last_updated 
            FROM historical_analytics_summary
            ORDER BY zone, overall_avg_watts DESC;
        """)
        rows = cursor.fetchall()

        for row in rows:
            analytics_list.append({
                "zone": row['zone'],
                "device_type": row['device_type'],
                "overall_avg_watts": float(row['overall_avg_watts']),
                "peak_power_watts": float(row['peak_power_watts']),
                "total_records": row['total_records_analyzed'],
                "last_updated": row['last_updated'].strftime('%Y-%m-%d %H:%M:%S') if row['last_updated'] else 'N/A'
            })
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ فشل جلب جدول الـ Batch: {e}")

    return templates.TemplateResponse(request=request, name="batch_analytics.html", context={"analytics_data": analytics_list})