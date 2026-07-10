-- 1. جدول المعالجة الفورية من السبارك (Streaming Table)
CREATE TABLE IF NOT EXISTS public.spark_windowed_energy (
    window_start TIMESTAMP WITHOUT TIME ZONE,
    window_end TIMESTAMP WITHOUT TIME ZONE,
    zone TEXT,
    device_type TEXT,
    avg_power_watts DOUBLE PRECISION,
    total_power_watts DOUBLE PRECISION,
    CONSTRAINT unique_zone_device_window UNIQUE (zone, device_type, window_end)
);

-- 2. جدول مستودع البيانات التراكمي (DWH Batch Table)
CREATE TABLE IF NOT EXISTS public.dwh_energy_analytics (
    report_date TIMESTAMP WITHOUT TIME ZONE,
    zone TEXT,
    device_type TEXT,
    is_room_occupied BOOLEAN,
    is_peak_hours BOOLEAN NOT NULL,
    raw_kwh_sum DOUBLE PRECISION,
    total_power_kwh DOUBLE PRECISION,
    estimated_cost_yer DOUBLE PRECISION,
    wasted_energy_kwh DOUBLE PRECISION,
    sensor_fault_count BIGINT,
    lost_signal_count BIGINT
);

-- 3. جدول ملخص التحليلات التاريخية (Cached Summary Table)
CREATE TABLE IF NOT EXISTS public.historical_analytics_summary (
    zone VARCHAR(100) NOT NULL,
    device_type VARCHAR(100) NOT NULL,
    overall_avg_watts NUMERIC(10,2),
    peak_power_watts NUMERIC(10,2),
    total_records_analyzed INTEGER,
    last_updated TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT historical_analytics_summary_pkey PRIMARY KEY (zone, device_type)
);

-- 4. جدول تقارير الفواتير الشهرية بالريال اليمني (Financial Report Table)
CREATE TABLE IF NOT EXISTS public.dwh_monthly_energy_reports (
    report_month VARCHAR(7) NOT NULL,
    total_kwh NUMERIC(12,2) NOT NULL,
    estimated_cost_yer NUMERIC(15,2) NOT NULL,
    last_updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dwh_monthly_energy_reports_pkey PRIMARY KEY (report_month)
);

-- 5. جدول تحليلات الأجهزة (Device Analytics Table)
CREATE TABLE IF NOT EXISTS public.pc_energy_analytics (
    report_date TIMESTAMP WITHOUT TIME ZONE,
    zone VARCHAR(100),
    device_type VARCHAR(100),
    average_wattage DOUBLE PRECISION,
    peak_wattage DOUBLE PRECISION,
    total_power_kwh DOUBLE PRECISION,
    estimated_cost_yer DOUBLE PRECISION,
    is_peak_hours BOOLEAN,
    total_readings INTEGER,
    extra_null_column VARCHAR(255),
    device_id VARCHAR(100)
);