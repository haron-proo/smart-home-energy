import os

class Settings:
    # إعدادات ميتابيس (Metabase Settings)
    METABASE_SITE_URL: str = os.getenv("METABASE_SITE_URL", "http://localhost:3000")
    METABASE_SECRET_KEY: str = os.getenv("METABASE_SECRET_KEY", "4279e175712f97ed45c1fa58fc00ebf4399dc8ce097d3555b4cc0adc0942d9c6")
    METABASE_DASHBOARD_ID: int = int(os.getenv("METABASE_DASHBOARD_ID", 1))

    # إعدادات قاعدة البيانات (PostgreSQL)
    DATABASE_URL: str = "postgresql://smarthome_user:smarthome_password@127.0.0.1:5433/smarthome_energy"

settings = Settings()