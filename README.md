# راهنمای نصب و راه‌اندازی داشبورد سرویس لول
## اتصال به SQL Server — سرور داخلی شرکت

---

## ساختار پروژه

    iran-sl-dashboard/
    ├── backend/
    │   ├── main.py           ← سرویس Python (API)
    │   └── requirements.txt
    └── frontend/
        └── index.html        ← داشبورد HTML

---

## مرحله ۱ — پیش‌نیازها

روی سرور داخلی نصب باشد:
- Python 3.9+ 
- Microsoft ODBC Driver 17 for SQL Server
  (دانلود: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## مرحله ۲ — نصب کتابخانه‌های Python

```bash
cd backend
pip install -r requirements.txt
```

---

## مرحله ۳ — تنظیم اتصال به دیتابیس

فایل `backend/main.py` را باز کنید و این خط را ویرایش کنید:

**اگه Windows Authentication دارید (رایج‌ترین حالت):**
```python
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.1.10\\SQLEXPRESS;"   # ← آدرس IP سرور SQL
    "DATABASE=YourDatabaseName;"          # ← نام دیتابیس
    "Trusted_Connection=yes;"
)
```

**اگه Username/Password دارید:**
```python
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.1.10;"
    "DATABASE=YourDatabaseName;"
    "UID=sa;"
    "PWD=YourPassword;"
)
```

---

## مرحله ۴ — تطبیق Query با جداول شما

در `main.py` بخش `QUERY` را با نام جداول واقعی خودتان جایگزین کنید.

### حالت A — اگه جدول Provinces دارید:
جدول `dbo.Provinces` باید شامل ستون‌های زیر باشد:
```
ProvinceID, ProvinceName (فارسی), SLTarget, Latitude, Longitude
```

### حالت B — اگه فقط یه جدول aggregated دارید:
```python
QUERY = """
SELECT
    ProvinceName    AS province_fa,
    SLPercent       AS sl_percent,
    TargetSL        AS target_percent,
    VendorCount     AS vendor_count,
    TotalOrders     AS total_orders,
    Lat             AS lat,
    Lng             AS lng
FROM dbo.YourProvinceSLView
"""
```

### مختصات جغرافیایی (Lat/Lng) استان‌ها:
اگه در دیتابیس ندارید، این داده را به جدول Provinces اضافه کنید:

| استان | Latitude | Longitude |
|-------|----------|-----------|
| تهران | 35.7 | 51.4 |
| مازندران | 36.2 | 52.4 |
| اصفهان | 32.6 | 51.7 |
| خراسان رضوی | 36.3 | 59.5 |
| فارس | 29.1 | 53.0 |
| خوزستان | 31.3 | 48.7 |
| گیلان | 37.3 | 49.6 |
| البرز | 35.9 | 50.9 |
| آذربایجان شرقی | 37.9 | 46.3 |
| آذربایجان غربی | 37.5 | 44.9 |
| کرمان | 29.5 | 57.1 |
| سیستان و بلوچستان | 27.2 | 60.9 |
| لرستان | 33.5 | 48.4 |
| هرمزگان | 27.1 | 56.3 |
| همدان | 34.8 | 48.5 |
| اردبیل | 38.5 | 47.9 |
| سمنان | 35.6 | 54.4 |
| زنجان | 36.7 | 48.5 |
| مرکزی | 34.1 | 49.7 |
| یزد | 31.9 | 54.4 |
| بوشهر | 28.9 | 51.2 |
| گلستان | 37.3 | 55.2 |
| چهارمحال و بختیاری | 32.0 | 50.9 |
| خراسان شمالی | 37.5 | 57.5 |
| خراسان جنوبی | 33.0 | 59.2 |
| کهگیلویه و بویراحمد | 30.7 | 51.6 |
| ایلام | 33.6 | 46.4 |
| کردستان | 35.3 | 46.8 |
| کرمانشاه | 34.3 | 46.8 |
| قم | 34.6 | 50.9 |
| قزوین | 36.3 | 50.0 |

---

## مرحله ۵ — اجرای Backend

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

تست کنید در مرورگر:
```
http://localhost:8000/api/service-level
```
باید JSON با لیست استان‌ها ببینید.

---

## مرحله ۶ — باز کردن داشبورد

فایل `frontend/index.html` را مستقیم در مرورگر باز کنید.
(یا روی IIS / Nginx سرور داخلی قرار دهید)

---

## Auto-Refresh

داشبورد به صورت خودکار هر **۵ دقیقه** داده را از دیتابیس می‌گیرد.
برای تغییر بازه، در `index.html` این خط را ویرایش کنید:

```javascript
const REFRESH_MS = 5 * 60 * 1000;  // ۵ دقیقه
// برای ۱۰ دقیقه:  10 * 60 * 1000
// برای ۱ دقیقه:    1 * 60 * 1000
```

---

## اجرای Backend به صورت سرویس Windows

برای اینکه بعد از restart سرور هم اجرا بماند:

```bash
pip install pywin32
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

یا از **NSSM** (Non-Sucking Service Manager) استفاده کنید:
```
nssm install IranSLDashboard "C:\Python39\python.exe" "-m uvicorn main:app --host 0.0.0.0 --port 8000"
nssm start IranSLDashboard
```

---

## عیب‌یابی

| خطا | راه‌حل |
|-----|--------|
| `No module named 'pyodbc'` | `pip install pyodbc` |
| `Data source name not found` | ODBC Driver 17 نصب نیست |
| `Login failed for user` | user/pass یا Trusted Connection را بررسی کنید |
| `Connection timeout` | firewall پورت SQL Server را باز کنید (1433) |
| داشبورد خالی است | در Console مرورگر (F12) خطای CORS را بررسی کنید |



PS D:\D\iran-sl-dashboard\Comprehensive-Iran-Dashboard\backend> python -m uvicorn main:app --host 0.0.0.0 --port 8000