"""
Iran Province Service Level Dashboard — Backend API
پیش‌نیاز: pip install fastapi uvicorn pyodbc
اجرا: python -m uvicorn main:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import pyodbc
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Iran SL Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "DELETE"],
    allow_headers=["*"],
)

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=okdc34017\\node;"
    "DATABASE=OKDWH;"
    "Trusted_Connection=yes;"
)


# ─── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL_PROVINCE = 15 * 60
CACHE_TTL_DETAIL   =  5 * 60
CACHE_TTL_FILTERS  = 30 * 60   # filter options rarely change


def cached_query(key: str, sql: str, params: tuple = (), ttl: int = CACHE_TTL_PROVINCE):
    now = time.monotonic()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < ttl:
            logger.info(f"cache hit: {key}")
            return result
    result = run_query(sql, params)
    _cache[key] = (result, now)
    return result


def get_connection():
    try:
        return pyodbc.connect(CONN_STR, timeout=10)
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        raise HTTPException(status_code=503, detail="خطا در اتصال به دیتابیس")


def run_query(sql: str, params: tuple = ()):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        data = []
        for row in cursor.fetchall():
            r = dict(zip(columns, row))
            # Numeric coercions for SL fields
            if "sl_percent" in r:
                r["sl_percent"]     = float(r.get("sl_percent") or 0)
                r["target_percent"] = float(r.get("target_percent") or 80)
                r["vendor_count"]   = int(r.get("vendor_count") or 0)
                r["total_orders"]   = int(r.get("total_orders") or 0)
                r["lat"]            = float(r.get("lat") or 0)
                r["lng"]            = float(r.get("lng") or 0)
            data.append(r)
        return data
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def run_distinct(sql: str, params: tuple = ()) -> list:
    """Return a flat list of distinct string values; silently returns [] on error."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return [str(row[0]) for row in cursor.fetchall() if row[0] is not None and str(row[0]).strip()]
    except Exception as e:
        logger.warning(f"filter-options query failed: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


# ─── Shared aggregation fragment ───────────────────────────────────────────────
_AGG = """
    ROUND(
        100.0 * SUM(CAST(f.IsOnTimeDeliverity AS INT))
        / NULLIF(COUNT(f.ID), 0)
    , 1)                                            AS sl_percent,
    80.0                                            AS target_percent,
    COUNT(DISTINCT f.COM_DIM_VendorRef)             AS vendor_count,
    COUNT(f.ID)                                     AS total_orders"""


# ─── Dynamic filter builder ────────────────────────────────────────────────────
def build_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
):
    """
    Returns (extra_joins: str, extra_where: str, params: tuple).
    Column names that may need adjustment:
      DIM_Item  : ItemGroupLevel1 … ItemGroupLevel5
      DIM_Vendor: CommerceDepartment
      Fact ref  : COM_DIM_ItemRef
    """
    conditions: list[str] = []
    params:     list      = []

    # Join DIM_Item when any item-level filter is active
    need_item = any([ig1, ig2, ig3, ig4, ig5, commerce_dept])

    # Location
    if province:
        conditions.append("loc.StateChart = ?"); params.append(province)
    if city:
        conditions.append("loc.CityChart = ?");  params.append(city)
    if district:
        conditions.append("ISNULL(NULLIF(loc.District, ''), N'نامشخص') = ?"); params.append(district)
    if store_id:
        conditions.append("loc.BKInventLocationId = ?"); params.append(store_id)

    # Item group levels — columns: Level1…Level5 in COM.DIM_Item
    for col, val in [("Level1",ig1),("Level2",ig2),("Level3",ig3),("Level4",ig4),("Level5",ig5)]:
        if val:
            conditions.append(f"itm.{col} = ?"); params.append(val)

    # CommerceDepartment is in COM.DIM_Item
    if commerce_dept:
        conditions.append("itm.CommerceDepartment = ?"); params.append(commerce_dept)

    # Extra JOIN only when needed
    extra_joins = ""
    if need_item:
        extra_joins = "\nLEFT JOIN [COM].[DIM_Item] AS itm ON itm.ID = f.COM_DIM_ItemRef"

    extra_where = ("AND " + " AND ".join(conditions)) if conditions else ""
    return extra_joins, extra_where, tuple(params)


def filter_key(base: str, kw: dict) -> str:
    parts = sorted((k, v) for k, v in kw.items() if v)
    suffix = "|".join(f"{k}={v}" for k, v in parts)
    return f"{base}:{suffix}" if suffix else base


# ─── Filter options (cascaded) ────────────────────────────────────────────────
@app.get("/api/filter-options")
def filter_options(
    province:      Optional[str] = None,
    city:          Optional[str] = None,
    district:      Optional[str] = None,
    ig1:           Optional[str] = None,
    ig2:           Optional[str] = None,
    ig3:           Optional[str] = None,
    ig4:           Optional[str] = None,
    ig5:           Optional[str] = None,
    commerce_dept: Optional[str] = None,
):
    """
    Location options cascade through Fact+DIM_InventLocation.
    Item options cascade within DIM_Item directly (no Fact join needed).
    """

    # ── Location cascade (through Fact table) ──────────────────────────────
    _LOC_BASE = (
        "FROM [SCM].[Fact_VendorServiceLevel] f "
        "JOIN [COM].[DIM_InventLocation] loc ON loc.ID = f.COM_DIM_InventLocationRef "
    )
    LOC_FILTERS = {
        "province": ("loc.StateChart = ?",                             province),
        "city":     ("loc.CityChart = ?",                             city),
        "district": ("ISNULL(NULLIF(loc.District,''),N'نامشخص') = ?", district),
    }

    def loc_opts(col_expr: str, null_guard: str, exclude_key: str) -> list:
        conds, params = [null_guard], []
        for key, (cond, val) in LOC_FILTERS.items():
            if key != exclude_key and val:
                conds.append(cond); params.append(val)
        sql = f"SELECT DISTINCT {col_expr} AS v {_LOC_BASE} WHERE {' AND '.join(conds)} ORDER BY v"
        return run_distinct(sql, tuple(params))

    # ── Item cascade (directly from DIM_Item — no Fact join) ───────────────
    ITEM_COL = {
        "ig1":"Level1","ig2":"Level2","ig3":"Level3",
        "ig4":"Level4","ig5":"Level5","cd":"CommerceDepartment",
    }
    ITEM_VAL = {
        "ig1":ig1,"ig2":ig2,"ig3":ig3,"ig4":ig4,"ig5":ig5,"cd":commerce_dept,
    }

    def item_opts(col: str, exclude_key: str) -> list:
        conds  = [f"{col} IS NOT NULL", f"{col} <> ''"]
        params = []
        for key, val in ITEM_VAL.items():
            if key != exclude_key and val:
                conds.append(f"{ITEM_COL[key]} = ?"); params.append(val)
        sql = f"SELECT DISTINCT {col} AS v FROM [COM].[DIM_Item] WHERE {' AND '.join(conds)} ORDER BY v"
        return run_distinct(sql, tuple(params))

    return {
        "provinces":      loc_opts("loc.StateChart",
                                   "loc.StateChart IS NOT NULL AND loc.StateChart <> ''",
                                   "province"),
        "cities":         loc_opts("loc.CityChart",
                                   "loc.CityChart IS NOT NULL AND loc.CityChart <> ''",
                                   "city"),
        "districts":      loc_opts("ISNULL(NULLIF(loc.District,''),N'نامشخص')",
                                   "loc.StateChart IS NOT NULL",
                                   "district"),
        "ig1":            item_opts("Level1",            "ig1"),
        "ig2":            item_opts("Level2",            "ig2"),
        "ig3":            item_opts("Level3",            "ig3"),
        "ig4":            item_opts("Level4",            "ig4"),
        "ig5":            item_opts("Level5",            "ig5"),
        "commerce_depts": item_opts("CommerceDepartment","cd"),
    }


# ─── Province ──────────────────────────────────────────────────────────────────
@app.get("/api/service-level")
def service_level(
    province:      Optional[str] = None,
    city:          Optional[str] = None,
    district:      Optional[str] = None,
    store_id:      Optional[str] = None,
    ig1:           Optional[str] = None,
    ig2:           Optional[str] = None,
    ig3:           Optional[str] = None,
    ig4:           Optional[str] = None,
    ig5:           Optional[str] = None,
    commerce_dept: Optional[str] = None,
):
    ej, ew, fp = build_filters(province, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept)
    sql = f"""
SELECT
    loc.StateChart                                  AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel]    AS f
JOIN [COM].[DIM_InventLocation]         AS loc ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart IS NOT NULL AND loc.StateChart <> ''
{ew}
GROUP BY loc.StateChart
ORDER BY sl_percent DESC;"""
    kw = dict(province=province,city=city,district=district,store_id=store_id,
               ig1=ig1,ig2=ig2,ig3=ig3,ig4=ig4,ig5=ig5,cd=commerce_dept)
    data = cached_query(filter_key("provinces", kw), sql, fp, CACHE_TTL_PROVINCE)
    return {"status": "ok", "level": "province", "data": data}


# ─── City ──────────────────────────────────────────────────────────────────────
@app.get("/api/service-level/cities")
def service_level_cities(
    province:      str,
    city:          Optional[str] = None,
    district:      Optional[str] = None,
    store_id:      Optional[str] = None,
    ig1:           Optional[str] = None,
    ig2:           Optional[str] = None,
    ig3:           Optional[str] = None,
    ig4:           Optional[str] = None,
    ig5:           Optional[str] = None,
    commerce_dept: Optional[str] = None,
):
    ej, ew, fp = build_filters(None, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept)
    sql = f"""
SELECT
    loc.CityChart                                   AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel]    AS f
JOIN [COM].[DIM_InventLocation]         AS loc ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ?
  AND loc.CityChart IS NOT NULL AND loc.CityChart <> ''
{ew}
GROUP BY loc.CityChart
ORDER BY sl_percent DESC;"""
    kw = dict(province=province,city=city,district=district,store_id=store_id,
               ig1=ig1,ig2=ig2,ig3=ig3,ig4=ig4,ig5=ig5,cd=commerce_dept)
    data = cached_query(filter_key(f"cities:{province}", kw), sql, (province,)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "city", "data": data}


# ─── District ──────────────────────────────────────────────────────────────────
@app.get("/api/service-level/districts")
def service_level_districts(
    province:      str,
    city:          str,
    district:      Optional[str] = None,
    store_id:      Optional[str] = None,
    ig1:           Optional[str] = None,
    ig2:           Optional[str] = None,
    ig3:           Optional[str] = None,
    ig4:           Optional[str] = None,
    ig5:           Optional[str] = None,
    commerce_dept: Optional[str] = None,
):
    ej, ew, fp = build_filters(None, None, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept)
    sql = f"""
SELECT
    ISNULL(NULLIF(loc.District, ''), N'نامشخص')    AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel]    AS f
JOIN [COM].[DIM_InventLocation]         AS loc ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ? AND loc.CityChart = ?
{ew}
GROUP BY ISNULL(NULLIF(loc.District, ''), N'نامشخص')
ORDER BY sl_percent DESC;"""
    kw = dict(province=province,city=city,district=district,store_id=store_id,
               ig1=ig1,ig2=ig2,ig3=ig3,ig4=ig4,ig5=ig5,cd=commerce_dept)
    data = cached_query(filter_key(f"districts:{province}:{city}", kw), sql, (province,city)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "district", "data": data}


# ─── Store ─────────────────────────────────────────────────────────────────────
@app.get("/api/service-level/stores")
def service_level_stores(
    province:      str,
    city:          str,
    district:      str,
    store_id:      Optional[str] = None,
    ig1:           Optional[str] = None,
    ig2:           Optional[str] = None,
    ig3:           Optional[str] = None,
    ig4:           Optional[str] = None,
    ig5:           Optional[str] = None,
    commerce_dept: Optional[str] = None,
):
    ej, ew, fp = build_filters(None, None, None, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept)
    sql = f"""
SELECT
    loc.BKInventLocationId                          AS store_id,
    loc.Name                                        AS name_fa,{_AGG},
    MAX(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    MAX(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel]    AS f
JOIN [COM].[DIM_InventLocation]         AS loc ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ?
  AND loc.CityChart  = ?
  AND ISNULL(NULLIF(loc.District, ''), N'نامشخص') = ?
{ew}
GROUP BY loc.BKInventLocationId, loc.Name
ORDER BY sl_percent DESC;"""
    kw = dict(province=province,city=city,district=district,store_id=store_id,
               ig1=ig1,ig2=ig2,ig3=ig3,ig4=ig4,ig5=ig5,cd=commerce_dept)
    data = cached_query(filter_key(f"stores:{province}:{city}:{district}", kw), sql, (province,city,district)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "store", "data": data}


# ─── Cache management ──────────────────────────────────────────────────────────
@app.delete("/api/cache")
def clear_cache():
    _cache.clear()
    return {"status": "ok", "message": "cache cleared"}


@app.get("/health")
def health():
    return {"status": "ok"}
