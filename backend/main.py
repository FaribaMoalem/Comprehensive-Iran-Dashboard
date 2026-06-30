"""
Iran Province Service Level Dashboard — Backend API
پیش‌نیاز: pip install fastapi uvicorn pyodbc
اجرا: python -m uvicorn main:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Optional, List
import pyodbc
import logging
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─── Startup: pre-warm caches (background — server starts immediately) ─────────
def _warm_caches():
    try:
        logger.info("pre-warming caches (background)...")
        _refresh_loc_combos()
        _refresh_item_combos()
        _refresh_date_combos()
        service_level()
        logger.info("cache warm-up done")
    except Exception as e:
        logger.warning(f"warm-up failed (non-fatal): {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=_warm_caches, daemon=True).start()
    yield


app = FastAPI(title="Iran SL Dashboard API", lifespan=lifespan)

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
CACHE_TTL_COMBOS   = 30 * 60   # combo tables rarely change

# In-memory combo tables (loaded once, refreshed every 30 min)
# loc: [(province, city, district), ...]
# itm: [(l1, l2, l3, l4, l5, cd), ...]
# dt:  [(year, month, day), ...]
_loc_combos: list = []
_loc_combos_ts: float = 0.0
_itm_combos: list = []
_itm_combos_ts: float = 0.0
_date_combos: list = []
_date_combos_ts: float = 0.0


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


def run_rows(sql: str, params: tuple = ()) -> list:
    """Return raw list of tuples; returns [] on error."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return [tuple(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.warning(f"run_rows error: {e}")
        return []
    finally:
        conn.close()


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


# ─── Shared aggregation fragment (TRY_CAST avoids crash on 'Cancelled' rows) ───
_AGG = """
    ROUND(
        100.0 * SUM(TRY_CAST(f.IsOnTimeDeliverity AS INT))
        / NULLIF(SUM(CASE WHEN TRY_CAST(f.IsOnTimeDeliverity AS INT) IS NOT NULL THEN 1 ELSE 0 END), 0)
    , 1)                                            AS sl_percent,
    80.0                                            AS target_percent,
    COUNT(DISTINCT f.COM_DIM_VendorRef)             AS vendor_count,
    COUNT(f.ID)                                     AS total_orders"""

# Delivery date FK on fact table → COM.DIM_Date
_DATE_JOIN = "JOIN [COM].[DIM_Date] AS dt WITH (NOLOCK) ON dt.ID = f.COM_DIM_Date_DeliveryDateRef"

# Default Persian year range (applied when no specific year is selected)
DEFAULT_YEAR_FROM = 1399
DEFAULT_YEAR_TO   = 1405


# ─── Dynamic filter builder ────────────────────────────────────────────────────
def _tolist(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if x is not None and x != ""]
    return [v]


def _add_in(conditions: list, params: list, col: str, vals, cast=None):
    vals = _tolist(vals)
    if not vals:
        return
    if cast is not None:
        vals = [cast(v) for v in vals]
    if len(vals) == 1:
        conditions.append(f"{col} = ?")
        params.append(vals[0])
    else:
        conditions.append(f"{col} IN ({','.join('?' * len(vals))})")
        params.extend(vals)


def build_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    year=None, month=None, day=None,
):
    conditions: list[str] = []
    params:     list      = []
    need_item = any(_tolist(x) for x in [ig1, ig2, ig3, ig4, ig5, commerce_dept])

    _add_in(conditions, params, "loc.StateChart", province)
    _add_in(conditions, params, "loc.CityChart", city)
    _add_in(conditions, params, "ISNULL(NULLIF(loc.District, ''), N'نامشخص')", district)
    _add_in(conditions, params, "loc.BKInventLocationId", store_id)

    for col, val in [("Level1", ig1), ("Level2", ig2), ("Level3", ig3), ("Level4", ig4), ("Level5", ig5)]:
        _add_in(conditions, params, f"itm.{col}", val)
    _add_in(conditions, params, "itm.CommerceDepartment", commerce_dept)

    years = _tolist(year)
    if years:
        _add_in(conditions, params, "dt.PersianYearInt", years, cast=int)
    else:
        conditions.append("dt.PersianYearInt BETWEEN ? AND ?")
        params.extend([DEFAULT_YEAR_FROM, DEFAULT_YEAR_TO])

    _add_in(conditions, params, "dt.PersianMonthNo", month, cast=int)
    _add_in(conditions, params, "dt.PersianDayInMonth", day, cast=int)

    extra_joins = f"\n{_DATE_JOIN}"
    if need_item:
        extra_joins += "\nLEFT JOIN [COM].[DIM_Item] AS itm WITH (NOLOCK) ON itm.ID = f.COM_DIM_ItemRef"

    extra_where = ("AND " + " AND ".join(conditions)) if conditions else ""
    return extra_joins, extra_where, tuple(params)


def filter_kw(**kwargs) -> dict:
    out: dict = {}
    for k, v in kwargs.items():
        vals = _tolist(v)
        if vals:
            out[k] = ",".join(str(x) for x in sorted(vals, key=str))
    if not _tolist(kwargs.get("year")):
        out["yr_range"] = f"{DEFAULT_YEAR_FROM}-{DEFAULT_YEAR_TO}"
    return out


def filter_key(base: str, kw: dict) -> str:
    parts = sorted((k, v) for k, v in kw.items() if v)
    suffix = "|".join(f"{k}={v}" for k, v in parts)
    return f"{base}:{suffix}" if suffix else base


# ─── Combo-table cache (load once, Python-side cascade) ───────────────────────

def _refresh_loc_combos():
    """One DB query → all (province, city, district) combos. Cached 30 min."""
    global _loc_combos, _loc_combos_ts
    sql = """
SELECT DISTINCT
    loc.StateChart,
    loc.CityChart,
    ISNULL(NULLIF(loc.District, ''), N'نامشخص') AS dist
FROM [SCM].[Fact_VendorServiceLevel] AS f WITH (NOLOCK)
JOIN [COM].[DIM_InventLocation]      AS loc WITH (NOLOCK)
  ON loc.ID = f.COM_DIM_InventLocationRef
WHERE loc.StateChart IS NOT NULL AND loc.StateChart <> ''
  AND loc.CityChart  IS NOT NULL AND loc.CityChart  <> ''"""
    rows = run_rows(sql)
    if rows:
        _loc_combos    = [(str(r[0]), str(r[1]), str(r[2])) for r in rows]
        _loc_combos_ts = time.monotonic()
        logger.info(f"loc_combos loaded: {len(_loc_combos)} rows")


def _refresh_item_combos():
    """One DB query → all (Level1…5, CommerceDepartment) combos. Cached 30 min."""
    global _itm_combos, _itm_combos_ts
    sql = """
SELECT DISTINCT
    ISNULL(Level1, ''),  ISNULL(Level2, ''),  ISNULL(Level3, ''),
    ISNULL(Level4, ''),  ISNULL(Level5, ''),  ISNULL(CommerceDepartment, '')
FROM [COM].[DIM_Item] WITH (NOLOCK)
WHERE Level1 IS NOT NULL AND Level1 <> ''"""
    rows = run_rows(sql)
    if rows:
        _itm_combos    = [tuple(str(c) for c in r) for r in rows]
        _itm_combos_ts = time.monotonic()
        logger.info(f"itm_combos loaded: {len(_itm_combos)} rows")


def _refresh_date_combos():
    """One DB query → all (year, month, day) combos with fact data. Cached 30 min."""
    global _date_combos, _date_combos_ts
    sql = f"""
SELECT DISTINCT
    dt.PersianYearInt,
    dt.PersianMonthNo,
    dt.PersianDayInMonth
FROM (
    SELECT DISTINCT COM_DIM_Date_DeliveryDateRef AS date_ref
    FROM [SCM].[Fact_VendorServiceLevel] WITH (NOLOCK)
    WHERE COM_DIM_Date_DeliveryDateRef IS NOT NULL
) AS fd
JOIN [COM].[DIM_Date] AS dt WITH (NOLOCK) ON dt.ID = fd.date_ref
WHERE dt.PersianYearInt BETWEEN {DEFAULT_YEAR_FROM} AND {DEFAULT_YEAR_TO}"""
    rows = run_rows(sql)
    if rows:
        _date_combos    = [(int(r[0]), int(r[1]), int(r[2])) for r in rows]
        _date_combos_ts = time.monotonic()
        logger.info(f"date_combos loaded: {len(_date_combos)} rows")
    else:
        logger.warning("date_combos: no rows returned — check COM_DIM_Date_DeliveryDateRef join")


def _get_loc_combos() -> list:
    if time.monotonic() - _loc_combos_ts > CACHE_TTL_COMBOS:
        _refresh_loc_combos()
    return _loc_combos


def _get_itm_combos() -> list:
    if time.monotonic() - _itm_combos_ts > CACHE_TTL_COMBOS:
        _refresh_item_combos()
    return _itm_combos


def _get_date_combos() -> list:
    if time.monotonic() - _date_combos_ts > CACHE_TTL_COMBOS:
        _refresh_date_combos()
    return _date_combos


# ─── Filter options — Python-side cascade (no extra DB queries) ────────────────
@app.get("/api/filter-options")
def filter_options(
    province:      Optional[List[str]] = Query(None),
    city:          Optional[List[str]] = Query(None),
    district:      Optional[List[str]] = Query(None),
    ig1:           Optional[List[str]] = Query(None),
    ig2:           Optional[List[str]] = Query(None),
    ig3:           Optional[List[str]] = Query(None),
    ig4:           Optional[List[str]] = Query(None),
    ig5:           Optional[List[str]] = Query(None),
    commerce_dept: Optional[List[str]] = Query(None),
    year:          Optional[List[int]] = Query(None),
    month:         Optional[List[int]] = Query(None),
    day:           Optional[List[int]] = Query(None),
):
    locs  = _get_loc_combos()
    itms  = _get_itm_combos()
    dates = [r for r in _get_date_combos() if DEFAULT_YEAR_FROM <= r[0] <= DEFAULT_YEAR_TO]

    def _cascade(rows, idx, filters: dict) -> list:
        for pidx, pval in filters.items():
            vals = _tolist(pval)
            if vals:
                if isinstance(vals[0], str):
                    allowed = set(vals)
                else:
                    allowed = {int(v) for v in vals}
                rows = [r for r in rows if r[pidx] in allowed]
        return sorted({r[idx] for r in rows if r[idx] is not None and r[idx] != ""})

    provinces = _cascade(locs, 0, {})
    cities    = _cascade(locs, 1, {0: province})
    districts = _cascade(locs, 2, {0: province, 1: city})

    ig1_opts = _cascade(itms, 0, {})
    ig2_opts = _cascade(itms, 1, {0: ig1})
    ig3_opts = _cascade(itms, 2, {0: ig1, 1: ig2})
    ig4_opts = _cascade(itms, 3, {0: ig1, 1: ig2, 2: ig3})
    ig5_opts = _cascade(itms, 4, {0: ig1, 1: ig2, 2: ig3, 3: ig4})
    cd_opts  = _cascade(itms, 5, {0: ig1, 1: ig2, 2: ig3, 3: ig4, 4: ig5})

    years  = sorted(_cascade(dates, 0, {}), reverse=True)
    months = _cascade(dates, 1, {0: year})
    days   = _cascade(dates, 2, {0: year, 1: month})

    return {
        "provinces":      provinces,
        "cities":         cities,
        "districts":      districts,
        "ig1":            ig1_opts,
        "ig2":            ig2_opts,
        "ig3":            ig3_opts,
        "ig4":            ig4_opts,
        "ig5":            ig5_opts,
        "commerce_depts": cd_opts,
        "years":          years,
        "months":         months,
        "days":           days,
        "default_year_from": DEFAULT_YEAR_FROM,
        "default_year_to":   DEFAULT_YEAR_TO,
    }


# ─── Province ──────────────────────────────────────────────────────────────────
@app.get("/api/service-level")
def service_level(
    province:      Optional[List[str]] = Query(None),
    city:          Optional[List[str]] = Query(None),
    district:      Optional[List[str]] = Query(None),
    store_id:      Optional[List[str]] = Query(None),
    ig1:           Optional[List[str]] = Query(None),
    ig2:           Optional[List[str]] = Query(None),
    ig3:           Optional[List[str]] = Query(None),
    ig4:           Optional[List[str]] = Query(None),
    ig5:           Optional[List[str]] = Query(None),
    commerce_dept: Optional[List[str]] = Query(None),
    year:          Optional[List[int]] = Query(None),
    month:         Optional[List[int]] = Query(None),
    day:           Optional[List[int]] = Query(None),
):
    ej, ew, fp = build_filters(
        province, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        year, month, day,
    )
    sql = f"""
SELECT
    loc.StateChart                                  AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel] AS f   WITH (NOLOCK)
JOIN [COM].[DIM_InventLocation]      AS loc WITH (NOLOCK) ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart IS NOT NULL AND loc.StateChart <> ''
{ew}
GROUP BY loc.StateChart
ORDER BY sl_percent DESC;"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key("provinces", kw), sql, fp, CACHE_TTL_PROVINCE)
    return {"status": "ok", "level": "province", "data": data}


# ─── City ──────────────────────────────────────────────────────────────────────
@app.get("/api/service-level/cities")
def service_level_cities(
    province:      str,
    city:          Optional[List[str]] = Query(None),
    district:      Optional[List[str]] = Query(None),
    store_id:      Optional[List[str]] = Query(None),
    ig1:           Optional[List[str]] = Query(None),
    ig2:           Optional[List[str]] = Query(None),
    ig3:           Optional[List[str]] = Query(None),
    ig4:           Optional[List[str]] = Query(None),
    ig5:           Optional[List[str]] = Query(None),
    commerce_dept: Optional[List[str]] = Query(None),
    year:          Optional[List[int]] = Query(None),
    month:         Optional[List[int]] = Query(None),
    day:           Optional[List[int]] = Query(None),
):
    ej, ew, fp = build_filters(
        None, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        year, month, day,
    )
    sql = f"""
SELECT
    loc.CityChart                                   AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel] AS f   WITH (NOLOCK)
JOIN [COM].[DIM_InventLocation]      AS loc WITH (NOLOCK) ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ?
  AND loc.CityChart IS NOT NULL AND loc.CityChart <> ''
{ew}
GROUP BY loc.CityChart
ORDER BY sl_percent DESC;"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"cities:{province}", kw), sql, (province,)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "city", "data": data}


# ─── District ──────────────────────────────────────────────────────────────────
@app.get("/api/service-level/districts")
def service_level_districts(
    province:      str,
    city:          str,
    district:      Optional[List[str]] = Query(None),
    store_id:      Optional[List[str]] = Query(None),
    ig1:           Optional[List[str]] = Query(None),
    ig2:           Optional[List[str]] = Query(None),
    ig3:           Optional[List[str]] = Query(None),
    ig4:           Optional[List[str]] = Query(None),
    ig5:           Optional[List[str]] = Query(None),
    commerce_dept: Optional[List[str]] = Query(None),
    year:          Optional[List[int]] = Query(None),
    month:         Optional[List[int]] = Query(None),
    day:           Optional[List[int]] = Query(None),
):
    ej, ew, fp = build_filters(
        None, None, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        year, month, day,
    )
    sql = f"""
SELECT
    ISNULL(NULLIF(loc.District, ''), N'نامشخص')    AS name_fa,{_AGG},
    AVG(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    AVG(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel] AS f   WITH (NOLOCK)
JOIN [COM].[DIM_InventLocation]      AS loc WITH (NOLOCK) ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ? AND loc.CityChart = ?
{ew}
GROUP BY ISNULL(NULLIF(loc.District, ''), N'نامشخص')
ORDER BY sl_percent DESC;"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"districts:{province}:{city}", kw), sql, (province,city)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "district", "data": data}


# ─── Store ─────────────────────────────────────────────────────────────────────
@app.get("/api/service-level/stores")
def service_level_stores(
    province:      str,
    city:          str,
    district:      str,
    store_id:      Optional[List[str]] = Query(None),
    ig1:           Optional[List[str]] = Query(None),
    ig2:           Optional[List[str]] = Query(None),
    ig3:           Optional[List[str]] = Query(None),
    ig4:           Optional[List[str]] = Query(None),
    ig5:           Optional[List[str]] = Query(None),
    commerce_dept: Optional[List[str]] = Query(None),
    year:          Optional[List[int]] = Query(None),
    month:         Optional[List[int]] = Query(None),
    day:           Optional[List[int]] = Query(None),
):
    ej, ew, fp = build_filters(
        None, None, None, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        year, month, day,
    )
    sql = f"""
SELECT
    loc.BKInventLocationId                          AS store_id,
    loc.Name                                        AS name_fa,{_AGG},
    MAX(TRY_CAST(loc.Latitude  AS FLOAT))           AS lat,
    MAX(TRY_CAST(loc.Longitude AS FLOAT))           AS lng
FROM [SCM].[Fact_VendorServiceLevel] AS f   WITH (NOLOCK)
JOIN [COM].[DIM_InventLocation]      AS loc WITH (NOLOCK) ON loc.ID = f.COM_DIM_InventLocationRef
{ej}
WHERE loc.StateChart = ?
  AND loc.CityChart  = ?
  AND ISNULL(NULLIF(loc.District, ''), N'نامشخص') = ?
{ew}
GROUP BY loc.BKInventLocationId, loc.Name
ORDER BY sl_percent DESC;"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"stores:{province}:{city}:{district}", kw), sql, (province,city,district)+fp, CACHE_TTL_DETAIL)
    return {"status": "ok", "level": "store", "data": data}


# ─── Cache management ──────────────────────────────────────────────────────────
@app.delete("/api/cache")
def clear_cache():
    global _loc_combos, _itm_combos, _date_combos_ts, _loc_combos_ts, _itm_combos_ts
    _cache.clear()
    _loc_combos_ts = 0.0
    _itm_combos_ts = 0.0
    _date_combos_ts = 0.0
    return {"status": "ok", "message": "cache cleared"}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "loc_combos": len(_loc_combos),
        "itm_combos": len(_itm_combos),
        "date_combos": len(_date_combos),
        "cached_keys": len(_cache),
    }
