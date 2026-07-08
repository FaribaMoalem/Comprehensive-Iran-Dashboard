"""
Iran Province Service Level Dashboard — Backend API
پیش‌نیاز: pip install fastapi uvicorn clickhouse-connect
اجرا: python -m uvicorn main:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Optional, List
import clickhouse_connect
import logging
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse tables
T_FACT = "ChatBotDB.Fact_VendorServiceLevel"
T_LOC  = "COM.DIM_InventLocation"
T_ITEM = "COM.DIM_Item"
T_DATE = "COM.DIM_Date"

_DIST = "ifNull(nullIf(loc.District, ''), 'نامشخص')"
_ON_TIME = "if(f.receive_date_id > 0 AND f.receive_date_id <= f.delivery_date_id, 1, 0)"


# ─── Startup: pre-warm caches (background — server starts immediately) ─────────
def _warm_caches():
    try:
        logger.info("pre-warming caches (background)...")
        _refresh_loc_combos()
        _refresh_item_combos()
        _refresh_date_combos()
        service_level(
            province=None, city=None, district=None, store_id=None,
            ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
            commerce_dept=None, year=None, month=None, day=None,
        )
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


def get_client():
    try:
        return clickhouse_connect.get_client(
            host="10.192.31.65",
            port=8123,
            username="default",
            password="Aliz@123",
            secure=False,
            compress=False,
        )
    except Exception as e:
        logger.error(f"ClickHouse connection error: {e}")
        raise HTTPException(status_code=503, detail="خطا در اتصال به دیتابیس")


# ─── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL_PROVINCE = 15 * 60
CACHE_TTL_DETAIL   =  5 * 60
CACHE_TTL_COMBOS   = 30 * 60

_loc_combos: list = []
_loc_combos_ts: float = 0.0
_itm_combos: list = []
_itm_combos_ts: float = 0.0
_date_combos: list = []
_date_combos_ts: float = 0.0


def run_query(sql: str, params: dict | None = None):
    client = get_client()
    try:
        result = client.query(sql, parameters=params or {})
        columns = result.column_names
        data = []
        for row in result.result_rows:
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


def run_rows(sql: str, params: dict | None = None) -> list:
    try:
        result = get_client().query(sql, parameters=params or {})
        return [tuple(row) for row in result.result_rows]
    except Exception as e:
        logger.warning(f"run_rows error: {e}")
        return []


def cached_query(key: str, sql: str, params: dict | None = None, ttl: int = CACHE_TTL_PROVINCE):
    now = time.monotonic()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < ttl:
            logger.info(f"cache hit: {key}")
            return result
    result = run_query(sql, params)
    _cache[key] = (result, now)
    return result


# ─── Shared aggregation ────────────────────────────────────────────────────────
_AGG = f"""
    round(
        100.0 * sum({_ON_TIME})
        / nullIf(sum(if({_ON_TIME} IS NOT NULL, 1, 0)), 0),
    1)                                              AS sl_percent,
    80.0                                            AS target_percent,
    uniqExact(f.vendor_id)                          AS vendor_count,
    count()                                         AS total_orders"""

_DATE_JOIN = f"JOIN {T_DATE} AS dt ON dt.ID = f.delivery_date_id"

DEFAULT_YEAR_FROM = 1399
DEFAULT_YEAR_TO   = 1405


# ─── Dynamic filter builder ────────────────────────────────────────────────────
def _tolist(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if x is not None and x != ""]
    return [v]


def _add_in(conditions: list, params: dict, col: str, vals, typ: str = "String"):
    vals = _tolist(vals)
    if not vals:
        return
    key = f"f_{len(params)}"
    if typ == "Int32":
        vals = [int(v) for v in vals]
    if len(vals) == 1:
        params[key] = vals[0]
        conditions.append(f"{col} = {{{key}:{typ}}}")
    else:
        params[key] = vals
        conditions.append(f"{col} IN {{{key}:Array({typ})}}")


def build_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    year=None, month=None, day=None,
):
    conditions: list[str] = []
    params: dict = {}
    need_item = any(_tolist(x) for x in [ig1, ig2, ig3, ig4, ig5, commerce_dept])

    _add_in(conditions, params, "loc.StateChart", province)
    _add_in(conditions, params, "loc.CityChart", city)
    _add_in(conditions, params, _DIST, district)
    _add_in(conditions, params, "loc.BKInventLocationId", store_id)

    for col, val in [("Level1", ig1), ("Level2", ig2), ("Level3", ig3), ("Level4", ig4), ("Level5", ig5)]:
        _add_in(conditions, params, f"itm.{col}", val)
    _add_in(conditions, params, "itm.CommerceDepartment", commerce_dept)

    years = _tolist(year)
    if years:
        _add_in(conditions, params, "dt.PersianYearInt", years, typ="Int32")
    else:
        params["yr_from"] = DEFAULT_YEAR_FROM
        params["yr_to"]   = DEFAULT_YEAR_TO
        conditions.append("dt.PersianYearInt BETWEEN {yr_from:Int32} AND {yr_to:Int32}")

    _add_in(conditions, params, "dt.PersianMonthNo", month, typ="Int32")
    _add_in(conditions, params, "dt.PersianDayInMonth", day, typ="Int32")

    extra_joins = f"\n{_DATE_JOIN}"
    if need_item:
        extra_joins += f"\nLEFT JOIN {T_ITEM} AS itm ON itm.ID = f.item_id"

    extra_where = ("AND " + " AND ".join(conditions)) if conditions else ""
    return extra_joins, extra_where, params


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


def _merge_params(*dicts) -> dict:
    merged: dict = {}
    for d in dicts:
        merged.update(d)
    return merged


# ─── Combo-table cache ─────────────────────────────────────────────────────────

def _refresh_loc_combos():
    global _loc_combos, _loc_combos_ts
    sql = f"""
SELECT DISTINCT
    loc.StateChart,
    loc.CityChart,
    {_DIST} AS dist
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
WHERE loc.StateChart != ''
  AND loc.CityChart  != ''"""
    rows = run_rows(sql)
    if rows:
        _loc_combos    = [(str(r[0]), str(r[1]), str(r[2])) for r in rows]
        _loc_combos_ts = time.monotonic()
        logger.info(f"loc_combos loaded: {len(_loc_combos)} rows")


def _refresh_item_combos():
    global _itm_combos, _itm_combos_ts
    sql = f"""
SELECT DISTINCT
    ifNull(Level1, ''), ifNull(Level2, ''), ifNull(Level3, ''),
    ifNull(Level4, ''), ifNull(Level5, ''), ifNull(CommerceDepartment, '')
FROM {T_ITEM}
WHERE Level1 != ''"""
    rows = run_rows(sql)
    if rows:
        _itm_combos    = [tuple(str(c) for c in r) for r in rows]
        _itm_combos_ts = time.monotonic()
        logger.info(f"itm_combos loaded: {len(_itm_combos)} rows")


def _refresh_date_combos():
    global _date_combos, _date_combos_ts
    sql = f"""
SELECT DISTINCT
    dt.PersianYearInt,
    dt.PersianMonthNo,
    dt.PersianDayInMonth
FROM (
    SELECT DISTINCT delivery_date_id AS date_ref
    FROM {T_FACT}
    WHERE delivery_date_id IS NOT NULL
) AS fd
JOIN {T_DATE} AS dt ON dt.ID = fd.date_ref
WHERE dt.PersianYearInt BETWEEN {{yr_from:Int32}} AND {{yr_to:Int32}}"""
    rows = run_rows(sql, {"yr_from": DEFAULT_YEAR_FROM, "yr_to": DEFAULT_YEAR_TO})
    if rows:
        _date_combos    = [(int(r[0]), int(r[1]), int(r[2])) for r in rows]
        _date_combos_ts = time.monotonic()
        logger.info(f"date_combos loaded: {len(_date_combos)} rows")
    else:
        logger.warning("date_combos: no rows returned")


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


# ─── Filter options ────────────────────────────────────────────────────────────
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
    avg(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    avg(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
{ej}
WHERE loc.StateChart != ''
{ew}
GROUP BY loc.StateChart
ORDER BY sl_percent DESC"""
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
    params = _merge_params(fp, {"drill_province": province})
    sql = f"""
SELECT
    loc.CityChart                                   AS name_fa,{_AGG},
    avg(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    avg(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
{ej}
WHERE loc.StateChart = {{drill_province:String}}
  AND loc.CityChart != ''
{ew}
GROUP BY loc.CityChart
ORDER BY sl_percent DESC"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"cities:{province}", kw), sql, params, CACHE_TTL_DETAIL)
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
    params = _merge_params(fp, {"drill_province": province, "drill_city": city})
    sql = f"""
SELECT
    {_DIST}                                         AS name_fa,{_AGG},
    avg(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    avg(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
{ej}
WHERE loc.StateChart = {{drill_province:String}}
  AND loc.CityChart  = {{drill_city:String}}
{ew}
GROUP BY {_DIST}
ORDER BY sl_percent DESC"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"districts:{province}:{city}", kw), sql, params, CACHE_TTL_DETAIL)
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
    params = _merge_params(fp, {
        "drill_province": province,
        "drill_city": city,
        "drill_district": district,
    })
    sql = f"""
SELECT
    loc.BKInventLocationId                          AS store_id,
    loc.Name                                        AS name_fa,{_AGG},
    max(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    max(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
{ej}
WHERE loc.StateChart = {{drill_province:String}}
  AND loc.CityChart  = {{drill_city:String}}
  AND {_DIST}        = {{drill_district:String}}
{ew}
GROUP BY loc.BKInventLocationId, loc.Name
ORDER BY sl_percent DESC"""
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
        year=year, month=month, day=day,
    )
    data = cached_query(filter_key(f"stores:{province}:{city}:{district}", kw), sql, params, CACHE_TTL_DETAIL)
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
        "database": "clickhouse",
        "loc_combos": len(_loc_combos),
        "itm_combos": len(_itm_combos),
        "date_combos": len(_date_combos),
        "cached_keys": len(_cache),
    }
