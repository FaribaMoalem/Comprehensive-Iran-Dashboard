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
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse tables
T_FACT = "ChatBotDB.Fact_VendorServiceLevel"
T_SALES = "RTL.Fact_SalesTrans"
T_LOC  = "COM.DIM_InventLocation"
T_ITEM = "COM.DIM_Item"
T_DATE = "COM.DIM_Date"

_DIST = "ifNull(nullIf(loc.District, ''), 'نامشخص')"
_ON_TIME = "if(f.receive_date_id > 0 AND f.receive_date_id <= f.delivery_date_id, 1, 0)"
SL_TARGET = 80.0
SL_LOW_THRESHOLD = 30.0
SALES_DROP_PCT = -5.0


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
            send_receive_timeout=1200,
            autogenerate_session_id=True,
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
            if "net_amount" in r:
                r["net_amount"] = float(r.get("net_amount") or 0)
            if "sl_prev" in r and r["sl_prev"] is not None:
                r["sl_prev"] = float(r["sl_prev"])
            if "net_amount_prev" in r and r["net_amount_prev"] is not None:
                r["net_amount_prev"] = float(r["net_amount_prev"])
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
SALES_DEFAULT_YEAR_FROM = 1404
SALES_DEFAULT_YEAR_TO   = 1405


# ─── Dynamic filter builder ────────────────────────────────────────────────────
def _tolist(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if x is not None and x != ""]
    return [v]


def _add_in(conditions: list, params: dict, col: str, vals, typ: str = "String", key: str | None = None):
    vals = _tolist(vals)
    if not vals:
        return
    if key is None:
        key = f"f_{len(params)}"
    if typ == "Int32":
        vals = [int(v) for v in vals]
    if len(vals) == 1:
        params[key] = vals[0]
        conditions.append(f"{col} = {{{key}:{typ}}}")
    else:
        params[key] = vals
        conditions.append(f"{col} IN {{{key}:Array({typ})}}")


def _add_day_between(conditions: list, params: dict, col: str, day_from, day_to, prefix: str = ""):
    d_from = int(day_from) if day_from is not None else None
    d_to   = int(day_to)   if day_to   is not None else None
    if d_from is not None and d_to is not None:
        if d_from > d_to:
            d_from, d_to = d_to, d_from
        params[f"{prefix}d_from"] = d_from
        params[f"{prefix}d_to"]   = d_to
        conditions.append(f"{col} BETWEEN {{{prefix}d_from:Int32}} AND {{{prefix}d_to:Int32}}")
    elif d_from is not None:
        params[f"{prefix}d_from"] = d_from
        conditions.append(f"{col} >= {{{prefix}d_from:Int32}}")
    elif d_to is not None:
        params[f"{prefix}d_to"] = d_to
        conditions.append(f"{col} <= {{{prefix}d_to:Int32}}")


def build_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    year=None, month=None, day=None,
    day_from=None, day_to=None,
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
    if day_from is not None or day_to is not None:
        _add_day_between(conditions, params, "dt.PersianDayInMonth", day_from, day_to)
    else:
        _add_in(conditions, params, "dt.PersianDayInMonth", day, typ="Int32")

    extra_joins = f"\n{_DATE_JOIN}"
    if need_item:
        extra_joins += f"\nINNER JOIN {T_ITEM} AS itm ON itm.ID = f.item_id"

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


def _pct_change(cur: float, prev: float) -> float | None:
    if prev is None or prev == 0:
        return None
    return round(100.0 * (cur - prev) / prev, 1)


def _diagnose(sl: float, sl_prev: float | None, sales_chg: float | None) -> tuple[str, str]:
    sales_down = sales_chg is not None and sales_chg <= SALES_DROP_PCT
    sl_low = sl < SL_TARGET
    sl_down = sl_prev is not None and sl < sl_prev - 2
    if sales_down and (sl_low or sl_down):
        return "warning", "⚠ کاهش فروش همزمان با SL پایین"
    if sales_down:
        return "investigate", "🔍 کاهش فروش — علت غیر SL"
    if sl_low and not sales_down:
        return "risk", "⚠ SL پایین — ریسک آینده"
    return "ok", "✅ وضعیت سالم"


def _resolve_sales_date_ids(
    year, month, day, prefix: str,
    day_from=None, day_to=None,
    default_from: int | None = None, default_to: int | None = None,
) -> tuple[list[int], dict]:
    conditions: list[str] = []
    params: dict = {}
    years = _tolist(year)
    if years:
        _add_in(conditions, params, "PersianYearInt", years, typ="Int32", key=f"{prefix}year")
    else:
        params[f"{prefix}yr_from"] = default_from if default_from is not None else DEFAULT_YEAR_FROM
        params[f"{prefix}yr_to"] = default_to if default_to is not None else DEFAULT_YEAR_TO
        conditions.append(
            f"PersianYearInt BETWEEN {{{prefix}yr_from:Int32}} AND {{{prefix}yr_to:Int32}}"
        )

    _add_in(conditions, params, "PersianMonthNo", month, typ="Int32", key=f"{prefix}month")

    if day_from is not None or day_to is not None:
        _add_day_between(conditions, params, "PersianDayInMonth", day_from, day_to, prefix)
    else:
        _add_in(conditions, params, "PersianDayInMonth", day, typ="Int32", key=f"{prefix}day")

    if not conditions:
        return [], params

    sql = f"SELECT ID FROM {T_DATE} WHERE " + " AND ".join(conditions)
    rows = run_rows(sql, params)
    return [int(r[0]) for r in rows], params


def _add_sales_date(conditions: list, params: dict, year, month, day, prefix: str,
                    day_from=None, day_to=None,
                    default_from: int | None = None, default_to: int | None = None):
    date_ids, date_params = _resolve_sales_date_ids(
        year, month, day, prefix, day_from, day_to, default_from, default_to,
    )
    params.update(date_params)
    if not date_ids:
        conditions.append("0")
    elif len(date_ids) == 1:
        key = f"{prefix}date_id"
        params[key] = date_ids[0]
        conditions.append(f"d.COM_DIM_Date_TransRef = {{{key}:Int32}}")
    else:
        key = f"{prefix}date_ids"
        params[key] = date_ids
        conditions.append(f"d.COM_DIM_Date_TransRef IN {{{key}:Array(Int32)}}")


def build_sl_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    year=None, month=None, day=None,
    day_from=None, day_to=None,
):
    return build_filters(
        province, city, district, store_id,
        ig1, ig2, ig3, ig4, ig5, commerce_dept,
        year, month, day, day_from, day_to,
    )


def build_sales_filters(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    year=None, month=None, day=None,
    day_from=None, day_to=None,
    prefix: str = "s_",
):
    inner_conditions: list[str] = ["d.RTL_DIM_SaleIsReturnSaleRef = 1"]
    outer_conditions: list[str] = []
    params: dict = {}

    _add_sales_date(
        inner_conditions, params, year, month, day, prefix, day_from, day_to,
        default_from=SALES_DEFAULT_YEAR_FROM, default_to=SALES_DEFAULT_YEAR_TO,
    )

    need_item = any(_tolist(x) for x in [ig1, ig2, ig3, ig4, ig5, commerce_dept])

    _add_in(outer_conditions, params, "loc.StateChart", province)
    _add_in(outer_conditions, params, "loc.CityChart", city)
    _add_in(outer_conditions, params, _DIST, district)
    _add_in(outer_conditions, params, "loc.BKInventLocationId", store_id)

    for col, val in [("Level1", ig1), ("Level2", ig2), ("Level3", ig3), ("Level4", ig4), ("Level5", ig5)]:
        _add_in(inner_conditions, params, f"itm.{col}", val)
    _add_in(inner_conditions, params, "itm.CommerceDepartment", commerce_dept)

    item_join = ""
    if need_item:
        item_join = f"INNER JOIN {T_ITEM} AS itm ON itm.ID = d.COM_DIM_ItemRef"
    inner_joins = f"\n{item_join}" if item_join else ""

    return inner_joins, inner_conditions, outer_conditions, params


def combined_filter_kw(
    a_year=None, a_month=None, a_day_from=None, a_day_to=None,
    b_year=None, b_month=None, b_day_from=None, b_day_to=None,
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
) -> dict:
    kw = filter_kw(
        province=province, city=city, district=district, store_id=store_id,
        ig1=ig1, ig2=ig2, ig3=ig3, ig4=ig4, ig5=ig5, cd=commerce_dept,
    )
    for label, y, m, df, dt in [
        ("a", a_year, a_month, a_day_from, a_day_to),
        ("b", b_year, b_month, b_day_from, b_day_to),
    ]:
        ys, ms = _tolist(y), _tolist(m)
        if ys:
            kw[f"{label}_year"] = ",".join(str(x) for x in sorted(ys, key=str))
        if ms:
            kw[f"{label}_month"] = ",".join(str(x) for x in sorted(ms, key=str))
        if df is not None or dt is not None:
            kw[f"{label}_day"] = f"{df or ''}-{dt or ''}"
        if label == "a" and not ys:
            kw["a_yr_range"] = f"{DEFAULT_YEAR_FROM}-{DEFAULT_YEAR_TO}"
    return kw


def _rows_to_map(rows: list, key: str = "name_fa") -> dict:
    return {str(r[key]): r for r in rows}


def _query_sl(group_by: str, name_expr: str, select_extra: str, drill_where: str,
              drill_params: dict, period_year, period_month, period_day, geo_kw: dict,
              day_from=None, day_to=None):
    ej, ew, fp = build_sl_filters(
        **geo_kw, year=period_year, month=period_month, day=period_day,
        day_from=day_from, day_to=day_to,
    )
    params = _merge_params(fp, drill_params)
    sql = f"""
SELECT
    {name_expr}                                     AS name_fa,{select_extra}{_AGG},
    avg(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    avg(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_FACT} AS f
JOIN {T_LOC} AS loc ON loc.ID = f.location_id
{ej}
WHERE loc.StateChart != ''
  {drill_where}
{ew}
GROUP BY {group_by}
ORDER BY sl_percent DESC"""
    return run_query(sql, params)


def _query_sales(group_by: str, name_expr: str, select_extra: str, drill_where: str,
                 drill_params: dict, period_year, period_month, period_day,
                 geo_kw: dict, prefix: str, day_from=None, day_to=None):
    inner_joins, inner_conds, outer_conds, fp = build_sales_filters(
        **geo_kw, year=period_year, month=period_month, day=period_day, prefix=prefix,
        day_from=day_from, day_to=day_to,
    )
    params = _merge_params(fp, drill_params)
    all_conds = inner_conds + outer_conds
    filter_where = (" AND " + " AND ".join(all_conds)) if all_conds else ""
    sql = f"""
SELECT
    {name_expr}                                     AS name_fa,{select_extra}
    sum(toFloat64(d.NetAmount))                     AS net_amount,
    avg(toFloat64OrNull(toString(loc.Latitude)))    AS lat,
    avg(toFloat64OrNull(toString(loc.Longitude)))   AS lng
FROM {T_SALES} AS d
JOIN {T_LOC} AS loc ON loc.ID = d.COM_DIM_InventLocationRef
{inner_joins}
WHERE loc.StateChart != ''
  {drill_where}
{filter_where}
GROUP BY {group_by}
ORDER BY net_amount DESC"""
    return run_query(sql, params)


def _query_store_sl_counts(drill_where: str, drill_params: dict,
                           period_year, period_month, geo_kw: dict,
                           day_from=None, day_to=None) -> dict:
    ej, ew, fp = build_sl_filters(
        **geo_kw, year=period_year, month=period_month, day=None,
        day_from=day_from, day_to=day_to,
    )
    params = _merge_params(fp, drill_params)
    sql = f"""
SELECT
    countIf(sl_percent >= {{sl_high:Float64}}) AS above_80,
    countIf(sl_percent < {{sl_low:Float64}})   AS below_30
FROM (
    SELECT
        round(
            100.0 * sum({_ON_TIME})
            / nullIf(sum(if({_ON_TIME} IS NOT NULL, 1, 0)), 0),
        1)                                          AS sl_percent
    FROM {T_FACT} AS f
    JOIN {T_LOC} AS loc ON loc.ID = f.location_id
    {ej}
    WHERE loc.StateChart != ''
      AND loc.BKInventLocationId != ''
      {drill_where}
    {ew}
    GROUP BY loc.BKInventLocationId
) AS stores"""
    params["sl_high"] = SL_TARGET
    params["sl_low"] = SL_LOW_THRESHOLD
    rows = run_query(sql, params)
    if not rows:
        return {"above_80": 0, "below_30": 0}
    return {
        "above_80": int(rows[0].get("above_80") or 0),
        "below_30": int(rows[0].get("below_30") or 0),
    }


def _merge_combined(sl_a: list, sl_b: list, sales_a: list, sales_b: list, level: str) -> list:
    ma, mb, sa, sb = _rows_to_map(sl_a), _rows_to_map(sl_b), _rows_to_map(sales_a), _rows_to_map(sales_b)
    keys = set(ma) | set(sa)
    out = []
    for key in keys:
        a = ma.get(key, {})
        b = mb.get(key, {})
        sa_row = sa.get(key, {})
        sb_row = sb.get(key, {})
        sl = float(a.get("sl_percent") or 0)
        sl_prev = float(b["sl_percent"]) if "sl_percent" in b else None
        net = float(sa_row.get("net_amount") or 0)
        net_prev = float(sb_row["net_amount"]) if "net_amount" in sb_row else None
        sl_chg = round(sl - sl_prev, 1) if sl_prev is not None else None
        sales_chg = _pct_change(net, net_prev) if net_prev is not None else None
        diag, diag_label = _diagnose(sl, sl_prev, sales_chg)
        row = {
            "name_fa": key,
            "sl_percent": sl,
            "sl_prev": sl_prev,
            "sl_change_pct": sl_chg,
            "target_percent": float(a.get("target_percent") or SL_TARGET),
            "vendor_count": int(a.get("vendor_count") or 0),
            "total_orders": int(a.get("total_orders") or 0),
            "net_amount": net,
            "net_amount_prev": net_prev,
            "sales_change_pct": sales_chg,
            "diagnosis": diag,
            "diagnosis_label": diag_label,
            "lat": float(a.get("lat") or sa_row.get("lat") or 0),
            "lng": float(a.get("lng") or sa_row.get("lng") or 0),
        }
        if level == "store":
            row["store_id"] = a.get("store_id") or sa_row.get("store_id") or key
        out.append(row)
    out.sort(key=lambda r: r["sl_percent"], reverse=True)
    return out


def _combined_endpoint(
    level: str,
    group_sql: str,
    name_expr: str,
    select_extra: str,
    drill_where: str,
    drill_params: dict,
    cache_base: str,
    geo_kw: dict,
    a_year, a_month, b_year, b_month,
    a_day_from=None, a_day_to=None,
    b_day_from=None, b_day_to=None,
    ttl: int = CACHE_TTL_PROVINCE,
):
    kw = combined_filter_kw(
        a_year=a_year, a_month=a_month, a_day_from=a_day_from, a_day_to=a_day_to,
        b_year=b_year, b_month=b_month, b_day_from=b_day_from, b_day_to=b_day_to,
        province=geo_kw.get("province"), city=geo_kw.get("city"),
        district=geo_kw.get("district"), store_id=geo_kw.get("store_id"),
        ig1=geo_kw.get("ig1"), ig2=geo_kw.get("ig2"), ig3=geo_kw.get("ig3"),
        ig4=geo_kw.get("ig4"), ig5=geo_kw.get("ig5"),
        commerce_dept=geo_kw.get("commerce_dept"),
    )
    key = filter_key(cache_base, kw)

    def _fetch():
        with ThreadPoolExecutor(max_workers=5) as pool:
            f_sl_a = pool.submit(
                _query_sl, group_sql, name_expr, select_extra, drill_where, drill_params,
                a_year, a_month, None, geo_kw, a_day_from, a_day_to,
            )
            f_sa = pool.submit(
                _query_sales, group_sql, name_expr, select_extra, drill_where, drill_params,
                a_year, a_month, None, geo_kw, "sa_", a_day_from, a_day_to,
            )
            f_counts = pool.submit(
                _query_store_sl_counts, drill_where, drill_params,
                a_year, a_month, geo_kw, a_day_from, a_day_to,
            )
            has_b = any(_tolist(x) for x in [b_year, b_month]) or b_day_from is not None or b_day_to is not None
            f_sl_b = f_sb = None
            if has_b:
                f_sl_b = pool.submit(
                    _query_sl, group_sql, name_expr, select_extra, drill_where, drill_params,
                    b_year, b_month, None, geo_kw, b_day_from, b_day_to,
                )
                f_sb = pool.submit(
                    _query_sales, group_sql, name_expr, select_extra, drill_where, drill_params,
                    b_year, b_month, None, geo_kw, "sb_", b_day_from, b_day_to,
                )
            sl_a = f_sl_a.result()
            sales_a = f_sa.result()
            store_sl_counts = f_counts.result()
            if has_b:
                sl_b = f_sl_b.result()
                sales_b = f_sb.result()
            else:
                sl_b, sales_b = [], []
        return _merge_combined(sl_a, sl_b, sales_a, sales_b, level), store_sl_counts

    now = time.monotonic()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < ttl:
            logger.info(f"cache hit: {key}")
            return result
    result = _fetch()
    _cache[key] = (result, now)
    return result


def _combined_params(
    province=None, city=None, district=None, store_id=None,
    ig1=None, ig2=None, ig3=None, ig4=None, ig5=None,
    commerce_dept=None,
    a_year=None, a_month=None, a_day_from=None, a_day_to=None,
    b_year=None, b_month=None, b_day_from=None, b_day_to=None,
):
    return {
        "province": province, "city": city, "district": district, "store_id": store_id,
        "ig1": ig1, "ig2": ig2, "ig3": ig3, "ig4": ig4, "ig5": ig5,
        "commerce_dept": commerce_dept,
    }, a_year, a_month, b_year, b_month, a_day_from, a_day_to, b_day_from, b_day_to


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


# ─── Combined SL + Sales (dual period) ────────────────────────────────────────

@app.get("/api/combined")
def combined_provinces(
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
    a_year:        Optional[List[int]] = Query(None, alias="a_year"),
    a_month:       Optional[List[int]] = Query(None, alias="a_month"),
    a_day_from:    Optional[int] = Query(None, alias="a_day_from"),
    a_day_to:      Optional[int] = Query(None, alias="a_day_to"),
    b_year:        Optional[List[int]] = Query(None, alias="b_year"),
    b_month:       Optional[List[int]] = Query(None, alias="b_month"),
    b_day_from:    Optional[int] = Query(None, alias="b_day_from"),
    b_day_to:      Optional[int] = Query(None, alias="b_day_to"),
):
    geo_kw, ay, am, by, bm, adf, adt, bdf, bdt = _combined_params(
        province, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        a_year, a_month, a_day_from, a_day_to, b_year, b_month, b_day_from, b_day_to,
    )
    data, store_sl_counts = _combined_endpoint(
        "province", "loc.StateChart", "loc.StateChart", "",
        "", {}, "combined:provinces", geo_kw,
        ay, am, by, bm, adf, adt, bdf, bdt, CACHE_TTL_PROVINCE,
    )
    return {"status": "ok", "level": "province", "data": data, "store_sl_counts": store_sl_counts}


@app.get("/api/combined/cities")
def combined_cities(
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
    a_year:        Optional[List[int]] = Query(None, alias="a_year"),
    a_month:       Optional[List[int]] = Query(None, alias="a_month"),
    a_day_from:    Optional[int] = Query(None, alias="a_day_from"),
    a_day_to:      Optional[int] = Query(None, alias="a_day_to"),
    b_year:        Optional[List[int]] = Query(None, alias="b_year"),
    b_month:       Optional[List[int]] = Query(None, alias="b_month"),
    b_day_from:    Optional[int] = Query(None, alias="b_day_from"),
    b_day_to:      Optional[int] = Query(None, alias="b_day_to"),
):
    geo_kw, ay, am, by, bm, adf, adt, bdf, bdt = _combined_params(
        None, city, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        a_year, a_month, a_day_from, a_day_to, b_year, b_month, b_day_from, b_day_to,
    )
    data, store_sl_counts = _combined_endpoint(
        "city", "loc.CityChart", "loc.CityChart", "",
        "AND loc.StateChart = {drill_province:String} AND loc.CityChart != ''",
        {"drill_province": province},
        f"combined:cities:{province}", geo_kw,
        ay, am, by, bm, adf, adt, bdf, bdt, CACHE_TTL_DETAIL,
    )
    return {"status": "ok", "level": "city", "data": data, "store_sl_counts": store_sl_counts}


@app.get("/api/combined/districts")
def combined_districts(
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
    a_year:        Optional[List[int]] = Query(None, alias="a_year"),
    a_month:       Optional[List[int]] = Query(None, alias="a_month"),
    a_day_from:    Optional[int] = Query(None, alias="a_day_from"),
    a_day_to:      Optional[int] = Query(None, alias="a_day_to"),
    b_year:        Optional[List[int]] = Query(None, alias="b_year"),
    b_month:       Optional[List[int]] = Query(None, alias="b_month"),
    b_day_from:    Optional[int] = Query(None, alias="b_day_from"),
    b_day_to:      Optional[int] = Query(None, alias="b_day_to"),
):
    geo_kw, ay, am, by, bm, adf, adt, bdf, bdt = _combined_params(
        None, None, district, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        a_year, a_month, a_day_from, a_day_to, b_year, b_month, b_day_from, b_day_to,
    )
    data, store_sl_counts = _combined_endpoint(
        "district", _DIST, _DIST, "",
        "AND loc.StateChart = {drill_province:String} AND loc.CityChart = {drill_city:String}",
        {"drill_province": province, "drill_city": city},
        f"combined:districts:{province}:{city}", geo_kw,
        ay, am, by, bm, adf, adt, bdf, bdt, CACHE_TTL_DETAIL,
    )
    return {"status": "ok", "level": "district", "data": data, "store_sl_counts": store_sl_counts}


@app.get("/api/combined/stores")
def combined_stores(
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
    a_year:        Optional[List[int]] = Query(None, alias="a_year"),
    a_month:       Optional[List[int]] = Query(None, alias="a_month"),
    a_day_from:    Optional[int] = Query(None, alias="a_day_from"),
    a_day_to:      Optional[int] = Query(None, alias="a_day_to"),
    b_year:        Optional[List[int]] = Query(None, alias="b_year"),
    b_month:       Optional[List[int]] = Query(None, alias="b_month"),
    b_day_from:    Optional[int] = Query(None, alias="b_day_from"),
    b_day_to:      Optional[int] = Query(None, alias="b_day_to"),
):
    geo_kw, ay, am, by, bm, adf, adt, bdf, bdt = _combined_params(
        None, None, None, store_id, ig1, ig2, ig3, ig4, ig5, commerce_dept,
        a_year, a_month, a_day_from, a_day_to, b_year, b_month, b_day_from, b_day_to,
    )
    data, store_sl_counts = _combined_endpoint(
        "store",
        "loc.BKInventLocationId, loc.Name",
        "loc.Name",
        "loc.BKInventLocationId AS store_id,",
        "AND loc.StateChart = {drill_province:String} AND loc.CityChart = {drill_city:String} "
        f"AND {_DIST} = {{drill_district:String}}",
        {"drill_province": province, "drill_city": city, "drill_district": district},
        f"combined:stores:{province}:{city}:{district}", geo_kw,
        ay, am, by, bm, adf, adt, bdf, bdt, CACHE_TTL_DETAIL,
    )
    return {"status": "ok", "level": "store", "data": data, "store_sl_counts": store_sl_counts}


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
