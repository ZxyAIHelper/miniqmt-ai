from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from config import QMT_USER_DATA_PATH


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = r"E:\WorkSpace\ai\QMT\qmt_outputs"
CACHE_DIR = r"E:\WorkSpace\ai\QMT\.qmt_cache"
DB_FILE = os.path.join(CACHE_DIR, "miniqmt_cb_history.sqlite3")
TRADES_FILE = os.path.join(OUTPUT_DIR, "cb_backtest_trades.csv")
EQUITY_FILE = os.path.join(OUTPUT_DIR, "cb_backtest_equity.csv")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "cb_backtest_summary.csv")
OPTIMIZE_FILE = os.path.join(OUTPUT_DIR, "cb_strategy_search.csv")
STRATEGY_FILE = os.path.join(OUTPUT_DIR, "cb_strategy_definitions.csv")
BEST_BY_STRATEGY_FILE = os.path.join(OUTPUT_DIR, "cb_strategy_best_by_type.csv")
TARGET_FILE = r"E:\WorkSpace\ai\QMT\target_strategy_metrics.csv"
TARGET_APPLIED_FILE = os.path.join(OUTPUT_DIR, "cb_target_constraints_applied.csv")
MONTHLY_FILE = os.path.join(OUTPUT_DIR, "cb_backtest_monthly_returns.csv")
YEARLY_FILE = os.path.join(OUTPUT_DIR, "cb_backtest_yearly_returns.csv")
RUNS_DIR = os.path.join(OUTPUT_DIR, "runs")
STRATEGY_CANDIDATES_FILE = os.path.join(BASE_DIR, "strategy_candidates.json")

WORKER_DATA: dict[str, pd.DataFrame] | None = None
WORKER_ARGS: argparse.Namespace | None = None
WORKER_STOCK_DATA: dict[str, pd.DataFrame] | None = None
ACTIVE_STRATEGIES: list[dict[str, Any]] = []


FACTOR_DESCRIPTIONS = {
    "momentum": "N-day convertible-bond price momentum; higher is stronger trend.",
    "short_reversal": "Negative 5-day momentum; higher favors short-term pullback.",
    "liquidity": "Recent log turnover amount; higher favors easier trading.",
    "liquidity_stability": "Negative recent amount variability; higher favors more stable liquidity.",
    "low_volatility": "Negative return volatility; higher favors smoother bonds.",
    "low_price": "Negative convertible-bond close price; higher favors cheaper bonds.",
    "price_band": "Closeness to the preferred 100-130 price band.",
    "drawdown_control": "Negative recent peak-to-current drawdown; higher avoids names falling far from recent highs.",
    "trend_filter": "Close price above moving average; higher favors healthier trend.",
    "low_amplitude": "Negative high-low amplitude; higher avoids jumpy bonds.",
    "up_day_consistency": "Share of positive return days in the lookback window.",
    "low_downside_volatility": "Negative volatility of losing days only; higher avoids downside turbulence.",
    "tail_loss_control": "Negative worst one-day return in the lookback window.",
    "liquidity_trend": "Recent turnover amount versus the full lookback average.",
    "volume_trend": "Recent volume versus the full lookback average.",
    "price_position": "Close position inside the recent high-low channel.",
    "low_gap_risk": "Negative average open-to-previous-close gap risk.",
    "conversion_premium": "Negative conversion premium rate when MiniQMT provides it; lower premium is better.",
    "double_low": "Negative price plus premium proxy; lower double-low value is better.",
    "conversion_value": "Estimated conversion value from stock price and conversion price; higher is better.",
    "ytm": "Yield to maturity when available; higher is better.",
    "remaining_years": "Remaining maturity years; moderate/longer duration is preferred.",
    "remaining_size": "Remaining issue size proxy; avoids tiny illiquid bonds.",
    "listed_days": "Listed days; avoids very new bonds.",
    "force_redeem_safety": "Penalty for near or announced forced redemption risk.",
    "rating_score": "Credit rating score when available.",
    "stock_momentum": "Underlying stock momentum when stock code is available.",
    "stock_volatility": "Negative underlying stock volatility when stock code is available.",
}


GENERATED_STRATEGIES = [
    {
        "name": "low_price_core",
        "weights": {"low_price": 0.35, "price_band": 0.25, "low_volatility": 0.20, "liquidity": 0.20},
        "description": "Low-price defensive core with liquidity support.",
    },
    {
        "name": "low_price_liquid",
        "weights": {"low_price": 0.30, "price_band": 0.20, "liquidity": 0.30, "low_volatility": 0.20},
        "description": "Low-price bonds that are easier to trade.",
    },
    {
        "name": "low_price_momentum",
        "weights": {"price_band": 0.25, "low_price": 0.25, "momentum": 0.20, "trend_filter": 0.15, "liquidity": 0.15},
        "description": "Cheap bonds with moderate trend confirmation.",
    },
    {
        "name": "low_vol_core",
        "weights": {"low_volatility": 0.40, "low_amplitude": 0.25, "liquidity": 0.20, "price_band": 0.15},
        "description": "Low-volatility defensive rotation.",
    },
    {
        "name": "balanced_defensive",
        "weights": {"price_band": 0.25, "low_volatility": 0.20, "liquidity": 0.20, "drawdown_control": 0.20, "momentum": 0.15},
        "description": "Balanced low price, low volatility, liquidity, and trend.",
    },
    {
        "name": "liquid_low_vol",
        "weights": {"liquidity": 0.30, "liquidity_stability": 0.20, "low_volatility": 0.30, "price_band": 0.20},
        "description": "Liquid names with volatility control.",
    },
    {
        "name": "reversal_defensive",
        "weights": {"short_reversal": 0.25, "drawdown_control": 0.25, "low_volatility": 0.20, "price_band": 0.20, "liquidity": 0.10},
        "description": "Defensive short-term reversal.",
    },
    {
        "name": "drawdown_guard",
        "weights": {"drawdown_control": 0.35, "low_volatility": 0.25, "price_band": 0.20, "liquidity": 0.20},
        "description": "Avoids bonds in deeper recent drawdown while keeping price and liquidity discipline.",
    },
    {
        "name": "stable_liquidity",
        "weights": {"liquidity_stability": 0.30, "liquidity": 0.25, "low_amplitude": 0.25, "price_band": 0.20},
        "description": "Focuses on stable active bonds with controlled intraday amplitude.",
    },
    {
        "name": "trend_band",
        "weights": {"trend_filter": 0.30, "price_band": 0.25, "momentum": 0.20, "low_volatility": 0.15, "liquidity": 0.10},
        "description": "Preferred price band plus trend confirmation.",
    },
    {
        "name": "double_low_core",
        "weights": {"double_low": 0.40, "price_band": 0.20, "liquidity": 0.20, "force_redeem_safety": 0.20},
        "description": "Double-low style ranking when premium data is available.",
    },
    {
        "name": "premium_defensive",
        "weights": {"conversion_premium": 0.35, "low_volatility": 0.20, "price_band": 0.20, "remaining_size": 0.15, "force_redeem_safety": 0.10},
        "description": "Lower premium with defensive liquidity and redemption-risk controls.",
    },
    {
        "name": "ytm_size_defensive",
        "weights": {"ytm": 0.25, "remaining_size": 0.25, "low_volatility": 0.20, "price_band": 0.20, "rating_score": 0.10},
        "description": "Yield, size, rating, and low-volatility defensive mix.",
    },
    {
        "name": "stock_confirmed",
        "weights": {"stock_momentum": 0.25, "stock_volatility": 0.20, "price_band": 0.20, "conversion_premium": 0.20, "liquidity": 0.15},
        "description": "Uses underlying-stock confirmation when stock codes are available.",
    },
    {
        "name": "consistency_trend",
        "weights": {"up_day_consistency": 0.25, "low_downside_volatility": 0.25, "trend_filter": 0.20, "price_band": 0.15, "liquidity": 0.15},
        "description": "Steady positive days, controlled downside, and a modest trend.",
    },
    {
        "name": "tail_guard",
        "weights": {"tail_loss_control": 0.30, "low_downside_volatility": 0.25, "drawdown_control": 0.20, "low_amplitude": 0.15, "liquidity": 0.10},
        "description": "Avoids bonds with jumpy amplitude, large downside tails, and poor drawdown behavior.",
    },
    {
        "name": "liquidity_acceleration",
        "weights": {"liquidity_trend": 0.30, "volume_trend": 0.20, "liquidity_stability": 0.15, "low_volatility": 0.20, "price_band": 0.15},
        "description": "Improving liquidity without giving up price and volatility discipline.",
    },
    {
        "name": "channel_recovery",
        "weights": {"price_position": 0.25, "short_reversal": 0.20, "trend_filter": 0.20, "low_gap_risk": 0.20, "liquidity": 0.15},
        "description": "Recovery inside the recent price channel with controlled gap risk.",
    },
    {
        "name": "maturity_defensive",
        "weights": {"remaining_years": 0.20, "remaining_size": 0.20, "ytm": 0.20, "price_band": 0.25, "low_volatility": 0.15},
        "description": "Remaining-term, size, YTM, and price-band defenses.",
    },
]


@dataclass
class Position:
    volume: int
    cost: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MiniQMT convertible-bond backtest with buy/sell trade records.")
    parser.add_argument("--start", default=three_years_ago())
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--limit", type=int, default=0, help="Convertible-bond universe size. 0 means full universe.")
    parser.add_argument("--strategy-file", default=STRATEGY_CANDIDATES_FILE, help="JSON strategy candidate file generated by humans or AI.")
    parser.add_argument("--strategy", default="balanced_defensive", help="Strategy name from --strategy-file for single backtest mode.")
    parser.add_argument("--top", type=int, default=5, help="Target holding count.")
    parser.add_argument("--lookback", type=int, default=40, help="Signal lookback bars.")
    parser.add_argument("--rebalance-days", type=int, default=10, help="Rebalance every N trading days.")
    parser.add_argument("--cash", type=float, default=10000.0)
    parser.add_argument("--fee-rate", type=float, default=0.0002)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--max-drawdown", type=float, default=0.15, help="Maximum allowed drawdown for optimize mode.")
    parser.add_argument("--optimize", action="store_true", help="Search strategy/parameter combinations and keep the best under max drawdown.")
    parser.add_argument("--workers", type=int, default=default_workers(), help="Parallel worker processes for optimize mode.")
    return parser.parse_args()


def three_years_ago() -> str:
    return (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")


def default_workers() -> int:
    cpu_count = os.cpu_count() or 2
    return max(1, min(8, cpu_count - 1))


def bootstrap_xtquant() -> None:
    qmt_root = os.path.dirname(os.path.abspath(QMT_USER_DATA_PATH))
    bin_dir = os.path.join(qmt_root, "bin.x64")
    site_packages = os.path.join(bin_dir, "Lib", "site-packages")
    if os.path.isdir(bin_dir):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
        try:
            os.add_dll_directory(bin_dir)
        except Exception:
            pass
    if os.path.isdir(site_packages) and site_packages not in sys.path:
        sys.path.append(site_packages)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        if not math.isfinite(number):
            return default
        return number
    except Exception:
        return default


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def strategy_definition_rows(strategies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for item in strategies:
        rows.append({
            "strategy": item["name"],
            "definition": " + ".join(f"{weight:.4f}*{factor}" for factor, weight in item["weights"].items()),
            "description": item.get("description", ""),
        })
    return rows


def validate_strategies(strategies: Any, source: str) -> list[dict[str, Any]]:
    if isinstance(strategies, dict):
        strategies = strategies.get("strategies", [])
    if not isinstance(strategies, list) or not strategies:
        raise ValueError(f"No strategies found in {source}.")

    seen = set()
    cleaned = []
    for index, item in enumerate(strategies, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Strategy #{index} in {source} is not an object.")
        name = safe_text(item.get("name")).strip()
        if not name:
            raise ValueError(f"Strategy #{index} in {source} has no name.")
        if name in seen:
            raise ValueError(f"Duplicate strategy name in {source}: {name}")
        weights = item.get("weights")
        if not isinstance(weights, dict) or not weights:
            raise ValueError(f"Strategy {name} in {source} has no weights.")
        clean_weights = {}
        for factor, weight in weights.items():
            factor_name = safe_text(factor).strip()
            if factor_name not in FACTOR_DESCRIPTIONS:
                raise ValueError(f"Strategy {name} uses unknown factor: {factor_name}")
            factor_weight = safe_float(weight, math.nan)
            if not math.isfinite(factor_weight) or factor_weight == 0:
                raise ValueError(f"Strategy {name} has invalid weight for {factor_name}: {weight}")
            clean_weights[factor_name] = factor_weight
        seen.add(name)
        cleaned.append({
            "name": name,
            "weights": clean_weights,
            "description": safe_text(item.get("description")),
        })
    return cleaned


def ensure_strategy_file(path: str) -> None:
    if os.path.exists(path):
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    payload = {
        "version": 1,
        "notes": "Editable strategy candidates. AI can add, remove, or adjust weights after each archived backtest round.",
        "strategies": GENERATED_STRATEGIES,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_strategy_candidates(path: str) -> list[dict[str, Any]]:
    ensure_strategy_file(path)
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return validate_strategies(payload, path)


def set_active_strategies(strategies: list[dict[str, Any]]) -> None:
    global ACTIVE_STRATEGIES
    ACTIVE_STRATEGIES = strategies


def copy_file(src: str, dst: str) -> None:
    if not os.path.exists(src):
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        fdst.write(fsrc.read())


def first_value(data: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in data and data.get(key) not in [None, ""]:
            return data.get(key)
    return default


def normalize_yyyymmdd(value: Any) -> str:
    try:
        text = str(int(float(value)))
    except Exception:
        return ""
    if len(text) >= 8 and "1970010" not in text and text not in ["0", "99999999"]:
        return text[:8]
    return ""


def rating_to_score(rating: str) -> float:
    order = ["C", "CC", "CCC", "B-", "B", "B+", "BB-", "BB", "BB+", "BBB-", "BBB", "BBB+", "A-", "A", "A+", "AA-", "AA", "AA+", "AAA"]
    rating = safe_text(rating).strip().upper()
    if rating not in order:
        return 0.0
    return order.index(rating) / max(len(order) - 1, 1)


def days_between(start: str, end: str) -> int:
    try:
        d1 = datetime.strptime(start[:8], "%Y%m%d")
        d2 = datetime.strptime(end[:8], "%Y%m%d")
        return (d2 - d1).days
    except Exception:
        return 0


def is_cb_code(code: str) -> bool:
    code = str(code)
    return code.endswith((".SH", ".SZ")) and code[:3] in ["110", "111", "113", "118", "123", "127", "128"]


def as_date_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    result = df.copy()
    result.index = pd.to_datetime(result.index)
    result = result.sort_index()
    return result


def init_db() -> sqlite3.Connection:
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (code, date)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_bars_date ON daily_bars(date)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            code TEXT PRIMARY KEY,
            synced_until TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cb_metadata (
            code TEXT PRIMARY KEY,
            name TEXT,
            stock_code TEXT,
            list_date TEXT,
            maturity_date TEXT,
            force_redeem_date TEXT,
            force_redeem_status TEXT,
            conv_price REAL,
            conversion_premium REAL,
            ytm REAL,
            remaining_size REAL,
            rating TEXT,
            raw_json TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def next_yyyymmdd(date_text: str) -> str:
    return (datetime.strptime(date_text, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def cached_range(conn: sqlite3.Connection, code: str) -> tuple[str | None, str | None]:
    row = conn.execute("SELECT MIN(date), MAX(date) FROM daily_bars WHERE code = ?", (code,)).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def synced_until(conn: sqlite3.Connection, code: str) -> str | None:
    row = conn.execute("SELECT synced_until FROM sync_state WHERE code = ?", (code,)).fetchone()
    return row[0] if row else None


def mark_synced(conn: sqlite3.Connection, code: str, end: str) -> None:
    conn.execute(
        """
        INSERT INTO sync_state (code, synced_until, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            synced_until=excluded.synced_until,
            updated_at=excluded.updated_at
        """,
        (code, end, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )


def normalize_conversion_premium(value: Any) -> float:
    number = safe_float(value, 0.0)
    if abs(number) > 3.0:
        number = number / 100.0
    return number


def sync_cb_metadata(xtdata: Any, conn: sqlite3.Connection, codes: list[str]) -> None:
    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for idx, code in enumerate(codes, 1):
        info: dict[str, Any] = {}
        detail: dict[str, Any] = {}
        try:
            info = xtdata.get_cb_info(code) or {}
        except Exception:
            info = {}
        try:
            detail = xtdata.get_instrument_detail(code) or {}
        except Exception:
            detail = {}
        merged = {**detail, **info}
        stock_code = safe_text(first_value(merged, ["stockCode", "StockCode", "underlyingCode"], ""))
        rows.append((
            code,
            safe_text(first_value(merged, ["bondName", "InstrumentName", "instrumentName", "Name"], "")),
            stock_code if is_stock_code(stock_code) else "",
            normalize_yyyymmdd(first_value(merged, ["bondListDate", "OpenDate", "openDate"], "")),
            normalize_yyyymmdd(first_value(merged, ["bondMaturityDate", "ExpireDate", "expireDate", "delistDate"], "")),
            normalize_yyyymmdd(first_value(merged, ["forceRedeemTradeDate", "forceRedeemDate"], "")),
            safe_text(first_value(merged, ["redeemStatus", "forceRedeemStatus"], "")),
            safe_float(first_value(merged, ["bondConvPrice", "convPrice", "conversionPrice"], 0.0), 0.0),
            normalize_conversion_premium(first_value(merged, ["analConvpremiumratio", "convPremiumRatio", "conversionPremiumRate"], 0.0)),
            safe_float(first_value(merged, ["ytm", "analYTM"], 0.0), 0.0),
            safe_float(first_value(merged, ["bondReMainSize", "remainSize", "FloatVolume", "bondIssueSize", "TotalVolume"], 0.0), 0.0),
            safe_text(first_value(merged, ["level", "rating"], "")),
            json.dumps(merged, ensure_ascii=False, default=str),
            now,
        ))
        if idx % 50 == 0 or idx == len(codes):
            print(f"metadata_sync_progress={idx}/{len(codes)}", flush=True)

    conn.executemany(
        """
        INSERT INTO cb_metadata (
            code, name, stock_code, list_date, maturity_date, force_redeem_date,
            force_redeem_status, conv_price, conversion_premium, ytm, remaining_size,
            rating, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name=excluded.name,
            stock_code=excluded.stock_code,
            list_date=excluded.list_date,
            maturity_date=excluded.maturity_date,
            force_redeem_date=excluded.force_redeem_date,
            force_redeem_status=excluded.force_redeem_status,
            conv_price=excluded.conv_price,
            conversion_premium=excluded.conversion_premium,
            ytm=excluded.ytm,
            remaining_size=excluded.remaining_size,
            rating=excluded.rating,
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    conn.commit()


def load_metadata(conn: sqlite3.Connection, codes: list[str]) -> dict[str, dict[str, Any]]:
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    rows = conn.execute(
        f"""
        SELECT code, name, stock_code, list_date, maturity_date, force_redeem_date,
               force_redeem_status, conv_price, conversion_premium, ytm,
               remaining_size, rating
        FROM cb_metadata
        WHERE code IN ({placeholders})
        """,
        codes,
    ).fetchall()
    result = {}
    for row in rows:
        result[row[0]] = {
            "name": row[1],
            "stock_code": row[2],
            "list_date": row[3],
            "maturity_date": row[4],
            "force_redeem_date": row[5],
            "force_redeem_status": row[6],
            "conv_price": safe_float(row[7], 0.0),
            "conversion_premium": safe_float(row[8], 0.0),
            "ytm": safe_float(row[9], 0.0),
            "remaining_size": safe_float(row[10], 0.0),
            "rating": row[11],
        }
    return result


def is_stock_code(code: str) -> bool:
    code = str(code)
    return code.endswith((".SH", ".SZ")) and len(code) >= 9


def upsert_bars(conn: sqlite3.Connection, code: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    normalized = as_date_index(df)
    for date, row in normalized.iterrows():
        close = safe_float(row.get("close"), 0.0)
        if close <= 0:
            continue
        rows.append((
            code,
            date.strftime("%Y%m%d"),
            safe_float(row.get("open"), 0.0),
            safe_float(row.get("high"), 0.0),
            safe_float(row.get("low"), 0.0),
            close,
            safe_float(row.get("volume"), 0.0),
            safe_float(row.get("amount"), 0.0),
            now,
        ))
    conn.executemany(
        """
        INSERT INTO daily_bars (code, date, open, high, low, close, volume, amount, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume,
            amount=excluded.amount,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def fetch_and_store(xtdata: Any, conn: sqlite3.Connection, codes: list[str], start: str, end: str) -> int:
    raw = xtdata.get_market_data_ex(
        ["open", "high", "low", "close", "volume", "amount"],
        codes,
        period="1d",
        start_time=start,
        end_time=end,
        count=-1,
        fill_data=True,
    )
    saved = 0
    for code, df in (raw or {}).items():
        saved += upsert_bars(conn, code, df)
    conn.commit()
    return saved


def ensure_history_sqlite(xtdata: Any, conn: sqlite3.Connection, codes: list[str], start: str, end: str) -> None:
    requested = 0
    skipped = 0
    for idx, code in enumerate(codes, 1):
        if (synced_until(conn, code) or "") >= end:
            skipped += 1
            if idx % 30 == 0 or idx == len(codes):
                print(f"sqlite_sync_progress={idx}/{len(codes)} requested={requested} skipped={skipped}", flush=True)
            continue

        first_date, last_date = cached_range(conn, code)
        ranges: list[tuple[str, str]] = []
        if first_date is None or last_date is None:
            ranges.append((start, end))
        else:
            if last_date < end:
                ranges.append((next_yyyymmdd(last_date), end))

        if not ranges:
            skipped += 1
            mark_synced(conn, code, end)
        for range_start, range_end in ranges:
            if range_start > range_end:
                continue
            requested += 1
            xtdata.download_history_data(code, period="1d", start_time=range_start, end_time=range_end)
            fetch_and_store(xtdata, conn, [code], range_start, range_end)
            mark_synced(conn, code, end)
            conn.commit()

        if idx % 30 == 0 or idx == len(codes):
            print(f"sqlite_sync_progress={idx}/{len(codes)} requested={requested} skipped={skipped}", flush=True)
    print(f"sqlite_sync_done requested={requested} skipped={skipped} db={DB_FILE}", flush=True)


def load_data_from_sqlite(conn: sqlite3.Connection, codes: list[str], start: str, end: str, metadata: dict[str, dict[str, Any]] | None = None) -> dict[str, pd.DataFrame]:
    data = {}
    for code in codes:
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM daily_bars
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date
            """,
            conn,
            params=(code, start, end),
        )
        if df.empty:
            continue
        df.index = pd.to_datetime(df["date"])
        df = df.drop(columns=["date"])
        df = df[df["close"].map(safe_float) > 0]
        if not df.empty:
            if metadata and code in metadata:
                df.attrs["meta"] = metadata[code]
            data[code] = df
    return data


def trading_dates(data: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    dates: set[pd.Timestamp] = set()
    for df in data.values():
        dates.update(df.index)
    return sorted(dates)


def bar_on_or_before(df: pd.DataFrame, date: pd.Timestamp) -> pd.Series | None:
    hist = df.loc[df.index <= date]
    if hist.empty:
        return None
    return hist.iloc[-1]


def close_on(data: dict[str, pd.DataFrame], code: str, date: pd.Timestamp) -> float:
    df = data.get(code)
    if df is None or df.empty:
        return 0.0
    bar = bar_on_or_before(df, date)
    return safe_float(bar["close"], 0.0) if bar is not None else 0.0


def stock_stats(stock_data: dict[str, pd.DataFrame] | None, stock_code: str, date: pd.Timestamp, lookback: int) -> tuple[float, float]:
    if not stock_data or not stock_code:
        return 0.0, 0.0
    df = stock_data.get(stock_code)
    if df is None or df.empty:
        return 0.0, 0.0
    hist = df.loc[df.index <= date].tail(lookback + 1)
    if len(hist) < 2:
        return 0.0, 0.0
    first_close = safe_float(hist["close"].iloc[0], 0.0)
    last_close = safe_float(hist["close"].iloc[-1], 0.0)
    momentum = last_close / first_close - 1.0 if first_close > 0 else 0.0
    volatility = safe_float(hist["close"].pct_change(fill_method=None).std(), 0.0)
    return momentum, volatility


def zscore(value: float, values: list[float]) -> float:
    if not values:
        return 0.0
    avg = sum(values) / len(values)
    var = sum((item - avg) * (item - avg) for item in values) / len(values)
    std = math.sqrt(var)
    if std <= 0:
        return 0.0
    return (value - avg) / std


def strategy_weights(strategy: str) -> dict[str, float]:
    for item in ACTIVE_STRATEGIES:
        if item["name"] == strategy:
            return item["weights"]
    raise ValueError(f"Unknown strategy: {strategy}")


def score_universe(data: dict[str, pd.DataFrame], date: pd.Timestamp, lookback: int, strategy: str, stock_data: dict[str, pd.DataFrame] | None = None) -> list[tuple[str, float]]:
    features = []
    for code, df in data.items():
        hist = df.loc[df.index <= date].tail(lookback + 1)
        if len(hist) < lookback + 1:
            continue
        first_close = safe_float(hist["close"].iloc[0], 0.0)
        last_close = safe_float(hist["close"].iloc[-1], 0.0)
        if first_close <= 0 or last_close <= 0:
            continue
        returns = hist["close"].pct_change(fill_method=None)
        recent_returns = returns.dropna()
        momentum = last_close / first_close - 1.0
        short_momentum = last_close / safe_float(hist["close"].tail(6).iloc[0], last_close) - 1.0
        liquidity = safe_float(hist["amount"].tail(5).mean(), 0.0)
        lookback_liquidity = safe_float(hist["amount"].mean(), 0.0)
        recent_volume = safe_float(hist["volume"].tail(5).mean(), 0.0)
        lookback_volume = safe_float(hist["volume"].mean(), 0.0)
        liquidity_std = safe_float(hist["amount"].tail(min(lookback, 20)).std(), 0.0)
        volatility = safe_float(returns.std(), 0.0)
        downside_returns = recent_returns[recent_returns < 0]
        downside_volatility = safe_float(downside_returns.std(), 0.0) if len(downside_returns) > 1 else 0.0
        worst_return = safe_float(recent_returns.min(), 0.0) if len(recent_returns) else 0.0
        up_day_consistency = safe_float((recent_returns > 0).mean(), 0.0) if len(recent_returns) else 0.0
        recent_high = safe_float(hist["close"].max(), last_close)
        recent_low = safe_float(hist["close"].min(), last_close)
        recent_drawdown = last_close / recent_high - 1.0 if recent_high > 0 else 0.0
        price_position = (last_close - recent_low) / (recent_high - recent_low) if recent_high > recent_low else 0.5
        ma_window = min(20, len(hist))
        moving_average = safe_float(hist["close"].tail(ma_window).mean(), last_close)
        trend_strength = last_close / moving_average - 1.0 if moving_average > 0 else 0.0
        amplitude = safe_float(((hist["high"] - hist["low"]) / hist["close"]).tail(min(lookback, 20)).mean(), 0.0)
        gaps = (hist["open"] / hist["close"].shift(1) - 1.0).abs().dropna()
        gap_risk = safe_float(gaps.tail(min(lookback, 20)).mean(), 0.0)
        liquidity_trend = liquidity / lookback_liquidity - 1.0 if lookback_liquidity > 0 else 0.0
        volume_trend = recent_volume / lookback_volume - 1.0 if lookback_volume > 0 else 0.0
        price_band_distance = 0.0 if 100.0 <= last_close <= 130.0 else min(abs(last_close - 100.0), abs(last_close - 130.0)) / 100.0
        meta = df.attrs.get("meta", {})
        trade_date = date.strftime("%Y%m%d")
        list_date = safe_text(meta.get("list_date"))
        maturity_date = safe_text(meta.get("maturity_date"))
        force_redeem_date = safe_text(meta.get("force_redeem_date"))
        listed_days = days_between(list_date, trade_date) if list_date else 0
        remaining_years = days_between(trade_date, maturity_date) / 365.0 if maturity_date else 0.0
        force_days = days_between(trade_date, force_redeem_date) if force_redeem_date else 999999
        redeem_status = safe_text(meta.get("force_redeem_status"))
        force_redeem_safety = 0.0
        if 0 <= force_days <= 30:
            force_redeem_safety = -1.0
        elif any(word in redeem_status for word in ["强赎", "赎回", "最后", "公告", "满足"]):
            force_redeem_safety = -0.7
        conv_price = safe_float(meta.get("conv_price"), 0.0)
        stock_code = safe_text(meta.get("stock_code"))
        stock_momentum, stock_volatility = stock_stats(stock_data, stock_code, date, lookback)
        stock_price = close_on(stock_data or {}, stock_code, date) if stock_code else 0.0
        conversion_value = 100.0 / conv_price * stock_price if conv_price > 0 and stock_price > 0 else 0.0
        conversion_premium = safe_float(meta.get("conversion_premium"), 0.0)
        if conversion_premium == 0.0 and conversion_value > 0:
            conversion_premium = last_close / conversion_value - 1.0
        double_low = last_close + conversion_premium * 100.0 if conversion_premium != 0 else last_close
        features.append({
            "code": code,
            "momentum": momentum,
            "short_momentum": short_momentum,
            "liquidity": liquidity,
            "liquidity_stability": -liquidity_std / (liquidity + 1.0),
            "volatility": volatility,
            "close": last_close,
            "price_band": -price_band_distance,
            "drawdown_control": recent_drawdown,
            "trend_filter": trend_strength,
            "amplitude": amplitude,
            "up_day_consistency": up_day_consistency,
            "downside_volatility": downside_volatility,
            "worst_return": worst_return,
            "liquidity_trend": liquidity_trend,
            "volume_trend": volume_trend,
            "price_position": price_position,
            "gap_risk": gap_risk,
            "conversion_premium": conversion_premium,
            "double_low": double_low,
            "conversion_value": conversion_value,
            "ytm": safe_float(meta.get("ytm"), 0.0),
            "remaining_years": remaining_years,
            "remaining_size": safe_float(meta.get("remaining_size"), 0.0),
            "listed_days": listed_days,
            "force_redeem_safety": force_redeem_safety,
            "rating_score": rating_to_score(safe_text(meta.get("rating"))),
            "stock_momentum": stock_momentum,
            "stock_volatility": stock_volatility,
        })

    if not features:
        return []

    momentum_values = [item["momentum"] for item in features]
    liquidity_values = [math.log1p(item["liquidity"]) for item in features]
    liquidity_stability_values = [item["liquidity_stability"] for item in features]
    volatility_values = [item["volatility"] for item in features]
    close_values = [item["close"] for item in features]
    short_momentum_values = [item["short_momentum"] for item in features]
    price_band_values = [item["price_band"] for item in features]
    drawdown_values = [item["drawdown_control"] for item in features]
    trend_values = [item["trend_filter"] for item in features]
    amplitude_values = [item["amplitude"] for item in features]
    up_day_consistency_values = [item["up_day_consistency"] for item in features]
    downside_volatility_values = [item["downside_volatility"] for item in features]
    worst_return_values = [item["worst_return"] for item in features]
    liquidity_trend_values = [item["liquidity_trend"] for item in features]
    volume_trend_values = [item["volume_trend"] for item in features]
    price_position_values = [item["price_position"] for item in features]
    gap_risk_values = [item["gap_risk"] for item in features]
    conversion_premium_values = [item["conversion_premium"] for item in features]
    double_low_values = [item["double_low"] for item in features]
    conversion_value_values = [item["conversion_value"] for item in features]
    ytm_values = [item["ytm"] for item in features]
    remaining_years_values = [item["remaining_years"] for item in features]
    remaining_size_values = [math.log1p(max(item["remaining_size"], 0.0)) for item in features]
    listed_days_values = [min(item["listed_days"], 365.0) for item in features]
    force_redeem_safety_values = [item["force_redeem_safety"] for item in features]
    rating_score_values = [item["rating_score"] for item in features]
    stock_momentum_values = [item["stock_momentum"] for item in features]
    stock_volatility_values = [item["stock_volatility"] for item in features]

    scored = []
    for item in features:
        momentum_z = zscore(item["momentum"], momentum_values)
        short_reversal_z = -zscore(item["short_momentum"], short_momentum_values)
        liquidity_z = zscore(math.log1p(item["liquidity"]), liquidity_values)
        liquidity_stability_z = zscore(item["liquidity_stability"], liquidity_stability_values)
        low_vol_z = -zscore(item["volatility"], volatility_values)
        low_price_z = -zscore(item["close"], close_values)
        price_band_z = zscore(item["price_band"], price_band_values)
        drawdown_control_z = zscore(item["drawdown_control"], drawdown_values)
        trend_filter_z = zscore(item["trend_filter"], trend_values)
        low_amplitude_z = -zscore(item["amplitude"], amplitude_values)
        up_day_consistency_z = zscore(item["up_day_consistency"], up_day_consistency_values)
        low_downside_volatility_z = -zscore(item["downside_volatility"], downside_volatility_values)
        tail_loss_control_z = zscore(item["worst_return"], worst_return_values)
        liquidity_trend_z = zscore(item["liquidity_trend"], liquidity_trend_values)
        volume_trend_z = zscore(item["volume_trend"], volume_trend_values)
        price_position_z = zscore(item["price_position"], price_position_values)
        low_gap_risk_z = -zscore(item["gap_risk"], gap_risk_values)
        conversion_premium_z = -zscore(item["conversion_premium"], conversion_premium_values)
        double_low_z = -zscore(item["double_low"], double_low_values)
        conversion_value_z = zscore(item["conversion_value"], conversion_value_values)
        ytm_z = zscore(item["ytm"], ytm_values)
        remaining_years_z = zscore(item["remaining_years"], remaining_years_values)
        remaining_size_z = zscore(math.log1p(max(item["remaining_size"], 0.0)), remaining_size_values)
        listed_days_z = zscore(min(item["listed_days"], 365.0), listed_days_values)
        force_redeem_safety_z = zscore(item["force_redeem_safety"], force_redeem_safety_values)
        rating_score_z = zscore(item["rating_score"], rating_score_values)
        stock_momentum_z = zscore(item["stock_momentum"], stock_momentum_values)
        stock_volatility_z = -zscore(item["stock_volatility"], stock_volatility_values)

        factor_values = {
            "momentum": momentum_z,
            "short_reversal": short_reversal_z,
            "liquidity": liquidity_z,
            "liquidity_stability": liquidity_stability_z,
            "low_volatility": low_vol_z,
            "low_price": low_price_z,
            "price_band": price_band_z,
            "drawdown_control": drawdown_control_z,
            "trend_filter": trend_filter_z,
            "low_amplitude": low_amplitude_z,
            "up_day_consistency": up_day_consistency_z,
            "low_downside_volatility": low_downside_volatility_z,
            "tail_loss_control": tail_loss_control_z,
            "liquidity_trend": liquidity_trend_z,
            "volume_trend": volume_trend_z,
            "price_position": price_position_z,
            "low_gap_risk": low_gap_risk_z,
            "conversion_premium": conversion_premium_z,
            "double_low": double_low_z,
            "conversion_value": conversion_value_z,
            "ytm": ytm_z,
            "remaining_years": remaining_years_z,
            "remaining_size": remaining_size_z,
            "listed_days": listed_days_z,
            "force_redeem_safety": force_redeem_safety_z,
            "rating_score": rating_score_z,
            "stock_momentum": stock_momentum_z,
            "stock_volatility": stock_volatility_z,
        }
        score = sum(weight * factor_values[factor] for factor, weight in strategy_weights(strategy).items())

        scored.append((item["code"], score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def portfolio_value(cash: float, positions: dict[str, Position], data: dict[str, pd.DataFrame], date: pd.Timestamp) -> float:
    value = cash
    for code, pos in positions.items():
        value += pos.volume * close_on(data, code, date)
    return value


def round_lot(volume: float, lot: int = 10) -> int:
    return int(volume / lot) * lot


def write_csv(path: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def copy_csv(src: str, dst: str) -> None:
    copy_file(src, dst)


def period_returns(equity: list[dict[str, Any]], period_len: int) -> list[dict[str, Any]]:
    if not equity:
        return []
    df = pd.DataFrame(equity)
    df["date"] = pd.to_datetime(df["date"])
    df["total_value"] = df["total_value"].map(safe_float)
    df["period"] = df["date"].dt.strftime("%Y%m" if period_len == 6 else "%Y")
    rows = []
    for period, group in df.groupby("period"):
        start_value = safe_float(group["total_value"].iloc[0], 0.0)
        end_value = safe_float(group["total_value"].iloc[-1], 0.0)
        period_return = end_value / start_value - 1.0 if start_value > 0 else 0.0
        rows.append({"period": period, "return": round(period_return, 6)})
    return rows


def advanced_metrics(equity: list[dict[str, Any]], initial_cash: float) -> dict[str, Any]:
    if not equity:
        return {
            "annual_return": 0.0,
            "calmar": 0.0,
            "sharpe": 0.0,
            "monthly_win_rate": 0.0,
            "max_monthly_loss": 0.0,
            "max_drawdown_days": 0,
        }
    values = [safe_float(row["total_value"], initial_cash) for row in equity]
    total_return = values[-1] / initial_cash - 1.0 if initial_cash > 0 else 0.0
    years = max(len(values) / 242.0, 1 / 242.0)
    annual_return = (1.0 + total_return) ** (1.0 / years) - 1.0 if total_return > -1 else -1.0
    daily_returns = pd.Series(values).pct_change(fill_method=None).dropna()
    sharpe = 0.0
    if not daily_returns.empty and safe_float(daily_returns.std(), 0.0) > 0:
        sharpe = safe_float(daily_returns.mean(), 0.0) / safe_float(daily_returns.std(), 0.0) * math.sqrt(242.0)

    peak = values[0]
    max_drawdown = 0.0
    current_drawdown_days = 0
    max_drawdown_days = 0
    for value in values:
        if value >= peak:
            peak = value
            current_drawdown_days = 0
        else:
            current_drawdown_days += 1
            max_drawdown_days = max(max_drawdown_days, current_drawdown_days)
        if peak > 0:
            max_drawdown = min(max_drawdown, value / peak - 1.0)

    monthly = period_returns(equity, 6)
    monthly_returns = [safe_float(row["return"], 0.0) for row in monthly]
    monthly_win_rate = sum(1 for ret in monthly_returns if ret > 0) / len(monthly_returns) if monthly_returns else 0.0
    max_monthly_loss = min(monthly_returns) if monthly_returns else 0.0
    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0.0
    return {
        "annual_return": round(annual_return, 6),
        "calmar": round(calmar, 6),
        "sharpe": round(sharpe, 6),
        "monthly_win_rate": round(monthly_win_rate, 6),
        "max_monthly_loss": round(max_monthly_loss, 6),
        "max_drawdown_days": max_drawdown_days,
    }


def write_strategy_definitions(strategies: list[dict[str, Any]]) -> None:
    write_csv(STRATEGY_FILE, strategy_definition_rows(strategies), ["strategy", "definition", "description"])


def write_factor_definitions(path: str) -> None:
    rows = [{"factor": factor, "description": description} for factor, description in FACTOR_DESCRIPTIONS.items()]
    write_csv(path, rows, ["factor", "description"])


def write_target_constraints() -> None:
    rows = [
        {"constraint": "initial_cash", "value": "10000", "source": TARGET_FILE},
        {"constraint": "annual_return_min", "value": "0.08", "source": TARGET_FILE},
        {"constraint": "max_drawdown_max", "value": "0.15", "source": TARGET_FILE},
        {"constraint": "calmar_min", "value": "1.0", "source": TARGET_FILE},
        {"constraint": "monthly_win_rate_min", "value": "0.60", "source": TARGET_FILE},
        {"constraint": "max_monthly_loss_min", "value": "-0.05", "source": TARGET_FILE},
        {"constraint": "holding_count_range", "value": "3-8", "source": TARGET_FILE},
        {"constraint": "price_preference", "value": "100-130 preferred through low_price score", "source": TARGET_FILE},
        {"constraint": "costs", "value": "fee_rate + slippage_rate", "source": TARGET_FILE},
    ]
    write_csv(TARGET_APPLIED_FILE, rows, ["constraint", "value", "source"])


def best_by_strategy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not row.get("passed"):
            continue
        strategy = row["strategy"]
        if strategy not in best or safe_float(row["rank_score"], 0.0) > safe_float(best[strategy]["rank_score"], 0.0):
            best[strategy] = row
    return [best[name] for name in sorted(best)]


def run_backtest(args: argparse.Namespace, data: dict[str, pd.DataFrame], stock_data: dict[str, pd.DataFrame] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    dates = trading_dates(data)
    if len(dates) <= args.lookback:
        raise RuntimeError("Not enough historical bars for the requested lookback.")

    cash = args.cash
    positions: dict[str, Position] = {}
    trades: list[dict[str, Any]] = []
    equity: list[dict[str, Any]] = []
    rebalance_index = 0

    for idx, date in enumerate(dates):
        total_value = portfolio_value(cash, positions, data, date)
        equity.append({
            "date": date.strftime("%Y%m%d"),
            "cash": round(cash, 4),
            "position_value": round(total_value - cash, 4),
            "total_value": round(total_value, 4),
            "holding_count": len(positions),
        })

        if idx < args.lookback:
            continue
        if rebalance_index % args.rebalance_days != 0:
            rebalance_index += 1
            continue
        rebalance_index += 1

        selected = [code for code, _ in score_universe(data, date, args.lookback, args.strategy, stock_data)[: args.top]]
        selected_set = set(selected)

        for code in list(positions):
            if code in selected_set:
                continue
            price = close_on(data, code, date)
            if price <= 0:
                continue
            sell_price = price * (1.0 - args.slippage_rate)
            pos = positions.pop(code)
            amount = pos.volume * sell_price
            fee = amount * args.fee_rate
            cash += amount - fee
            trades.append({
                "date": date.strftime("%Y%m%d"),
                "side": "sell",
                "code": code,
                "price": round(sell_price, 4),
                "volume": pos.volume,
                "amount": round(amount, 4),
                "fee": round(fee, 4),
                "reason": "not_selected",
            })

        total_value = portfolio_value(cash, positions, data, date)
        target_value = total_value / len(selected) if selected else 0.0
        for code in selected:
            price = close_on(data, code, date)
            if price <= 0:
                continue
            buy_price = price * (1.0 + args.slippage_rate)
            current_volume = positions.get(code, Position(0, 0.0)).volume
            current_value = current_volume * buy_price
            buy_value = target_value - current_value
            volume = round_lot(buy_value / buy_price)
            if volume <= 0:
                continue
            amount = volume * buy_price
            fee = amount * args.fee_rate
            if amount + fee > cash:
                volume = round_lot(cash / (buy_price * (1 + args.fee_rate)))
                amount = volume * buy_price
                fee = amount * args.fee_rate
            if volume <= 0:
                continue
            cash -= amount + fee
            positions[code] = Position(current_volume + volume, buy_price)
            trades.append({
                "date": date.strftime("%Y%m%d"),
                "side": "buy",
                "code": code,
                "price": round(buy_price, 4),
                "volume": volume,
                "amount": round(amount, 4),
                "fee": round(fee, 4),
                "reason": "selected",
            })

    final_date = dates[-1]
    for code in list(positions):
        price = close_on(data, code, final_date)
        if price <= 0:
            continue
        sell_price = price * (1.0 - args.slippage_rate)
        pos = positions.pop(code)
        amount = pos.volume * sell_price
        fee = amount * args.fee_rate
        cash += amount - fee
        trades.append({
            "date": final_date.strftime("%Y%m%d"),
            "side": "sell",
            "code": code,
            "price": round(sell_price, 4),
            "volume": pos.volume,
            "amount": round(amount, 4),
            "fee": round(fee, 4),
            "reason": "final_liquidation",
        })

    final_value = cash
    start_value = args.cash
    peak = start_value
    max_drawdown = 0.0
    for row in equity:
        value = safe_float(row["total_value"], start_value)
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = min(max_drawdown, value / peak - 1.0)

    summary = {
        "strategy": args.strategy,
        "start": args.start,
        "end": args.end,
        "universe": len(data),
        "top": args.top,
        "lookback": args.lookback,
        "rebalance_days": args.rebalance_days,
        "initial_cash": round(start_value, 4),
        "final_value": round(final_value, 4),
        "total_return": round(final_value / start_value - 1.0, 6),
        "max_drawdown": round(max_drawdown, 6),
        **advanced_metrics(equity, start_value),
        "trade_count": len(trades),
        "buy_count": sum(1 for row in trades if row["side"] == "buy"),
        "sell_count": sum(1 for row in trades if row["side"] == "sell"),
    }
    return trades, equity, summary


def clone_args(args: argparse.Namespace, strategy: str, top: int, lookback: int, rebalance_days: int) -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    copied.strategy = strategy
    copied.top = top
    copied.lookback = lookback
    copied.rebalance_days = rebalance_days
    return copied


def optimize_fields() -> list[str]:
    return [
        "run_id", "strategy", "start", "end", "universe", "top", "lookback", "rebalance_days",
        "initial_cash", "final_value", "total_return", "annual_return", "max_drawdown",
        "calmar", "sharpe", "monthly_win_rate", "max_monthly_loss", "max_drawdown_days",
        "trade_count", "buy_count", "sell_count", "passed", "rank_score",
    ]


def strategy_trials() -> list[tuple[str, int, int, int]]:
    strategies = [item["name"] for item in ACTIVE_STRATEGIES]
    tops = [3, 5, 8]
    lookbacks = [40, 60, 90]
    rebalance_days = [10, 20, 40]
    return [
        (strategy, top, lookback, rebalance_day)
        for strategy in strategies
        for top in tops
        for lookback in lookbacks
        for rebalance_day in rebalance_days
    ]


def result_for_trial(args: argparse.Namespace, data: dict[str, pd.DataFrame], stock_data: dict[str, pd.DataFrame] | None, run_id: str, trial: tuple[str, int, int, int]) -> tuple[dict[str, Any], argparse.Namespace]:
    strategy, top, lookback, rebalance_day = trial
    trial_args = clone_args(args, strategy, top, lookback, rebalance_day)
    _trades, _equity, summary = run_backtest(trial_args, data, stock_data)
    passed = (
        abs(safe_float(summary["max_drawdown"], 0.0)) <= args.max_drawdown
        and safe_float(summary["annual_return"], 0.0) >= 0.08
        and safe_float(summary["calmar"], 0.0) >= 1.0
        and safe_float(summary["monthly_win_rate"], 0.0) >= 0.60
        and safe_float(summary["max_monthly_loss"], 0.0) >= -0.05
    )
    turnover_penalty = max(summary["trade_count"] / 1000.0, 0.0)
    rank_score = (
        summary["annual_return"]
        + summary["calmar"] * 0.05
        + summary["sharpe"] * 0.03
        + summary["monthly_win_rate"] * 0.05
        - abs(summary["max_drawdown"]) * 0.8
        - turnover_penalty * 0.05
    )
    row = {**summary, "run_id": run_id, "passed": passed, "rank_score": round(rank_score, 6)}
    return row, trial_args


def init_worker(args: argparse.Namespace, data: dict[str, pd.DataFrame], stock_data: dict[str, pd.DataFrame]) -> None:
    global WORKER_ARGS, WORKER_DATA, WORKER_STOCK_DATA
    WORKER_ARGS = args
    WORKER_DATA = data
    WORKER_STOCK_DATA = stock_data
    set_active_strategies(args.strategy_candidates)


def worker_trial(payload: tuple[str, tuple[str, int, int, int]]) -> tuple[dict[str, Any], argparse.Namespace]:
    run_id, trial = payload
    if WORKER_ARGS is None or WORKER_DATA is None:
        raise RuntimeError("Worker was not initialized.")
    return result_for_trial(WORKER_ARGS, WORKER_DATA, WORKER_STOCK_DATA, run_id, trial)


def optimize(args: argparse.Namespace, data: dict[str, pd.DataFrame], stock_data: dict[str, pd.DataFrame] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    trials = strategy_trials()
    rows = []
    best: dict[str, Any] | None = None
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir = os.path.join(RUNS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    setattr(args, "run_id", run_id)
    setattr(args, "run_dir", run_dir)
    fields = optimize_fields()
    write_csv(OPTIMIZE_FILE, rows, fields)
    write_csv(os.path.join(run_dir, "cb_strategy_search.csv"), rows, fields)
    copy_csv(STRATEGY_FILE, os.path.join(run_dir, "cb_strategy_definitions.csv"))
    copy_file(args.strategy_file, os.path.join(run_dir, "strategy_candidates.json"))
    copy_csv(TARGET_APPLIED_FILE, os.path.join(run_dir, "cb_target_constraints_applied.csv"))
    write_factor_definitions(os.path.join(run_dir, "cb_factor_definitions.csv"))

    total = len(trials)
    done = 0

    workers = max(1, int(args.workers or 1))
    print(f"optimize_workers={workers} trials={total}", flush=True)
    if workers == 1:
        iterator = (result_for_trial(args, data, stock_data, run_id, trial) for trial in trials)
        for row, trial_args in iterator:
            done += 1
            rows.append(row)
            if done == 1 or done % 20 == 0 or done == total:
                write_csv(OPTIMIZE_FILE, rows, fields)
                write_csv(os.path.join(run_dir, "cb_strategy_search.csv"), rows, fields)
            if done % 20 == 0 or done == total:
                print(f"optimize_progress={done}/{total}", flush=True)
            if row["passed"] and (best is None or row["rank_score"] > best["rank_score"]):
                best = {**row, "_args": trial_args}
    else:
        payloads = [(run_id, trial) for trial in trials]
        with ProcessPoolExecutor(max_workers=workers, initializer=init_worker, initargs=(args, data, stock_data or {})) as executor:
            futures = [executor.submit(worker_trial, payload) for payload in payloads]
            for future in as_completed(futures):
                row, trial_args = future.result()
                done += 1
                rows.append(row)
                if done == 1 or done % 20 == 0 or done == total:
                    write_csv(OPTIMIZE_FILE, rows, fields)
                    write_csv(os.path.join(run_dir, "cb_strategy_search.csv"), rows, fields)
                if done % 20 == 0 or done == total:
                    print(f"optimize_progress={done}/{total}", flush=True)
                if row["passed"] and (best is None or row["rank_score"] > best["rank_score"]):
                    best = {**row, "_args": trial_args}

    rows.sort(key=lambda row: (not row["passed"], -safe_float(row["rank_score"], 0.0)))
    return rows, best


def main() -> int:
    total_started_at = time.perf_counter()
    args = parse_args()
    strategies = load_strategy_candidates(args.strategy_file)
    set_active_strategies(strategies)
    args.strategy_candidates = strategies
    strategy_names = {item["name"] for item in strategies}
    if args.strategy not in strategy_names:
        raise ValueError(f"Strategy {args.strategy!r} is not in {args.strategy_file}. Available: {', '.join(sorted(strategy_names))}")
    print("=" * 72)
    print("MiniQMT convertible-bond backtest")
    print(f"range={args.start}-{args.end} limit={args.limit} top={args.top}")
    print(f"strategy_file={args.strategy_file} strategies={len(strategies)}")
    print(f"userdata={QMT_USER_DATA_PATH}")
    print("=" * 72, flush=True)

    bootstrap_xtquant()
    from xtquant import xtdata

    write_strategy_definitions(strategies)
    write_target_constraints()
    print(f"wrote {STRATEGY_FILE}")
    print(f"loaded strategies from {args.strategy_file}")
    print(f"wrote {TARGET_APPLIED_FILE}")

    codes = sorted(set(xtdata.get_stock_list_in_sector("\u6caa\u6df1\u8f6c\u503a") or []))
    codes = [code for code in codes if is_cb_code(code)]
    if args.limit > 0:
        codes = codes[: args.limit]
    print(f"bond_universe_size={len(codes)}", flush=True)
    if not codes:
        raise RuntimeError("MiniQMT returned no convertible-bond codes.")

    sync_started_at = time.perf_counter()
    conn = init_db()
    ensure_history_sqlite(xtdata, conn, codes, args.start, args.end)
    sync_cb_metadata(xtdata, conn, codes)
    metadata = load_metadata(conn, codes)
    stock_codes = sorted({meta.get("stock_code", "") for meta in metadata.values() if is_stock_code(meta.get("stock_code", ""))})
    if stock_codes:
        print(f"underlying_stock_count={len(stock_codes)}", flush=True)
        ensure_history_sqlite(xtdata, conn, stock_codes, args.start, args.end)
    print(f"sync_elapsed_seconds={time.perf_counter() - sync_started_at:.2f}", flush=True)

    load_started_at = time.perf_counter()
    data = load_data_from_sqlite(conn, codes, args.start, args.end, metadata)
    stock_data = load_data_from_sqlite(conn, stock_codes, args.start, args.end) if stock_codes else {}
    print(f"loaded_bonds={len(data)}", flush=True)
    print(f"loaded_underlying_stocks={len(stock_data)}", flush=True)
    print(f"load_elapsed_seconds={time.perf_counter() - load_started_at:.2f}", flush=True)

    if args.optimize:
        started_at = time.perf_counter()
        rows, best = optimize(args, data, stock_data)
        fields = optimize_fields()
        write_csv(OPTIMIZE_FILE, rows, fields)
        write_csv(BEST_BY_STRATEGY_FILE, best_by_strategy(rows), fields)
        run_dir = getattr(args, "run_dir", "")
        if run_dir:
            write_csv(os.path.join(run_dir, "cb_strategy_search.csv"), rows, fields)
            write_csv(os.path.join(run_dir, "cb_strategy_best_by_type.csv"), best_by_strategy(rows), fields)
        print(f"wrote {OPTIMIZE_FILE}")
        print(f"wrote {BEST_BY_STRATEGY_FILE}")
        print(f"optimize_elapsed_seconds={time.perf_counter() - started_at:.2f}", flush=True)
        if best is None:
            print(f"No candidate passed max_drawdown <= {args.max_drawdown:.2%}.", flush=True)
            return 2
        print("Best candidate under drawdown limit")
        for key in fields:
            print(f"  {key}: {best[key]}", flush=True)
        args = best["_args"]

    trades, equity, summary = run_backtest(args, data, stock_data)
    monthly = period_returns(equity, 6)
    yearly = period_returns(equity, 4)
    write_csv(TRADES_FILE, trades, ["date", "side", "code", "price", "volume", "amount", "fee", "reason"])
    write_csv(EQUITY_FILE, equity, ["date", "cash", "position_value", "total_value", "holding_count"])
    write_csv(SUMMARY_FILE, [summary], list(summary.keys()))
    write_csv(MONTHLY_FILE, monthly, ["period", "return"])
    write_csv(YEARLY_FILE, yearly, ["period", "return"])
    run_dir = getattr(args, "run_dir", "")
    if run_dir:
        write_csv(os.path.join(run_dir, "cb_backtest_trades.csv"), trades, ["date", "side", "code", "price", "volume", "amount", "fee", "reason"])
        write_csv(os.path.join(run_dir, "cb_backtest_equity.csv"), equity, ["date", "cash", "position_value", "total_value", "holding_count"])
        write_csv(os.path.join(run_dir, "cb_backtest_summary.csv"), [summary], list(summary.keys()))
        write_csv(os.path.join(run_dir, "cb_backtest_monthly_returns.csv"), monthly, ["period", "return"])
        write_csv(os.path.join(run_dir, "cb_backtest_yearly_returns.csv"), yearly, ["period", "return"])
        print(f"wrote run archive {run_dir}")

    print("Backtest summary")
    for key, value in summary.items():
        print(f"  {key}: {value}", flush=True)
    print(f"wrote {TRADES_FILE}")
    print(f"wrote {EQUITY_FILE}")
    print(f"wrote {SUMMARY_FILE}")
    print(f"wrote {MONTHLY_FILE}")
    print(f"wrote {YEARLY_FILE}")
    print(f"total_elapsed_seconds={time.perf_counter() - total_started_at:.2f}", flush=True)
    if summary["buy_count"] == 0 or summary["sell_count"] == 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
