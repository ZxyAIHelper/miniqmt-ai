from __future__ import annotations

import argparse
import csv
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

WORKER_DATA: dict[str, pd.DataFrame] | None = None
WORKER_ARGS: argparse.Namespace | None = None


STRATEGY_DEFINITIONS = {
    "balanced": "0.35*momentum + 0.25*low_volatility + 0.20*liquidity + 0.20*low_price",
    "low_vol": "low_volatility + 0.25*liquidity + 0.10*momentum",
    "low_price": "low_price + 0.25*low_volatility + 0.15*liquidity",
    "momentum": "momentum + 0.25*liquidity - 0.35*volatility",
    "reversal": "short_term_reversal + 0.20*low_volatility + 0.20*liquidity",
    "liquidity": "liquidity + 0.20*momentum - 0.20*volatility",
}


@dataclass
class Position:
    volume: int
    cost: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MiniQMT convertible-bond backtest with buy/sell trade records.")
    parser.add_argument("--start", default=three_years_ago())
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--limit", type=int, default=0, help="Convertible-bond universe size. 0 means full universe.")
    parser.add_argument("--strategy", default="balanced", choices=["momentum", "reversal", "low_vol", "low_price", "liquidity", "balanced"])
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


def load_data_from_sqlite(conn: sqlite3.Connection, codes: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:
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


def zscore(value: float, values: list[float]) -> float:
    if not values:
        return 0.0
    avg = sum(values) / len(values)
    var = sum((item - avg) * (item - avg) for item in values) / len(values)
    std = math.sqrt(var)
    if std <= 0:
        return 0.0
    return (value - avg) / std


def score_universe(data: dict[str, pd.DataFrame], date: pd.Timestamp, lookback: int, strategy: str) -> list[tuple[str, float]]:
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
        momentum = last_close / first_close - 1.0
        short_momentum = last_close / safe_float(hist["close"].tail(6).iloc[0], last_close) - 1.0
        liquidity = safe_float(hist["amount"].tail(5).mean(), 0.0)
        volatility = safe_float(returns.std(), 0.0)
        features.append({
            "code": code,
            "momentum": momentum,
            "short_momentum": short_momentum,
            "liquidity": liquidity,
            "volatility": volatility,
            "close": last_close,
        })

    if not features:
        return []

    momentum_values = [item["momentum"] for item in features]
    liquidity_values = [math.log1p(item["liquidity"]) for item in features]
    volatility_values = [item["volatility"] for item in features]
    close_values = [item["close"] for item in features]
    short_momentum_values = [item["short_momentum"] for item in features]

    scored = []
    for item in features:
        momentum_z = zscore(item["momentum"], momentum_values)
        short_reversal_z = -zscore(item["short_momentum"], short_momentum_values)
        liquidity_z = zscore(math.log1p(item["liquidity"]), liquidity_values)
        low_vol_z = -zscore(item["volatility"], volatility_values)
        low_price_z = -zscore(item["close"], close_values)

        if strategy == "momentum":
            score = momentum_z + 0.25 * liquidity_z - 0.35 * zscore(item["volatility"], volatility_values)
        elif strategy == "reversal":
            score = short_reversal_z + 0.2 * low_vol_z + 0.2 * liquidity_z
        elif strategy == "low_vol":
            score = low_vol_z + 0.25 * liquidity_z + 0.1 * momentum_z
        elif strategy == "low_price":
            score = low_price_z + 0.25 * low_vol_z + 0.15 * liquidity_z
        elif strategy == "liquidity":
            score = liquidity_z + 0.2 * momentum_z - 0.2 * zscore(item["volatility"], volatility_values)
        else:
            score = 0.35 * momentum_z + 0.25 * low_vol_z + 0.2 * liquidity_z + 0.2 * low_price_z

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


def write_strategy_definitions() -> None:
    rows = [{"strategy": name, "definition": definition} for name, definition in STRATEGY_DEFINITIONS.items()]
    write_csv(STRATEGY_FILE, rows, ["strategy", "definition"])


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


def run_backtest(args: argparse.Namespace, data: dict[str, pd.DataFrame]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
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

        selected = [code for code, _ in score_universe(data, date, args.lookback, args.strategy)[: args.top]]
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
    strategies = ["balanced", "low_vol", "low_price", "liquidity"]
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


def result_for_trial(args: argparse.Namespace, data: dict[str, pd.DataFrame], run_id: str, trial: tuple[str, int, int, int]) -> tuple[dict[str, Any], argparse.Namespace]:
    strategy, top, lookback, rebalance_day = trial
    trial_args = clone_args(args, strategy, top, lookback, rebalance_day)
    _trades, _equity, summary = run_backtest(trial_args, data)
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


def init_worker(args: argparse.Namespace, data: dict[str, pd.DataFrame]) -> None:
    global WORKER_ARGS, WORKER_DATA
    WORKER_ARGS = args
    WORKER_DATA = data


def worker_trial(payload: tuple[str, tuple[str, int, int, int]]) -> tuple[dict[str, Any], argparse.Namespace]:
    run_id, trial = payload
    if WORKER_ARGS is None or WORKER_DATA is None:
        raise RuntimeError("Worker was not initialized.")
    return result_for_trial(WORKER_ARGS, WORKER_DATA, run_id, trial)


def optimize(args: argparse.Namespace, data: dict[str, pd.DataFrame]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    trials = strategy_trials()
    rows = []
    best: dict[str, Any] | None = None
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    fields = optimize_fields()
    write_csv(OPTIMIZE_FILE, rows, fields)

    total = len(trials)
    done = 0

    workers = max(1, int(args.workers or 1))
    print(f"optimize_workers={workers} trials={total}", flush=True)
    if workers == 1:
        iterator = (result_for_trial(args, data, run_id, trial) for trial in trials)
        for row, trial_args in iterator:
            done += 1
            rows.append(row)
            if done == 1 or done % 20 == 0 or done == total:
                write_csv(OPTIMIZE_FILE, rows, fields)
            if done % 20 == 0 or done == total:
                print(f"optimize_progress={done}/{total}", flush=True)
            if row["passed"] and (best is None or row["rank_score"] > best["rank_score"]):
                best = {**row, "_args": trial_args}
    else:
        payloads = [(run_id, trial) for trial in trials]
        with ProcessPoolExecutor(max_workers=workers, initializer=init_worker, initargs=(args, data)) as executor:
            futures = [executor.submit(worker_trial, payload) for payload in payloads]
            for future in as_completed(futures):
                row, trial_args = future.result()
                done += 1
                rows.append(row)
                if done == 1 or done % 20 == 0 or done == total:
                    write_csv(OPTIMIZE_FILE, rows, fields)
                if done % 20 == 0 or done == total:
                    print(f"optimize_progress={done}/{total}", flush=True)
                if row["passed"] and (best is None or row["rank_score"] > best["rank_score"]):
                    best = {**row, "_args": trial_args}

    rows.sort(key=lambda row: (not row["passed"], -safe_float(row["rank_score"], 0.0)))
    return rows, best


def main() -> int:
    total_started_at = time.perf_counter()
    args = parse_args()
    print("=" * 72)
    print("MiniQMT convertible-bond backtest")
    print(f"range={args.start}-{args.end} limit={args.limit} top={args.top}")
    print(f"userdata={QMT_USER_DATA_PATH}")
    print("=" * 72, flush=True)

    bootstrap_xtquant()
    from xtquant import xtdata

    write_strategy_definitions()
    write_target_constraints()
    print(f"wrote {STRATEGY_FILE}")
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
    print(f"sync_elapsed_seconds={time.perf_counter() - sync_started_at:.2f}", flush=True)

    load_started_at = time.perf_counter()
    data = load_data_from_sqlite(conn, codes, args.start, args.end)
    print(f"loaded_bonds={len(data)}", flush=True)
    print(f"load_elapsed_seconds={time.perf_counter() - load_started_at:.2f}", flush=True)

    if args.optimize:
        started_at = time.perf_counter()
        rows, best = optimize(args, data)
        fields = optimize_fields()
        write_csv(OPTIMIZE_FILE, rows, fields)
        write_csv(BEST_BY_STRATEGY_FILE, best_by_strategy(rows), fields)
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

    trades, equity, summary = run_backtest(args, data)
    monthly = period_returns(equity, 6)
    yearly = period_returns(equity, 4)
    write_csv(TRADES_FILE, trades, ["date", "side", "code", "price", "volume", "amount", "fee", "reason"])
    write_csv(EQUITY_FILE, equity, ["date", "cash", "position_value", "total_value", "holding_count"])
    write_csv(SUMMARY_FILE, [summary], list(summary.keys()))
    write_csv(MONTHLY_FILE, monthly, ["period", "return"])
    write_csv(YEARLY_FILE, yearly, ["period", "return"])

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
