from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "qmt_outputs"
RUNS_DIR = OUTPUT_DIR / "runs"
AI_DIR = OUTPUT_DIR / "ai_reviews"
HISTORY_FILE = OUTPUT_DIR / "ai_strategy_iterations.jsonl"
DEFAULT_STRATEGY_FILE = BASE_DIR / "strategy_candidates.json"
BACKTEST_SCRIPT = BASE_DIR / "miniqmt_cb_backtest.py"

DEFENSIVE_FACTORS = {
    "price_band": 0.06,
    "low_volatility": 0.06,
    "low_downside_volatility": 0.06,
    "tail_loss_control": 0.05,
    "drawdown_control": 0.05,
    "low_amplitude": 0.04,
    "low_gap_risk": 0.04,
    "liquidity_stability": 0.03,
}

RETURN_FACTORS = {
    "momentum": 0.05,
    "trend_filter": 0.05,
    "up_day_consistency": 0.04,
    "price_position": 0.04,
    "liquidity": 0.03,
    "liquidity_trend": 0.03,
}

WEAK_OR_UNAVAILABLE_FACTORS = {
    "stock_momentum",
    "stock_volatility",
    "conversion_premium",
    "conversion_value",
    "double_low",
    "ytm",
    "rating_score",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iteratively run MiniQMT CB optimization and evolve strategy candidates.")
    parser.add_argument("--rounds", type=int, default=0, help="Manual round cap. 0 means keep running until the AI stop decision.")
    parser.add_argument("--safety-max-rounds", type=int, default=100, help="Emergency guard for --rounds 0 to avoid accidental endless execution.")
    parser.add_argument("--patience", type=int, default=2, help="Stop after this many rounds without meaningful improvement.")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=20, help="Convertible-bond universe limit for each round. 0 means full universe.")
    parser.add_argument("--strategy-file", default=str(DEFAULT_STRATEGY_FILE))
    parser.add_argument("--min-improvement", type=float, default=0.01, help="Minimum rank-score improvement considered meaningful.")
    parser.add_argument("--max-strategies", type=int, default=32, help="Maximum strategies kept for the next round.")
    parser.add_argument("--dry-run-ai", action="store_true", help="Analyze latest run and propose strategies without launching backtest.")
    return parser.parse_args()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        if not math.isfinite(number):
            return default
        return number
    except Exception:
        return default


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def latest_run_dir() -> Path | None:
    if not RUNS_DIR.exists():
        return None
    dirs = [path for path in RUNS_DIR.iterdir() if path.is_dir()]
    return max(dirs, key=lambda path: path.stat().st_mtime) if dirs else None


def run_backtest_round(args: argparse.Namespace) -> tuple[int, Path | None]:
    before = latest_run_dir()
    command = [
        sys.executable,
        str(BACKTEST_SCRIPT),
        "--optimize",
        "--strategy-file",
        str(Path(args.strategy_file).resolve()),
        "--workers",
        str(args.workers),
        "--limit",
        str(args.limit),
    ]
    print("launch_round_command=" + " ".join(command), flush=True)
    completed = subprocess.run(command, cwd=str(BASE_DIR))
    after = latest_run_dir()
    if after is not None and after != before:
        return completed.returncode, after
    return completed.returncode, after


def strategy_map(strategy_file: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(strategy_file)
    return {item["name"]: item for item in payload.get("strategies", [])}


def sorted_results(run_dir: Path) -> list[dict[str, str]]:
    rows = read_csv(run_dir / "cb_strategy_search.csv")
    rows.sort(key=lambda row: safe_float(row.get("rank_score"), -999.0), reverse=True)
    return rows


def summarize_results(rows: list[dict[str, str]]) -> dict[str, Any]:
    top_rows = rows[:12]
    passed = [row for row in rows if str(row.get("passed")).lower() == "true"]
    best = top_rows[0] if top_rows else {}
    return {
        "tested": len(rows),
        "passed": len(passed),
        "best_strategy": best.get("strategy", ""),
        "best_rank_score": safe_float(best.get("rank_score"), -999.0),
        "best_annual_return": safe_float(best.get("annual_return"), 0.0),
        "best_max_drawdown": safe_float(best.get("max_drawdown"), 0.0),
        "best_calmar": safe_float(best.get("calmar"), 0.0),
        "best_monthly_win_rate": safe_float(best.get("monthly_win_rate"), 0.0),
        "top": [
            {
                "strategy": row.get("strategy"),
                "rank_score": safe_float(row.get("rank_score")),
                "annual_return": safe_float(row.get("annual_return")),
                "max_drawdown": safe_float(row.get("max_drawdown")),
                "calmar": safe_float(row.get("calmar")),
                "monthly_win_rate": safe_float(row.get("monthly_win_rate")),
                "top": int(safe_float(row.get("top"), 0)),
                "lookback": int(safe_float(row.get("lookback"), 0)),
                "rebalance_days": int(safe_float(row.get("rebalance_days"), 0)),
            }
            for row in top_rows
        ],
    }


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {factor: max(0.0, safe_float(weight)) for factor, weight in weights.items()}
    cleaned = {factor: weight for factor, weight in cleaned.items() if weight > 0.005}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {factor: round(weight / total, 4) for factor, weight in sorted(cleaned.items())}


def adjusted_weights(base: dict[str, float], additions: dict[str, float], cuts: set[str]) -> dict[str, float]:
    weights = {factor: safe_float(weight) for factor, weight in base.items() if factor not in cuts}
    for factor, delta in additions.items():
        weights[factor] = weights.get(factor, 0.0) + delta
    return normalize_weights(weights)


def signature(weights: dict[str, float]) -> str:
    return "|".join(f"{factor}:{weight:.4f}" for factor, weight in sorted(weights.items()))


def generate_candidates(current: dict[str, dict[str, Any]], summary: dict[str, Any], round_index: int, max_strategies: int) -> list[dict[str, Any]]:
    existing_signatures = {signature(item.get("weights", {})) for item in current.values()}
    kept = list(current.values())
    proposals = []

    for rank, row in enumerate(summary.get("top", [])[:8], 1):
        name = row["strategy"]
        base = current.get(name)
        if not base:
            continue
        base_weights = base.get("weights", {})
        drawdown = abs(safe_float(row.get("max_drawdown")))
        annual = safe_float(row.get("annual_return"))
        if drawdown > 0.15:
            additions = DEFENSIVE_FACTORS
            style = "defensive"
            cuts = {"momentum", "liquidity_trend", "volume_trend"}
        elif annual < 0.08:
            additions = RETURN_FACTORS
            style = "return"
            cuts = {"rating_score", "ytm"}
        else:
            additions = {**DEFENSIVE_FACTORS, **RETURN_FACTORS}
            style = "balanced"
            cuts = set()
        candidate_weights = adjusted_weights(base_weights, additions, cuts | WEAK_OR_UNAVAILABLE_FACTORS)
        candidate_signature = signature(candidate_weights)
        if not candidate_weights or candidate_signature in existing_signatures:
            continue
        existing_signatures.add(candidate_signature)
        proposals.append({
            "name": f"ai_r{round_index}_{style}_{rank}_{name}"[:64],
            "weights": candidate_weights,
            "description": f"AI iteration {round_index}: {style} variant based on {name}.",
        })

    next_pool = kept + proposals
    if len(next_pool) > max_strategies:
        original_names = set(current)
        proposed = [item for item in next_pool if item["name"] not in original_names]
        originals = [item for item in next_pool if item["name"] in original_names]
        next_pool = originals[: max(8, max_strategies - len(proposed))] + proposed
        next_pool = next_pool[-max_strategies:]
    return next_pool


def previous_history() -> list[dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    rows = []
    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def should_stop(history: list[dict[str, Any]], summary: dict[str, Any], patience: int, min_improvement: float, proposed_count: int) -> tuple[bool, str]:
    if proposed_count <= 0:
        return True, "No materially new strategy weights were generated."
    if summary.get("passed", 0) > 0:
        return True, "At least one strategy passed the hard constraints; stop for human review before further search."
    if len(history) < patience:
        return False, "Need more rounds before judging plateau."
    previous_best = max(safe_float(item.get("summary", {}).get("best_rank_score"), -999.0) for item in history[-patience:])
    current_best = safe_float(summary.get("best_rank_score"), -999.0)
    if current_best <= previous_best + min_improvement:
        return True, f"Best rank_score improved by less than {min_improvement} over the last {patience} rounds."
    return False, "Improvement is still meaningful enough to continue."


def write_prompt(run_dir: Path, summary: dict[str, Any], strategy_file: Path, decision: dict[str, Any]) -> Path:
    prompt_path = AI_DIR / f"{datetime.now().strftime('%Y%m%d%H%M%S')}_codex_prompt.md"
    lines = [
        "# Convertible Bond Strategy Iteration",
        "",
        "You are Codex reviewing a MiniQMT convertible-bond strategy search round.",
        "Decide whether to continue and, if continuing, propose new factor-weight strategies as JSON.",
        "",
        f"Run dir: {run_dir}",
        f"Strategy file: {strategy_file}",
        "",
        "## Summary",
        "```json",
        json.dumps(summary, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Current Decision",
        "```json",
        json.dumps(decision, ensure_ascii=False, indent=2),
        "```",
    ]
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text("\n".join(lines), encoding="utf-8")
    return prompt_path


def analyze_and_update(args: argparse.Namespace, run_dir: Path, round_index: int) -> tuple[bool, dict[str, Any]]:
    strategy_file = Path(args.strategy_file).resolve()
    current = strategy_map(strategy_file)
    rows = sorted_results(run_dir)
    summary = summarize_results(rows)
    next_strategies = generate_candidates(current, summary, round_index, args.max_strategies)
    proposed_count = max(0, len(next_strategies) - len(current))
    history = previous_history()
    stop, reason = should_stop(history, summary, args.patience, args.min_improvement, proposed_count)
    decision = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "round_index": round_index,
        "run_dir": str(run_dir),
        "summary": summary,
        "proposed_count": proposed_count,
        "stop": stop,
        "reason": reason,
    }
    prompt_path = write_prompt(run_dir, summary, strategy_file, decision)
    decision["codex_prompt"] = str(prompt_path)

    AI_DIR.mkdir(parents=True, exist_ok=True)
    write_json(AI_DIR / f"{run_dir.name}_decision.json", decision)
    append_jsonl(HISTORY_FILE, decision)

    if not stop:
        payload = {
            "version": 1,
            "notes": f"Updated by strategy_iteration_loop.py after run {run_dir.name}.",
            "strategies": next_strategies,
        }
        write_json(strategy_file, payload)
        print(f"ai_updated_strategy_file={strategy_file} strategies={len(next_strategies)} proposed={proposed_count}", flush=True)
    else:
        print(f"ai_stop_reason={reason}", flush=True)
    print(f"ai_decision_file={AI_DIR / f'{run_dir.name}_decision.json'}", flush=True)
    print(f"codex_prompt_file={prompt_path}", flush=True)
    return stop, decision


def main() -> int:
    args = parse_args()
    if args.dry_run_ai:
        run_dir = latest_run_dir()
        if run_dir is None:
            raise RuntimeError("No archived run is available for dry-run AI analysis.")
        stop, _decision = analyze_and_update(args, run_dir, 1)
        return 0 if stop else 1

    round_cap = args.rounds if args.rounds > 0 else args.safety_max_rounds
    round_label = str(args.rounds) if args.rounds > 0 else "AI_STOP"
    for round_index in range(1, round_cap + 1):
        print(f"iteration_round={round_index}/{round_label}", flush=True)
        returncode, run_dir = run_backtest_round(args)
        print(f"round_returncode={returncode}", flush=True)
        if run_dir is None:
            raise RuntimeError("Backtest did not create or update an archived run directory.")
        stop, _decision = analyze_and_update(args, run_dir, round_index)
        if stop:
            return 0
    if args.rounds > 0:
        print("ai_stop_reason=Reached manual round cap before AI stopped.", flush=True)
    else:
        print("ai_stop_reason=Reached safety max rounds before AI stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
