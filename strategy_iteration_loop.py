from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
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
REVIEW_POLICY_FILE = OUTPUT_DIR / "ai_review_policy.json"
DEFAULT_STRATEGY_FILE = BASE_DIR / "strategy_candidates.json"
BACKTEST_SCRIPT = BASE_DIR / "miniqmt_cb_backtest.py"
CODEX_DECISION_SCHEMA = BASE_DIR / "codex_strategy_decision.schema.json"

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

DEFAULT_TOP_VALUES = [3, 5, 8]
DEFAULT_LOOKBACK_VALUES = [40, 60, 90]
DEFAULT_REBALANCE_VALUES = [10, 20, 40]


def parse_args_from(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iteratively run MiniQMT CB optimization and evolve strategy candidates.")
    parser.add_argument("--rounds", type=int, default=0, help="Manual round cap. 0 means keep running until the AI stop decision.")
    parser.add_argument("--safety-max-rounds", type=int, default=100, help="Emergency guard for --rounds 0 to avoid accidental endless execution.")
    parser.add_argument("--patience", type=int, default=2, help="Stop after this many rounds without meaningful improvement.")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=20, help="Convertible-bond universe limit for each round. 0 means full universe.")
    parser.add_argument("--strategy-file", default=str(DEFAULT_STRATEGY_FILE))
    parser.add_argument("--min-improvement", type=float, default=0.01, help="Minimum rank-score improvement considered meaningful.")
    parser.add_argument("--max-strategies", type=int, default=32, help="Maximum strategies kept for the next round.")
    parser.add_argument("--ai-provider", choices=["codex", "heuristic"], default="codex", help="Decision agent used after each backtest round.")
    parser.add_argument("--codex-command", default="codex", help="Codex CLI command.")
    parser.add_argument("--codex-model", default="", help="Optional model name passed to codex exec.")
    parser.add_argument("--codex-timeout", type=int, default=900, help="Seconds to wait for the Codex decision agent.")
    parser.add_argument("--ignore-history", action="store_true", help="Do not include prior AI iteration history in the decision prompt.")
    parser.add_argument("--review-policy-file", default=str(REVIEW_POLICY_FILE), help="JSON file where the AI stores when it wants to review partial backtest results.")
    parser.add_argument("--dry-run-ai", action="store_true", help="Analyze latest run and propose strategies without launching backtest.")
    values = argv[1:] if argv is not None else None
    return parser.parse_args(values)


def parse_args() -> argparse.Namespace:
    return parse_args_from()


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


def parse_json_text(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
        raise


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
    policy = load_review_policy(Path(args.review_policy_file))
    stop_after_trials = review_policy_stop_after(policy)
    command = backtest_command(args, stop_after_trials)
    print("launch_round_command=" + " ".join(command), flush=True)
    if stop_after_trials:
        print(f"ai_review_checkpoint=after_trials={stop_after_trials} reason={policy.get('reason', '')}", flush=True)
    completed = subprocess.run(command, cwd=str(BASE_DIR))
    after = latest_run_dir()
    if after is not None and after != before:
        return completed.returncode, after
    return completed.returncode, after


def backtest_command(args: argparse.Namespace, stop_after_trials: int = 0) -> list[str]:
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
    if stop_after_trials > 0:
        command.extend(["--stop-after-trials", str(stop_after_trials)])
    return command


def normalize_review_policy(policy: Any) -> dict[str, Any]:
    if not isinstance(policy, dict):
        policy = {}
    mode = str(policy.get("mode", "full_round")).strip()
    if mode not in {"full_round", "after_n_trials"}:
        mode = "full_round"
    min_completed = int(max(20, min(2000, safe_float(policy.get("min_completed_trials"), 0))))
    every = int(max(20, min(2000, safe_float(policy.get("review_every_trials"), min_completed))))
    return {
        "mode": mode,
        "min_completed_trials": min_completed,
        "review_every_trials": every,
        "reason": str(policy.get("reason", "")).strip(),
    }


def review_policy_stop_after(policy: dict[str, Any]) -> int:
    if policy.get("mode") != "after_n_trials":
        return 0
    return int(policy.get("min_completed_trials", 0) or 0)


def load_review_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"mode": "full_round", "min_completed_trials": 0, "review_every_trials": 0, "reason": "No AI checkpoint policy yet."}
    return normalize_review_policy(read_json(path))


def strategy_map(strategy_file: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(strategy_file)
    return {item["name"]: item for item in payload.get("strategies", [])}


def factor_definitions(run_dir: Path) -> dict[str, str]:
    rows = read_csv(run_dir / "cb_factor_definitions.csv")
    return {row.get("factor", ""): row.get("description", "") for row in rows if row.get("factor")}


def sorted_results(run_dir: Path) -> list[dict[str, str]]:
    rows = read_csv(run_dir / "cb_strategy_search.csv")
    rows.sort(key=lambda row: safe_float(row.get("rank_score"), -999.0), reverse=True)
    return rows


def summarize_results(rows: list[dict[str, str]]) -> dict[str, Any]:
    top_rows = rows[:12]
    passed = [row for row in rows if str(row.get("passed")).lower() == "true"]
    best = top_rows[0] if top_rows else {}
    parameter_analysis = {
        "top": summarize_parameter(rows, "top"),
        "lookback": summarize_parameter(rows, "lookback"),
        "rebalance_days": summarize_parameter(rows, "rebalance_days"),
    }
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
        "parameter_analysis": parameter_analysis,
        "failure_modes": summarize_failure_modes(rows),
    }


def summarize_parameter(rows: list[dict[str, str]], field: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        key = str(int(safe_float(row.get(field), 0)))
        groups.setdefault(key, []).append(row)
    summary = {}
    for key, items in sorted(groups.items(), key=lambda item: int(item[0])):
        passed = [row for row in items if str(row.get("passed")).lower() == "true"]
        summary[key] = {
            "tested": len(items),
            "passed": len(passed),
            "avg_rank_score": round(sum(safe_float(row.get("rank_score"), -999.0) for row in items) / len(items), 6),
            "avg_annual_return": round(sum(safe_float(row.get("annual_return")) for row in items) / len(items), 6),
            "avg_max_drawdown": round(sum(safe_float(row.get("max_drawdown")) for row in items) / len(items), 6),
            "avg_monthly_win_rate": round(sum(safe_float(row.get("monthly_win_rate")) for row in items) / len(items), 6),
        }
    return summary


def summarize_failure_modes(rows: list[dict[str, str]]) -> dict[str, int]:
    modes = {
        "drawdown_over_20pct": 0,
        "annual_return_below_8pct": 0,
        "calmar_below_1": 0,
        "monthly_win_rate_below_60pct": 0,
        "max_monthly_loss_below_minus_5pct": 0,
    }
    for row in rows:
        if abs(safe_float(row.get("max_drawdown"))) > 0.20:
            modes["drawdown_over_20pct"] += 1
        if safe_float(row.get("annual_return")) < 0.08:
            modes["annual_return_below_8pct"] += 1
        if safe_float(row.get("calmar")) < 1.0:
            modes["calmar_below_1"] += 1
        if safe_float(row.get("monthly_win_rate")) < 0.60:
            modes["monthly_win_rate_below_60pct"] += 1
        if safe_float(row.get("max_monthly_loss")) < -0.05:
            modes["max_monthly_loss_below_minus_5pct"] += 1
    return modes


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {factor: max(0.0, safe_float(weight)) for factor, weight in weights.items()}
    cleaned = {factor: weight for factor, weight in cleaned.items() if weight > 0.005}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {factor: round(weight / total, 4) for factor, weight in sorted(cleaned.items())}


def normalize_parameter_values(raw_values: Any, allowed: list[int], default_values: list[int]) -> list[int]:
    if raw_values is None:
        return list(default_values)
    if not isinstance(raw_values, list):
        raw_values = [raw_values]
    values = []
    for raw_value in raw_values:
        try:
            value = int(float(raw_value))
        except Exception:
            continue
        if value in allowed and value not in values:
            values.append(value)
    return values or list(default_values)


def normalize_parameter_grid(grid: Any) -> dict[str, list[int]]:
    if not isinstance(grid, dict):
        return {
            "top": DEFAULT_TOP_VALUES,
            "lookback": DEFAULT_LOOKBACK_VALUES,
            "rebalance_days": DEFAULT_REBALANCE_VALUES,
        }
    return {
        "top": normalize_parameter_values(grid.get("top"), DEFAULT_TOP_VALUES, DEFAULT_TOP_VALUES),
        "lookback": normalize_parameter_values(grid.get("lookback"), DEFAULT_LOOKBACK_VALUES, DEFAULT_LOOKBACK_VALUES),
        "rebalance_days": normalize_parameter_values(grid.get("rebalance_days"), DEFAULT_REBALANCE_VALUES, DEFAULT_REBALANCE_VALUES),
    }


def validate_agent_strategies(strategies: Any, allowed_factors: set[str], existing_names: set[str]) -> list[dict[str, Any]]:
    if not isinstance(strategies, list):
        raise ValueError("Agent decision field 'strategies' must be a list.")
    cleaned = []
    seen = set(existing_names)
    for index, item in enumerate(strategies, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Agent strategy #{index} is not an object.")
        raw_name = str(item.get("name", "")).strip()
        if not raw_name:
            raise ValueError(f"Agent strategy #{index} has no name.")
        name = "".join(ch if ch.isalnum() or ch in ["_", "-"] else "_" for ch in raw_name)[:64]
        base_name = name
        suffix = 2
        while name in seen:
            name = f"{base_name[:58]}_{suffix}"
            suffix += 1
        weights = item.get("weights", {})
        if isinstance(weights, list):
            weights = {
                str(weight_item.get("factor", "")).strip(): weight_item.get("weight")
                for weight_item in weights
                if isinstance(weight_item, dict)
            }
        if not isinstance(weights, dict) or not weights:
            raise ValueError(f"Agent strategy {raw_name} has no weights.")
        cleaned_weights = {}
        for factor, weight in weights.items():
            factor_name = str(factor).strip()
            if factor_name not in allowed_factors:
                raise ValueError(f"Agent strategy {raw_name} uses unknown factor: {factor_name}")
            factor_weight = safe_float(weight, math.nan)
            if not math.isfinite(factor_weight) or factor_weight <= 0:
                raise ValueError(f"Agent strategy {raw_name} has invalid weight for {factor_name}: {weight}")
            cleaned_weights[factor_name] = factor_weight
        normalized = normalize_weights(cleaned_weights)
        if not normalized:
            raise ValueError(f"Agent strategy {raw_name} has no usable positive weights.")
        seen.add(name)
        cleaned_item = {
            "name": name,
            "weights": normalized,
            "description": str(item.get("description", "")).strip() or f"Codex generated strategy {name}.",
            "parameter_grid": normalize_parameter_grid(item.get("parameter_grid")),
        }
        research_thesis = str(item.get("research_thesis", "")).strip()
        if research_thesis:
            cleaned_item["research_thesis"] = research_thesis
        cleaned.append(cleaned_item)
    return cleaned


def merge_strategy_pool(current: dict[str, dict[str, Any]], proposed: list[dict[str, Any]], max_strategies: int) -> list[dict[str, Any]]:
    combined = list(current.values()) + proposed
    signatures = set()
    deduped = []
    for item in combined:
        item_signature = signature(item.get("weights", {}))
        if item_signature in signatures:
            continue
        signatures.add(item_signature)
        deduped.append(item)
    if len(deduped) <= max_strategies:
        return deduped
    original_names = set(current)
    originals = [item for item in deduped if item["name"] in original_names]
    new_items = [item for item in deduped if item["name"] not in original_names]
    kept_originals = originals[-max(0, max_strategies - len(new_items)) :]
    return (kept_originals + new_items)[-max_strategies:]


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
        if drawdown > 0.20:
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


def build_agent_prompt(run_dir: Path, strategy_file: Path, summary: dict[str, Any], current: dict[str, dict[str, Any]], history: list[dict[str, Any]]) -> str:
    factors = factor_definitions(run_dir)
    recent_history = [
        {
            "run_dir": item.get("run_dir"),
            "best_rank_score": item.get("summary", {}).get("best_rank_score"),
            "best_strategy": item.get("summary", {}).get("best_strategy"),
            "passed": item.get("summary", {}).get("passed"),
            "stop": item.get("stop"),
            "reason": item.get("reason"),
        }
        for item in history[-6:]
    ]
    current_strategies = list(current.values())
    return "\n".join([
        "# Convertible Bond Strategy Agent",
        "",
        "You are a Codex strategy research agent for a MiniQMT convertible-bond backtest loop.",
        "Your job is to act like a quantitative research expert: diagnose historical backtest results, decide whether the search should continue, and, only if useful, propose new factor-weight strategies with strategy-specific parameter grids.",
        "",
        "Return exactly the JSON object required by the provided output schema.",
        "",
        "Rules:",
        "- Do not edit files or run commands.",
        "- Set stop=true when the history suggests further factor-weight tweaks are unlikely to add value.",
        "- Set stop=false only when you can propose genuinely different strategy candidates backed by a research thesis.",
        "- Use only factors listed in available_factors.",
        "- Each strategy should use 3 to 8 factors with positive weights. Return weights as an array of {factor, weight}; the controller will normalize them.",
        "- Each proposed strategy must include parameter_grid with top, lookback, and rebalance_days arrays chosen from top=[3,5,8], lookback=[40,60,90], rebalance_days=[10,20,40].",
        "- Each proposed strategy should include research_thesis explaining why those factors and parameters are worth testing.",
        "- Include diagnosis, avoid, and focus fields in your response: diagnosis explains what historical backtest results imply; avoid lists weak factors or parameter regions; focus lists the next research direction.",
        "- Include review_policy to decide when you should inspect the next round. Use mode=after_n_trials with a professional checkpoint size when partial evidence is enough, or mode=full_round when the whole planned set is needed.",
        "- Avoid factors marked unavailable_or_weak unless the results strongly justify them.",
        "- Prefer a small number of high-quality new strategies, usually 1 to 5.",
        "- Do not brute-force the full grid by default. Use historical backtest results to narrow or change the next tests.",
        "- Consider max drawdown, annual return, Calmar, monthly win rate, max monthly loss, trade count, parameter behavior, factor behavior, and repeated lack of improvement.",
        "",
        "Context JSON:",
        "```json",
        json.dumps({
            "run_dir": str(run_dir),
            "strategy_file": str(strategy_file),
            "available_factors": factors,
            "unavailable_or_weak_factors": sorted(WEAK_OR_UNAVAILABLE_FACTORS),
            "latest_summary": summary,
            "recent_iteration_history": recent_history,
            "current_strategy_count": len(current_strategies),
            "current_strategies": current_strategies,
        }, ensure_ascii=False, indent=2),
        "```",
    ])


def write_prompt(run_dir: Path, prompt_text: str) -> Path:
    prompt_path = AI_DIR / f"{datetime.now().strftime('%Y%m%d%H%M%S')}_codex_prompt.md"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt_text, encoding="utf-8")
    return prompt_path


def call_codex_agent(args: argparse.Namespace, prompt_text: str, run_dir: Path) -> dict[str, Any]:
    output_path = AI_DIR / f"{run_dir.name}_codex_response.json"
    stderr_path = AI_DIR / f"{run_dir.name}_codex_stderr.txt"
    codex_command = args.codex_command
    if os.name == "nt" and not Path(codex_command).suffix:
        codex_command = shutil.which(f"{codex_command}.cmd") or shutil.which(codex_command) or codex_command
    command = [
        codex_command,
        "-a",
        "never",
        "exec",
        "--cd",
        str(BASE_DIR),
        "--sandbox",
        "read-only",
        "--output-schema",
        str(CODEX_DECISION_SCHEMA),
        "--output-last-message",
        str(output_path),
        "-",
    ]
    if args.codex_model:
        command[2:2] = ["--model", args.codex_model]
    AI_DIR.mkdir(parents=True, exist_ok=True)
    print("launch_codex_agent=" + " ".join(command), flush=True)
    completed = subprocess.run(
        command,
        cwd=str(BASE_DIR),
        input=prompt_text,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=args.codex_timeout,
    )
    stderr_path.write_text((completed.stdout or "") + "\n--- STDERR ---\n" + (completed.stderr or ""), encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"Codex agent failed with exit code {completed.returncode}. See {stderr_path}")
    if not output_path.exists():
        raise RuntimeError(f"Codex agent did not write {output_path}")
    decision = parse_json_text(output_path.read_text(encoding="utf-8"))
    if not isinstance(decision.get("stop"), bool):
        raise ValueError("Codex decision must contain boolean stop.")
    if not isinstance(decision.get("reason"), str):
        raise ValueError("Codex decision must contain string reason.")
    if "strategies" not in decision:
        raise ValueError("Codex decision must contain strategies.")
    return decision


def heuristic_decision(current: dict[str, dict[str, Any]], summary: dict[str, Any], history: list[dict[str, Any]], args: argparse.Namespace, round_index: int) -> dict[str, Any]:
    next_strategies = generate_candidates(current, summary, round_index, args.max_strategies)
    proposed_count = max(0, len(next_strategies) - len(current))
    stop, reason = should_stop(history, summary, args.patience, args.min_improvement, proposed_count)
    proposed_names = set(item["name"] for item in next_strategies) - set(current)
    return {
        "stop": stop,
        "reason": reason,
        "diagnosis": "Heuristic fallback adjusted weights from recent top results; use Codex provider for full historical parameter attribution.",
        "avoid": {},
        "focus": {"source": "heuristic", "note": "Generated from best recent rows and hard-constraint failures."},
        "review_policy": {"mode": "full_round", "min_completed_trials": 0, "review_every_trials": 0, "reason": "Heuristic fallback does not choose partial-review checkpoints."},
        "strategies": [item for item in next_strategies if item["name"] in proposed_names],
    }


def analyze_and_update(args: argparse.Namespace, run_dir: Path, round_index: int) -> tuple[bool, dict[str, Any]]:
    strategy_file = Path(args.strategy_file).resolve()
    current = strategy_map(strategy_file)
    rows = sorted_results(run_dir)
    summary = summarize_results(rows)
    history = [] if args.ignore_history else previous_history()
    prompt_text = build_agent_prompt(run_dir, strategy_file, summary, current, history)
    prompt_path = write_prompt(run_dir, prompt_text)
    if args.ai_provider == "codex":
        agent_response = call_codex_agent(args, prompt_text, run_dir)
    else:
        agent_response = heuristic_decision(current, summary, history, args, round_index)

    allowed_factors = set(factor_definitions(run_dir))
    proposed = validate_agent_strategies(agent_response.get("strategies", []), allowed_factors, set(current))
    proposed_count = len(proposed)
    stop = bool(agent_response.get("stop"))
    reason = str(agent_response.get("reason", "")).strip()
    if not reason:
        reason = "Agent did not provide a reason."
    if not stop and proposed_count <= 0:
        stop = True
        reason = "Agent chose to continue but did not provide any valid new strategies."
    diagnosis = str(agent_response.get("diagnosis", "")).strip()
    avoid = agent_response.get("avoid", {})
    focus = agent_response.get("focus", {})
    review_policy = normalize_review_policy(agent_response.get("review_policy", {}))
    next_strategies = merge_strategy_pool(current, proposed, args.max_strategies)
    print(
        "ai_round_summary="
        f"tested={summary.get('tested')} passed={summary.get('passed')} "
        f"best={summary.get('best_strategy')} "
        f"best_annual={summary.get('best_annual_return'):.6f} "
        f"best_drawdown={summary.get('best_max_drawdown'):.6f} "
        f"history_items={len(history)}",
        flush=True,
    )
    print(f"ai_agent_decision=stop={stop} proposed={proposed_count} reason={reason}", flush=True)
    if diagnosis:
        print(f"ai_research_diagnosis={diagnosis}", flush=True)
    print(
        "ai_review_policy="
        f"mode={review_policy.get('mode')} "
        f"min_completed_trials={review_policy.get('min_completed_trials')} "
        f"review_every_trials={review_policy.get('review_every_trials')} "
        f"reason={review_policy.get('reason')}",
        flush=True,
    )
    for item in proposed:
        weights = ", ".join(f"{factor}:{weight}" for factor, weight in item.get("weights", {}).items())
        grid = item.get("parameter_grid", {})
        print(f"ai_proposed_strategy={item.get('name')} grid={grid} weights={weights} description={item.get('description', '')}", flush=True)
    decision = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "round_index": round_index,
        "ai_provider": args.ai_provider,
        "run_dir": str(run_dir),
        "summary": summary,
        "proposed_count": proposed_count,
        "stop": stop,
        "reason": reason,
        "diagnosis": diagnosis,
        "avoid": avoid,
        "focus": focus,
        "review_policy": review_policy,
        "agent_response": agent_response,
    }
    decision["codex_prompt"] = str(prompt_path)

    AI_DIR.mkdir(parents=True, exist_ok=True)
    write_json(AI_DIR / f"{run_dir.name}_decision.json", decision)
    append_jsonl(HISTORY_FILE, decision)
    write_json(Path(args.review_policy_file), review_policy)

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
        print(f"dry_run_ai_stop={stop}", flush=True)
        return 0

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
