import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import miniqmt_cb_backtest
import qmt_dashboard
import strategy_iteration_loop


class DrawdownTargetTests(unittest.TestCase):
    def test_default_optimize_drawdown_limit_is_twenty_percent(self):
        with patch.object(sys, "argv", ["miniqmt_cb_backtest.py"]):
            args = miniqmt_cb_backtest.parse_args()

        self.assertEqual(args.max_drawdown, 0.20)

    def test_written_target_constraints_use_twenty_percent_drawdown_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            target_path = Path(tmp) / "constraints.csv"
            with patch.object(miniqmt_cb_backtest, "TARGET_APPLIED_FILE", str(target_path)):
                miniqmt_cb_backtest.write_target_constraints()

            with target_path.open("r", encoding="utf-8-sig", newline="") as f:
                rows = {row["constraint"]: row["value"] for row in csv.DictReader(f)}

        self.assertEqual(rows["max_drawdown_max"], "0.20")

    def test_ai_candidate_generation_treats_eighteen_percent_drawdown_as_allowed(self):
        current = {
            "sample": {
                "name": "sample",
                "weights": {"price_band": 0.5, "trend_filter": 0.5},
            }
        }
        summary = {
            "top": [
                {
                    "strategy": "sample",
                    "max_drawdown": -0.18,
                    "annual_return": 0.12,
                }
            ]
        }

        candidates = strategy_iteration_loop.generate_candidates(current, summary, 1, 10)

        self.assertTrue(any(item["name"].startswith("ai_r1_balanced_") for item in candidates))
        self.assertFalse(any(item["name"].startswith("ai_r1_defensive_") for item in candidates))

    def test_strategy_iteration_can_ignore_stale_history(self):
        with patch.object(sys, "argv", ["strategy_iteration_loop.py", "--ignore-history"]):
            args = strategy_iteration_loop.parse_args()

        self.assertTrue(args.ignore_history)

    def test_dashboard_contains_ai_decision_panel(self):
        self.assertIn('id="aiDecision"', qmt_dashboard.HTML)
        self.assertIn("renderAIDecision", qmt_dashboard.HTML)

    def test_optimize_mode_does_not_require_default_single_strategy(self):
        args = miniqmt_cb_backtest.parse_args_from([
            "miniqmt_cb_backtest.py",
            "--optimize",
        ])
        strategy_names = {"only_ai_strategy"}

        miniqmt_cb_backtest.validate_requested_strategy(args, strategy_names)

    def test_dashboard_contains_ai_progress_panel(self):
        self.assertIn('id="aiStatus"', qmt_dashboard.HTML)
        self.assertIn("refreshAIStatus", qmt_dashboard.HTML)

    def test_parse_ai_progress_from_log_text(self):
        status = qmt_dashboard.parse_ai_iteration_status(
            "\n".join([
                "iteration_round=2/AI_STOP",
                "optimize_workers=8 trials=810",
                "optimize_progress=520/810",
            ])
        )

        self.assertEqual(status["round"], "2/AI_STOP")
        self.assertEqual(status["progress_current"], 520)
        self.assertEqual(status["progress_total"], 810)
        self.assertEqual(status["remaining"], 290)

    def test_ai_strategy_validation_preserves_quant_parameter_grid(self):
        strategies = strategy_iteration_loop.validate_agent_strategies(
            [
                {
                    "name": "expert_channel",
                    "description": "Channel thesis with defensive pacing.",
                    "research_thesis": "Use fewer holdings only when the signal is stable; slow rebalance to reduce noise.",
                    "weights": [
                        {"factor": "price_position", "weight": 0.4},
                        {"factor": "trend_filter", "weight": 0.3},
                        {"factor": "low_gap_risk", "weight": 0.3},
                    ],
                    "parameter_grid": {
                        "top": [5, 8, 99],
                        "lookback": [60, 90],
                        "rebalance_days": [20, 40],
                    },
                }
            ],
            {"price_position", "trend_filter", "low_gap_risk"},
            set(),
        )

        self.assertEqual(strategies[0]["parameter_grid"], {
            "top": [5, 8],
            "lookback": [60, 90],
            "rebalance_days": [20, 40],
        })
        self.assertIn("stable", strategies[0]["research_thesis"])

    def test_strategy_trials_use_strategy_specific_parameter_grid(self):
        miniqmt_cb_backtest.set_active_strategies([
            {
                "name": "expert_channel",
                "weights": {"price_position": 1.0},
                "parameter_grid": {"top": [5], "lookback": [60, 90], "rebalance_days": [20]},
            },
            {
                "name": "expert_defensive",
                "weights": {"low_volatility": 1.0},
                "parameter_grid": {"top": [8], "lookback": [90], "rebalance_days": [40]},
            },
        ])

        trials = miniqmt_cb_backtest.strategy_trials()

        self.assertEqual(trials, [
            ("expert_channel", 5, 60, 20),
            ("expert_channel", 5, 90, 20),
            ("expert_defensive", 8, 90, 40),
        ])

    def test_agent_prompt_requires_history_based_quant_diagnosis(self):
        prompt = strategy_iteration_loop.build_agent_prompt(
            Path("sample_run"),
            Path("strategy_candidates.json"),
            {"tested": 3, "passed": 0, "top": []},
            {},
            [],
        )

        self.assertIn("diagnosis", prompt)
        self.assertIn("parameter_grid", prompt)
        self.assertIn("historical backtest results", prompt)

    def test_dashboard_contains_ai_research_panel(self):
        self.assertIn('id="aiResearch"', qmt_dashboard.HTML)
        self.assertIn("renderAIResearch", qmt_dashboard.HTML)
        self.assertIn("接手机制", qmt_dashboard.HTML)

    def test_result_summary_includes_parameter_analysis_for_ai_research(self):
        rows = [
            {"strategy": "a", "rank_score": "0.5", "annual_return": "0.2", "max_drawdown": "-0.25", "calmar": "0.8", "monthly_win_rate": "0.5", "top": "3", "lookback": "40", "rebalance_days": "10", "passed": "False"},
            {"strategy": "b", "rank_score": "0.4", "annual_return": "0.16", "max_drawdown": "-0.12", "calmar": "1.3", "monthly_win_rate": "0.62", "top": "5", "lookback": "90", "rebalance_days": "40", "passed": "True"},
        ]

        summary = strategy_iteration_loop.summarize_results(rows)

        self.assertIn("parameter_analysis", summary)
        self.assertEqual(summary["parameter_analysis"]["top"]["3"]["tested"], 1)
        self.assertEqual(summary["parameter_analysis"]["top"]["5"]["passed"], 1)
        self.assertIn("failure_modes", summary)

    def test_stop_after_trials_limits_strategy_trials(self):
        miniqmt_cb_backtest.set_active_strategies([
            {"name": "a", "weights": {"price_position": 1.0}},
            {"name": "b", "weights": {"low_volatility": 1.0}},
        ])
        args = miniqmt_cb_backtest.parse_args_from([
            "miniqmt_cb_backtest.py",
            "--optimize",
            "--stop-after-trials",
            "4",
        ])

        trials = miniqmt_cb_backtest.strategy_trials(args)

        self.assertEqual(len(trials), 4)

    def test_review_policy_is_sanitized_from_ai_decision(self):
        policy = strategy_iteration_loop.normalize_review_policy({
            "mode": "after_n_trials",
            "min_completed_trials": 12,
            "review_every_trials": 90,
            "reason": "Early read is enough after weak historical parameter regions.",
        })

        self.assertEqual(policy["mode"], "after_n_trials")
        self.assertEqual(policy["min_completed_trials"], 20)
        self.assertEqual(policy["review_every_trials"], 90)

    def test_run_command_uses_review_policy_stop_after_trials(self):
        args = strategy_iteration_loop.parse_args_from([
            "strategy_iteration_loop.py",
            "--review-policy-file",
            "policy.json",
        ])
        command = strategy_iteration_loop.backtest_command(args, stop_after_trials=120)

        self.assertIn("--stop-after-trials", command)
        self.assertIn("120", command)


if __name__ == "__main__":
    unittest.main()
