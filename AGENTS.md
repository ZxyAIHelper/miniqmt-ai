# MiniQMT Convertible-Bond Backtest Workspace

This workspace is intentionally narrow: it validates a MiniQMT/xtquant
convertible-bond backtest flow from historical data to buy/sell trade records.

## Run

```powershell
python miniqmt_cb_backtest.py
```

Search multiple strategies and keep candidates with max drawdown no worse than
the target constraints in `target_strategy_metrics.csv`:

```powershell
python miniqmt_cb_backtest.py --optimize
```

Strategies are loaded from `strategy_candidates.json` by default. This file is
the editable strategy pool for human or AI iteration; the Python code should
execute and validate candidates, not hard-code each experiment.

Use parallel CPU workers for optimization:

```powershell
python miniqmt_cb_backtest.py --optimize --workers 8
```

Run the iterative AI research loop:

```powershell
python strategy_iteration_loop.py --workers 8 --limit 0
```

`strategy_iteration_loop.py` launches one MiniQMT optimization round, reads the
archived search result, writes a Codex review prompt under
`qmt_outputs/ai_reviews/`, calls `codex exec` as the decision agent, validates
the agent's JSON response with `codex_strategy_decision.schema.json`, updates
`strategy_candidates.json` when Codex proposes useful variants, then starts the
next round automatically. By default `--rounds 0` means there is no
research-round target; the loop stops only when Codex says there is no useful
next strategy to try. `--safety-max-rounds` is an emergency guard only. Use
`--ai-provider heuristic` only as a local fallback when Codex CLI is unavailable.

The script:

- connects to local MiniQMT through `xtquant.xtdata`;
- loads the `沪深转债` universe;
- downloads and reads daily historical bars;
- runs a simple rebalance backtest;
- writes buy and sell records to `qmt_outputs/cb_backtest_trades.csv`;
- writes equity to `qmt_outputs/cb_backtest_equity.csv`;
- writes summary metrics to `qmt_outputs/cb_backtest_summary.csv`.
- writes strategy definitions to `qmt_outputs/cb_strategy_definitions.csv`;
- writes applied target constraints to
  `qmt_outputs/cb_target_constraints_applied.csv`;
- in optimize mode, writes all search results to
  `qmt_outputs/cb_strategy_search.csv` and best passed result per strategy to
  `qmt_outputs/cb_strategy_best_by_type.csv`.
  The search uses 10000 initial cash, cost after fee and slippage, holding count
  3-8, annual return, drawdown, Calmar, Sharpe, monthly win rate, monthly max
  loss, and drawdown duration metrics.
  Optimization is CPU-parallel after MiniQMT data has been synced into SQLite;
  worker processes do not call MiniQMT.
  Each optimize run is archived under `qmt_outputs/runs/<run_id>/`, including
  the source strategy candidate JSON, strategy definitions, factor definitions,
  target constraints, and search results. This keeps every search round
  available for later Codex analysis.

No real order is sent. This is only a backtest and signal-flow validation.

Downloaded daily bars are stored in `.qmt_cache/miniqmt_cb_history.sqlite3`.
Repeated runs read from SQLite and only request missing date ranges per bond
from MiniQMT, so daily runs only need to sync incremental updates.

Convertible-bond metadata is stored in the same SQLite database. The current
factor set includes price, volatility, liquidity, drawdown, price-channel,
downside-risk, maturity, size, forced-redemption, and optional conversion or
underlying-stock factors when MiniQMT provides the needed fields.

## Required Local Config

`config.py` stores local MiniQMT settings, especially `QMT_USER_DATA_PATH`.
Do not expose private account IDs or private paths.

## Notes

- Keep the project focused on MiniQMT external Python APIs.
- Do not reintroduce QMT client `passorder` strategy files unless explicitly
  requested.
- Do not add stock-pool strategy comparison code unless explicitly requested.
