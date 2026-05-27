from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "qmt_outputs"
RUNS_DIR = OUTPUT_DIR / "runs"
AI_REVIEWS_DIR = OUTPUT_DIR / "ai_reviews"
LOGS_DIR = OUTPUT_DIR / "logs"
STRATEGY_FILE = BASE_DIR / "strategy_candidates.json"
BACKTEST_SCRIPT = BASE_DIR / "miniqmt_cb_backtest.py"
JOBS: dict[str, dict[str, Any]] = {}


HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MiniQMT 可转债策略研究台</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --line: #d9dde5;
      --muted: #687386;
      --text: #182033;
      --accent: #1769aa;
      --good: #147a50;
      --bad: #b33a3a;
      --warn: #9a6700;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
    }
    button, input, select {
      font: inherit;
    }
    .app {
      display: grid;
      grid-template-columns: 260px 1fr;
      min-height: 100vh;
    }
    aside {
      border-right: 1px solid var(--line);
      background: #fbfcfe;
      padding: 18px 14px;
      position: sticky;
      top: 0;
      height: 100vh;
      overflow: auto;
    }
    main {
      padding: 18px 22px 32px;
      min-width: 0;
    }
    h1 {
      font-size: 20px;
      margin: 0 0 6px;
      letter-spacing: 0;
    }
    h2 {
      font-size: 15px;
      margin: 18px 0 10px;
    }
    .subtle { color: var(--muted); }
    .run-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-top: 12px;
    }
    .run-btn {
      width: 100%;
      text-align: left;
      border: 1px solid transparent;
      background: transparent;
      padding: 8px 9px;
      border-radius: 6px;
      cursor: pointer;
      color: var(--text);
    }
    .run-btn:hover { background: #eef3f8; }
    .run-btn.active {
      border-color: #b9cce0;
      background: #e7f0f8;
      color: #0d4f80;
    }
    .topbar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 14px;
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .btn {
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      border-radius: 6px;
      padding: 7px 10px;
      cursor: pointer;
    }
    .btn.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: white;
    }
    .btn:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 11px 12px;
      min-height: 74px;
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }
    .metric .value {
      font-size: 20px;
      font-weight: 650;
      line-height: 1.1;
    }
    .progress-panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 11px 12px;
      margin-bottom: 14px;
      display: grid;
      grid-template-columns: minmax(240px, 1.2fr) repeat(4, minmax(90px, .6fr));
      gap: 10px;
      align-items: center;
    }
    .progress-panel .label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 5px;
    }
    .progress-panel .value {
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .progress-track {
      grid-column: 1 / -1;
      height: 9px;
      background: #e8edf3;
      border-radius: 999px;
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      width: 0;
      background: var(--accent);
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 370px;
      gap: 14px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 7px;
      min-width: 0;
    }
    .side-stack {
      display: flex;
      flex-direction: column;
      gap: 14px;
      min-width: 0;
    }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
    }
    .filters {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .filters input, .filters select {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      min-width: 120px;
      background: white;
    }
    .table-wrap {
      overflow: auto;
      max-height: calc(100vh - 275px);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1060px;
    }
    th, td {
      padding: 8px 9px;
      border-bottom: 1px solid #edf0f4;
      text-align: right;
      white-space: nowrap;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #f9fafc;
      color: #4e596d;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    th:first-child, td:first-child { text-align: left; }
    tr:hover td { background: #f7fbff; }
    tr.selected td { background: #e9f3fb; }
    .pos { color: var(--good); }
    .neg { color: var(--bad); }
    .tag {
      display: inline-flex;
      align-items: center;
      border: 1px solid #cfd7e3;
      color: #445066;
      border-radius: 999px;
      padding: 2px 7px;
      font-size: 12px;
      background: #f8fafc;
    }
    .detail {
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .ai-decision {
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .ai-reason {
      border: 1px solid #edf0f4;
      border-radius: 6px;
      padding: 9px;
      line-height: 1.55;
      color: #334155;
      background: #fbfcfe;
      white-space: pre-wrap;
    }
    .ai-strategy {
      border: 1px solid #edf0f4;
      border-radius: 6px;
      padding: 9px;
      display: flex;
      flex-direction: column;
      gap: 7px;
    }
    .ai-strategy strong {
      overflow-wrap: anywhere;
    }
    .ai-mini {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .ai-mini span {
      border: 1px solid #d8e1ec;
      border-radius: 999px;
      padding: 2px 7px;
      font-size: 12px;
      background: #f8fafc;
      color: #3d4758;
    }
    .kv {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .kv div {
      border: 1px solid #edf0f4;
      border-radius: 6px;
      padding: 8px;
      min-width: 0;
    }
    .kv span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 5px;
    }
    .weights {
      display: flex;
      flex-direction: column;
      gap: 7px;
    }
    .bar-row {
      display: grid;
      grid-template-columns: 135px 1fr 48px;
      gap: 8px;
      align-items: center;
    }
    .bar-name {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #3d4758;
    }
    .bar-bg {
      height: 8px;
      background: #e8edf3;
      border-radius: 999px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      background: var(--accent);
    }
    .status {
      min-height: 22px;
      color: var(--muted);
    }
    @media (max-width: 1180px) {
      .app { grid-template-columns: 1fr; }
      aside { position: static; height: auto; border-right: none; border-bottom: 1px solid var(--line); }
      .layout { grid-template-columns: 1fr; }
      .grid { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <h1>MiniQMT 研究台</h1>
      <div class="subtle">可转债策略回测与 Agent 迭代结果</div>
      <h2>回测轮次</h2>
      <div id="runList" class="run-list"></div>
    </aside>
    <main>
      <div class="topbar">
        <div>
          <h1 id="title">策略搜索结果</h1>
          <div id="runMeta" class="subtle"></div>
        </div>
        <div class="actions">
          <button class="btn" id="refreshBtn">刷新</button>
          <button class="btn primary" id="rerunBtn" disabled>按当前策略重新回测</button>
        </div>
      </div>
      <section id="aiStatus" class="progress-panel">
        <div><div class="label">后台任务</div><div class="value" id="aiStatusStage">加载中</div></div>
        <div><div class="label">轮次</div><div class="value" id="aiStatusRound">-</div></div>
        <div><div class="label">进度</div><div class="value" id="aiStatusProgress">-</div></div>
        <div><div class="label">剩余</div><div class="value" id="aiStatusRemaining">-</div></div>
        <div><div class="label">最新 run</div><div class="value" id="aiStatusRun">-</div></div>
        <div class="progress-track"><div id="aiStatusFill" class="progress-fill"></div></div>
      </section>
      <section class="grid" id="metrics"></section>
      <section class="layout">
        <div class="panel">
          <div class="panel-head">
            <strong>策略结果</strong>
            <div class="filters">
              <input id="searchInput" placeholder="搜索策略名" />
              <select id="passFilter">
                <option value="all">全部</option>
                <option value="passed">仅通过</option>
                <option value="failed">未通过</option>
              </select>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th data-sort="strategy">策略</th>
                  <th data-sort="rank_score">评分</th>
                  <th data-sort="annual_return">年化</th>
                  <th data-sort="max_drawdown">最大回撤</th>
                  <th data-sort="calmar">Calmar</th>
                  <th data-sort="sharpe">Sharpe</th>
                  <th data-sort="monthly_win_rate">月胜率</th>
                  <th data-sort="max_monthly_loss">最大月亏</th>
                  <th data-sort="top">持仓</th>
                  <th data-sort="lookback">回看</th>
                  <th data-sort="rebalance_days">调仓</th>
                  <th data-sort="trade_count">交易数</th>
                  <th data-sort="passed">状态</th>
                </tr>
              </thead>
              <tbody id="resultBody"></tbody>
            </table>
          </div>
        </div>
        <div class="side-stack">
          <div class="panel">
            <div class="panel-head">
              <strong>单策略详情</strong>
              <span id="detailBadge" class="tag">未选择</span>
            </div>
            <div id="detail" class="detail">
              <div class="subtle">点击左侧表格中的一条结果查看参数、权重和重跑入口。</div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head">
              <strong>AI 如何选择</strong>
              <span id="aiDecisionBadge" class="tag">未生成</span>
            </div>
            <div id="aiDecision" class="ai-decision">
              <div class="subtle">这轮还没有 AI 决策记录；回测结束后会显示继续/停止原因和新策略权重。</div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head">
              <strong>AI 研究思路</strong>
              <span class="tag">历史归因</span>
            </div>
            <div id="aiResearch" class="ai-decision">
              <div class="subtle">回测结束并完成 AI 决策后，这里会显示诊断、规避方向和下一轮重点。</div>
            </div>
          </div>
        </div>
      </section>
      <p id="status" class="status"></p>
    </main>
  </div>
  <script>
    const state = { runs: [], selectedRun: "", results: [], definitions: {}, candidates: {}, decision: {}, selected: null, sort: "rank_score", desc: true };
    const numericFields = new Set(["rank_score","annual_return","max_drawdown","calmar","sharpe","monthly_win_rate","max_monthly_loss","top","lookback","rebalance_days","trade_count"]);
    const $ = id => document.getElementById(id);

    function pct(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return "-";
      return (n * 100).toFixed(2) + "%";
    }
    function num(v, digits = 3) {
      const n = Number(v);
      if (!Number.isFinite(n)) return "-";
      return n.toFixed(digits);
    }
    function cls(v) {
      const n = Number(v);
      if (!Number.isFinite(n) || n === 0) return "";
      return n > 0 ? "pos" : "neg";
    }
    function boolText(v) {
      return String(v).toLowerCase() === "true" ? "通过" : "未通过";
    }
    function esc(v) {
      return String(v ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[ch]));
    }
    async function api(path, options) {
      const res = await fetch(path, options);
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    }
    async function loadRuns() {
      const data = await api("/api/runs");
      state.runs = data.runs;
      if (!state.selectedRun && state.runs.length) state.selectedRun = state.runs[0].run_id;
      renderRuns();
      if (state.selectedRun) await loadRun(state.selectedRun);
    }
    async function refreshAIStatus() {
      try {
        const data = await api("/api/ai-status");
        const pctValue = Number(data.percent || 0);
        $("aiStatusStage").textContent = data.stage || "-";
        $("aiStatusRound").textContent = data.round || "-";
        $("aiStatusProgress").textContent = data.progress_total ? `${data.progress_current} / ${data.progress_total} (${pctValue.toFixed(1)}%)` : "-";
        $("aiStatusRemaining").textContent = data.remaining ?? "-";
        $("aiStatusRun").textContent = data.latest_run || "-";
        $("aiStatusFill").style.width = `${Math.max(0, Math.min(100, pctValue))}%`;
      } catch (err) {
        $("aiStatusStage").textContent = "状态读取失败";
        $("aiStatusProgress").textContent = err.message;
      }
    }
    async function loadRun(runId) {
      state.selectedRun = runId;
      state.selected = null;
      const data = await api(`/api/runs/${runId}`);
      state.results = data.results || [];
      state.definitions = data.definitions || {};
      state.candidates = data.candidates || {};
      state.decision = data.decision || {};
      $("title").textContent = `策略搜索结果 ${runId}`;
      $("runMeta").textContent = data.path || "";
      renderRuns();
      renderMetrics();
      renderTable();
      renderDetail();
      renderAIDecision();
      renderAIResearch();
    }
    function renderRuns() {
      $("runList").innerHTML = state.runs.map(run => `
        <button class="run-btn ${run.run_id === state.selectedRun ? "active" : ""}" data-run="${run.run_id}">
          <strong>${run.run_id}</strong><br>
          <span class="subtle">${run.result_count} 条 · ${run.modified}</span>
        </button>
      `).join("");
      document.querySelectorAll(".run-btn").forEach(btn => btn.onclick = () => loadRun(btn.dataset.run));
    }
    function filteredResults() {
      const q = $("searchInput").value.trim().toLowerCase();
      const pass = $("passFilter").value;
      const rows = state.results.filter(row => {
        const nameOk = !q || String(row.strategy || "").toLowerCase().includes(q);
        const passed = String(row.passed).toLowerCase() === "true";
        const passOk = pass === "all" || (pass === "passed" && passed) || (pass === "failed" && !passed);
        return nameOk && passOk;
      });
      rows.sort((a,b) => {
        const field = state.sort;
        let av = a[field], bv = b[field];
        if (numericFields.has(field)) { av = Number(av); bv = Number(bv); }
        const result = av > bv ? 1 : av < bv ? -1 : 0;
        return state.desc ? -result : result;
      });
      return rows;
    }
    function renderMetrics() {
      const rows = state.results;
      const best = [...rows].sort((a,b) => Number(b.rank_score) - Number(a.rank_score))[0] || {};
      const passed = rows.filter(r => String(r.passed).toLowerCase() === "true").length;
      const metrics = [
        ["组合数", rows.length || 0],
        ["通过数", passed],
        ["最佳策略", best.strategy || "-"],
        ["最佳评分", num(best.rank_score, 3)],
        ["最佳年化", pct(best.annual_return)],
        ["最佳回撤", pct(best.max_drawdown)]
      ];
      $("metrics").innerHTML = metrics.map(([label,value]) => `<div class="metric"><div class="label">${label}</div><div class="value">${value}</div></div>`).join("");
    }
    function renderTable() {
      const rows = filteredResults();
      $("resultBody").innerHTML = rows.map((row, i) => `
        <tr class="${state.selected === row ? "selected" : ""}" data-index="${state.results.indexOf(row)}">
          <td>${row.strategy || ""}</td>
          <td>${num(row.rank_score, 3)}</td>
          <td class="${cls(row.annual_return)}">${pct(row.annual_return)}</td>
          <td class="${cls(row.max_drawdown)}">${pct(row.max_drawdown)}</td>
          <td>${num(row.calmar, 2)}</td>
          <td>${num(row.sharpe, 2)}</td>
          <td>${pct(row.monthly_win_rate)}</td>
          <td class="${cls(row.max_monthly_loss)}">${pct(row.max_monthly_loss)}</td>
          <td>${row.top}</td>
          <td>${row.lookback}</td>
          <td>${row.rebalance_days}</td>
          <td>${row.trade_count}</td>
          <td><span class="tag">${boolText(row.passed)}</span></td>
        </tr>
      `).join("");
      document.querySelectorAll("#resultBody tr").forEach(tr => tr.onclick = () => {
        state.selected = state.results[Number(tr.dataset.index)];
        renderTable();
        renderDetail();
      });
    }
    function renderWeights(strategy) {
      const candidate = state.candidates[strategy] || {};
      const weights = candidate.weights || {};
      const entries = Object.entries(weights).sort((a,b) => Number(b[1]) - Number(a[1]));
      if (!entries.length) return `<div class="subtle">没有找到该策略的权重定义。</div>`;
      const max = Math.max(...entries.map(([,v]) => Number(v)));
      return `<div class="weights">${entries.map(([factor, weight]) => `
        <div class="bar-row" title="${factor}">
          <div class="bar-name">${factor}</div>
          <div class="bar-bg"><div class="bar-fill" style="width:${Math.max(4, Number(weight)/max*100)}%"></div></div>
          <div>${Number(weight).toFixed(3)}</div>
        </div>
      `).join("")}</div>`;
    }
    function renderDetail() {
      const row = state.selected;
      $("rerunBtn").disabled = !row;
      if (!row) {
        $("detailBadge").textContent = "未选择";
        $("detail").innerHTML = `<div class="subtle">点击左侧表格中的一条结果查看参数、权重和重跑入口。</div>`;
        return;
      }
      $("detailBadge").textContent = boolText(row.passed);
      const def = state.definitions[row.strategy] || {};
      $("detail").innerHTML = `
        <div>
          <strong>${row.strategy}</strong>
          <p class="subtle">${def.description || ""}</p>
        </div>
        <div class="kv">
          <div><span>年化</span><strong class="${cls(row.annual_return)}">${pct(row.annual_return)}</strong></div>
          <div><span>最大回撤</span><strong class="${cls(row.max_drawdown)}">${pct(row.max_drawdown)}</strong></div>
          <div><span>Calmar</span><strong>${num(row.calmar, 2)}</strong></div>
          <div><span>月胜率</span><strong>${pct(row.monthly_win_rate)}</strong></div>
          <div><span>持仓数量</span><strong>${row.top}</strong></div>
          <div><span>回看/调仓</span><strong>${row.lookback} / ${row.rebalance_days}</strong></div>
        </div>
        <div>
          <h2>因子权重</h2>
          ${renderWeights(row.strategy)}
        </div>
      `;
    }
    function normalizeAgentWeights(weights) {
      if (Array.isArray(weights)) return weights.map(item => [item.factor, item.weight]);
      return Object.entries(weights || {});
    }
    function renderAgentWeights(weights) {
      const entries = normalizeAgentWeights(weights).filter(([factor]) => factor);
      if (!entries.length) return "";
      return `<div class="ai-mini">${entries.map(([factor, weight]) => `<span>${esc(factor)} ${Number(weight).toFixed(3)}</span>`).join("")}</div>`;
    }
    function renderAIDecision() {
      const decision = state.decision || {};
      const hasDecision = Boolean(decision.time || decision.reason || decision.agent_response);
      if (!hasDecision) {
        $("aiDecisionBadge").textContent = "未生成";
        $("aiDecision").innerHTML = `<div class="subtle">这轮还没有 AI 决策记录；回测结束后会显示继续/停止原因和新策略权重。</div>`;
        return;
      }
      const response = decision.agent_response || {};
      const strategies = Array.isArray(response.strategies) ? response.strategies : [];
      $("aiDecisionBadge").textContent = decision.stop ? "停止" : "继续";
      const summary = decision.summary || {};
      $("aiDecision").innerHTML = `
        <div class="kv">
          <div><span>时间</span><strong>${esc(decision.time || "-")}</strong></div>
          <div><span>轮次</span><strong>${esc(decision.round_index ?? "-")}</strong></div>
          <div><span>测试/通过</span><strong>${esc(summary.tested ?? "-")} / ${esc(summary.passed ?? "-")}</strong></div>
          <div><span>新增策略</span><strong>${esc(decision.proposed_count ?? strategies.length)}</strong></div>
        </div>
        <div>
          <h2>决策原因</h2>
          <div class="ai-reason">${esc(decision.reason || response.reason || "-")}</div>
        </div>
        <div>
          <h2>AI 提出的策略</h2>
          ${strategies.length ? strategies.map(item => `
            <div class="ai-strategy">
              <strong>${esc(item.name || "-")}</strong>
              <div class="subtle">${esc(item.description || "")}</div>
              ${renderAgentWeights(item.weights)}
            </div>
          `).join("") : `<div class="subtle">本轮 AI 没有提出新的有效策略。</div>`}
        </div>
        ${decision.codex_prompt ? `<div class="subtle">Prompt: ${esc(decision.codex_prompt)}</div>` : ""}
      `;
    }
    function renderListBlock(value) {
      if (!value) return `<div class="subtle">-</div>`;
      if (typeof value === "string") return `<div class="ai-reason">${esc(value)}</div>`;
      return `<div class="ai-reason">${esc(JSON.stringify(value, null, 2))}</div>`;
    }
    function renderStrategyGrid(item) {
      const grid = item.parameter_grid || {};
      const parts = [
        ["持仓", grid.top],
        ["回看", grid.lookback],
        ["调仓", grid.rebalance_days]
      ].filter(([, values]) => Array.isArray(values) && values.length);
      if (!parts.length) return "";
      return `<div class="ai-mini">${parts.map(([label, values]) => `<span>${label}: ${values.join(", ")}</span>`).join("")}</div>`;
    }
    function renderAIResearch() {
      const decision = state.decision || {};
      const response = decision.agent_response || {};
      const strategies = Array.isArray(response.strategies) ? response.strategies : [];
      const diagnosis = decision.diagnosis || response.diagnosis || "";
      const avoid = decision.avoid || response.avoid || null;
      const focus = decision.focus || response.focus || null;
      const reviewPolicy = decision.review_policy || response.review_policy || null;
      if (!diagnosis && !avoid && !focus && !reviewPolicy && !strategies.some(item => item.research_thesis || item.parameter_grid)) {
        $("aiResearch").innerHTML = `<div class="subtle">这轮还没有 AI 研究归因；新的决策会显示诊断、规避方向、重点方向和参数选择理由。</div>`;
        return;
      }
      $("aiResearch").innerHTML = `
        <div>
          <h2>历史结果诊断</h2>
          ${renderListBlock(diagnosis)}
        </div>
        <div>
          <h2>规避方向</h2>
          ${renderListBlock(avoid)}
        </div>
        <div>
          <h2>下一轮重点</h2>
          ${renderListBlock(focus)}
        </div>
        <div>
          <h2>接手机制</h2>
          ${renderListBlock(reviewPolicy)}
        </div>
        <div>
          <h2>策略参数理由</h2>
          ${strategies.length ? strategies.map(item => `
            <div class="ai-strategy">
              <strong>${esc(item.name || "-")}</strong>
              <div class="subtle">${esc(item.research_thesis || item.description || "")}</div>
              ${renderStrategyGrid(item)}
            </div>
          `).join("") : `<div class="subtle">本轮没有新策略参数建议。</div>`}
        </div>
      `;
    }
    async function rerunSelected() {
      if (!state.selected) return;
      $("status").textContent = "已提交重新回测任务...";
      $("rerunBtn").disabled = true;
      try {
        const job = await api("/api/backtests", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            strategy: state.selected.strategy,
            top: Number(state.selected.top),
            lookback: Number(state.selected.lookback),
            rebalance_days: Number(state.selected.rebalance_days),
            start: state.selected.start,
            end: state.selected.end,
            limit: Number(state.selected.universe || 0)
          })
        });
        pollJob(job.job_id);
      } catch (err) {
        $("status").textContent = "提交失败: " + err.message;
        $("rerunBtn").disabled = false;
      }
    }
    async function pollJob(jobId) {
      const job = await api(`/api/jobs/${jobId}`);
      $("status").textContent = `任务 ${job.status}: ${job.command || ""}`;
      if (job.status === "running") {
        setTimeout(() => pollJob(jobId), 2500);
      } else {
        $("rerunBtn").disabled = false;
        $("status").textContent = job.status === "done" ? `重新回测完成，退出码 ${job.returncode}` : `重新回测失败，退出码 ${job.returncode}`;
        await loadRuns();
      }
    }
    $("refreshBtn").onclick = loadRuns;
    $("rerunBtn").onclick = rerunSelected;
    $("searchInput").oninput = renderTable;
    $("passFilter").onchange = renderTable;
    document.querySelectorAll("th[data-sort]").forEach(th => th.onclick = () => {
      const field = th.dataset.sort;
      if (state.sort === field) state.desc = !state.desc;
      else { state.sort = field; state.desc = true; }
      renderTable();
    });
    loadRuns().catch(err => $("status").textContent = "加载失败: " + err.message);
    refreshAIStatus();
    setInterval(refreshAIStatus, 5000);
  </script>
</body>
</html>
"""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if number == number else default
    except Exception:
        return default


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def run_dirs() -> list[Path]:
    if not RUNS_DIR.exists():
        return []
    return sorted([path for path in RUNS_DIR.iterdir() if path.is_dir()], key=lambda p: p.name, reverse=True)


def strategy_definitions(run_dir: Path) -> dict[str, dict[str, str]]:
    rows = read_csv(run_dir / "cb_strategy_definitions.csv")
    return {row.get("strategy", ""): row for row in rows if row.get("strategy")}


def strategy_candidates(run_dir: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(run_dir / "strategy_candidates.json") or read_json(STRATEGY_FILE)
    return {item.get("name", ""): item for item in payload.get("strategies", []) if item.get("name")}


def decision_for_run(run_id: str) -> dict[str, Any]:
    direct = AI_REVIEWS_DIR / f"{run_id}_decision.json"
    if direct.exists():
        return read_json(direct)
    return {}


def parse_ai_iteration_status(log_text: str) -> dict[str, Any]:
    round_matches = re.findall(r"iteration_round=([^\s]+)", log_text)
    progress_matches = re.findall(r"optimize_progress=(\d+)/(\d+)", log_text)
    trial_matches = re.findall(r"optimize_workers=\d+\s+trials=(\d+)", log_text)
    current = total = 0
    if progress_matches:
        current, total = (int(progress_matches[-1][0]), int(progress_matches[-1][1]))
    elif trial_matches:
        total = int(trial_matches[-1])

    if "ai_stop_reason=" in log_text:
        stage = "AI 已停止"
    elif "launch_codex_agent=" in log_text and log_text.rfind("launch_codex_agent=") > log_text.rfind("ai_agent_decision="):
        stage = "AI 决策中"
    elif total and current >= total:
        stage = "等待 AI 决策"
    elif total:
        stage = "回测中"
    else:
        stage = "等待启动"

    percent = round(current / total * 100, 1) if total else 0.0
    return {
        "stage": stage,
        "round": round_matches[-1] if round_matches else "",
        "progress_current": current,
        "progress_total": total,
        "remaining": max(total - current, 0) if total else None,
        "percent": percent,
    }


def latest_ai_status() -> dict[str, Any]:
    logs = sorted(LOGS_DIR.glob("ai_iteration_*_stdout.log"), key=lambda p: p.stat().st_mtime, reverse=True) if LOGS_DIR.exists() else []
    latest_run = run_dirs()[0].name if run_dirs() else ""
    if not logs:
        return {
            "stage": "没有 AI 迭代日志",
            "round": "",
            "progress_current": 0,
            "progress_total": 0,
            "remaining": None,
            "percent": 0.0,
            "latest_run": latest_run,
            "log": "",
        }
    path = logs[0]
    text = path.read_text(encoding="utf-8", errors="replace")
    status = parse_ai_iteration_status(text)
    status.update({
        "latest_run": latest_run,
        "log": str(path),
        "modified": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    })
    return status


def summarize_results(rows: list[dict[str, str]]) -> dict[str, Any]:
    if not rows:
        return {}
    sorted_rows = sorted(rows, key=lambda row: safe_float(row.get("rank_score"), -999), reverse=True)
    best = sorted_rows[0]
    passed = sum(1 for row in rows if str(row.get("passed")).lower() == "true")
    return {
        "result_count": len(rows),
        "passed_count": passed,
        "best_strategy": best.get("strategy"),
        "best_rank_score": safe_float(best.get("rank_score")),
        "best_annual_return": safe_float(best.get("annual_return")),
        "best_max_drawdown": safe_float(best.get("max_drawdown")),
        "best_calmar": safe_float(best.get("calmar")),
    }


def run_info(path: Path) -> dict[str, Any]:
    results = read_csv(path / "cb_strategy_search.csv")
    modified = datetime.fromtimestamp((path / "cb_strategy_search.csv").stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if (path / "cb_strategy_search.csv").exists() else ""
    return {
        "run_id": path.name,
        "path": str(path),
        "modified": modified,
        **summarize_results(results),
    }


def start_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    strategy = str(payload.get("strategy", "")).strip()
    if not strategy:
        raise ValueError("strategy is required")
    command = [
        sys.executable,
        str(BACKTEST_SCRIPT),
        "--strategy-file",
        str(STRATEGY_FILE),
        "--strategy",
        strategy,
        "--top",
        str(int(safe_float(payload.get("top"), 5))),
        "--lookback",
        str(int(safe_float(payload.get("lookback"), 40))),
        "--rebalance-days",
        str(int(safe_float(payload.get("rebalance_days"), 10))),
    ]
    for key in ["start", "end"]:
        if payload.get(key):
            command.extend([f"--{key}", str(payload[key])])
    if safe_float(payload.get("limit"), 0) > 0:
        command.extend(["--limit", str(int(safe_float(payload.get("limit"), 0)))])
    job_id = uuid.uuid4().hex[:12]
    job = {
        "job_id": job_id,
        "status": "running",
        "command": " ".join(command),
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "returncode": None,
        "output": "",
    }
    JOBS[job_id] = job

    def runner() -> None:
        try:
            completed = subprocess.run(command, cwd=str(BASE_DIR), capture_output=True, text=True, encoding="utf-8", errors="replace")
            job["returncode"] = completed.returncode
            job["output"] = (completed.stdout or "")[-8000:] + "\n" + (completed.stderr or "")[-4000:]
            job["status"] = "done" if completed.returncode == 0 else "failed"
            job["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as exc:
            job["status"] = "failed"
            job["output"] = str(exc)
            job["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    threading.Thread(target=runner, daemon=True).start()
    return job


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/api/runs":
            write_json(self, {"runs": [run_info(path) for path in run_dirs()]})
            return
        if path == "/api/ai-status":
            write_json(self, latest_ai_status())
            return
        if path.startswith("/api/runs/"):
            run_id = path.split("/")[-1]
            run_dir = RUNS_DIR / run_id
            if not run_dir.exists():
                write_json(self, {"error": "run not found"}, 404)
                return
            results = read_csv(run_dir / "cb_strategy_search.csv")
            results.sort(key=lambda row: safe_float(row.get("rank_score"), -999), reverse=True)
            write_json(self, {
                **run_info(run_dir),
                "results": results,
                "definitions": strategy_definitions(run_dir),
                "candidates": strategy_candidates(run_dir),
                "decision": decision_for_run(run_id),
            })
            return
        if path.startswith("/api/jobs/"):
            job_id = path.split("/")[-1]
            job = JOBS.get(job_id)
            if not job:
                write_json(self, {"error": "job not found"}, 404)
                return
            write_json(self, job)
            return
        write_json(self, {"error": "not found"}, 404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/backtests":
            write_json(self, {"error": "not found"}, 404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            job = start_backtest(payload)
            write_json(self, job)
        except Exception as exc:
            write_json(self, {"error": str(exc)}, 400)


def parse_args() -> tuple[str, int]:
    host = "127.0.0.1"
    port = 8765
    args = sys.argv[1:]
    if "--host" in args:
        host = args[args.index("--host") + 1]
    if "--port" in args:
        port = int(args[args.index("--port") + 1])
    return host, port


def main() -> int:
    host, port = parse_args()
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"QMT dashboard: http://{host}:{port}", flush=True)
    print("Press Ctrl+C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
