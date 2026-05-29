from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone

from aiohttp import web

from ..config import DEFAULT_SCENARIOS, MockServerConfig
from .harness import run_benchmark

logger = logging.getLogger(__name__)


HTML_PAGE = """<!doctype html>
<html lang=\"uk\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Async Resilience Dashboard</title>
  <style>
    :root {
      --bg: #0e1216;
      --panel: #171c22;
      --panel-2: #10141a;
      --panel-3: #1f2630;
      --accent: #23b5d3;
      --accent-2: #f6c945;
      --accent-3: #87ffb1;
      --text: #e7edf5;
      --muted: #a2afc0;
      --danger: #ff6b6b;
      --shadow: rgba(0, 0, 0, 0.45);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Montserrat", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 15% 20%, #1a2434 0%, var(--bg) 55%),
        radial-gradient(circle at 85% 15%, #1b2a33 0%, var(--bg) 65%),
        linear-gradient(180deg, #0d1116 0%, #0b0f14 100%);
      color: var(--text);
      min-height: 100vh;
    }

    header {
      padding: 32px 28px 16px 28px;
      display: grid;
      gap: 14px;
    }

    .title {
      display: flex;
      align-items: baseline;
      gap: 12px;
      flex-wrap: wrap;
    }

    header h1 {
      margin: 0;
      font-size: 34px;
      letter-spacing: 0.7px;
      text-transform: uppercase;
    }

    header h1 span {
      color: var(--accent);
    }

    header p {
      margin: 0;
      color: var(--muted);
      max-width: 840px;
    }

    .chips {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .chip {
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 6px 12px;
      border-radius: 999px;
      font-size: 12px;
      color: var(--muted);
      background: rgba(16, 20, 26, 0.5);
    }

    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      padding: 8px 28px 20px 28px;
    }

    button {
      background: linear-gradient(135deg, var(--accent), #1f8ea8);
      border: none;
      color: #081018;
      padding: 12px 18px;
      font-weight: 700;
      border-radius: 12px;
      cursor: pointer;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      box-shadow: 0 10px 20px rgba(35, 181, 211, 0.25);
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 14px 28px rgba(35, 181, 211, 0.35);
    }

    button.secondary {
      background: linear-gradient(135deg, #384152, #252c37);
      color: var(--text);
      box-shadow: none;
    }

    button:disabled {
      background: #3b4a5c;
      cursor: not-allowed;
      color: #c7d0db;
      box-shadow: none;
      transform: none;
    }

    .layout {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      padding: 0 28px 24px 28px;
    }

    .panel {
      background: var(--panel);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 12px 30px var(--shadow);
    }

    .panel h2 {
      margin: 0 0 10px 0;
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: 1.2px;
      color: var(--muted);
    }

    .hero {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }

    .stat {
      padding: 16px;
      border-radius: 14px;
      background: linear-gradient(140deg, rgba(35, 181, 211, 0.18), rgba(16, 20, 26, 0.7));
      border: 1px solid rgba(255, 255, 255, 0.06);
    }

    .stat h3 {
      margin: 0 0 8px 0;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.8px;
    }

    .stat span {
      font-size: 22px;
      font-weight: 700;
    }

    .chart {
      width: 100%;
      height: 240px;
      background: var(--panel-2);
      border-radius: 12px;
      padding: 8px;
    }

    canvas {
      width: 100%;
      height: 100%;
    }

    .legend {
      display: flex;
      gap: 12px;
      margin-top: 10px;
      flex-wrap: wrap;
      font-size: 12px;
      color: var(--muted);
    }

    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .legend i {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
    }

    .table-wrap {
      width: 100%;
    }

    .table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
      table-layout: fixed;
    }

    .table th,
    .table td {
      text-align: left;
      padding: 8px 6px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.07);
      white-space: normal;
      word-break: break-word;
    }

    .table th {
      color: var(--muted);
      font-weight: 600;
      font-size: 11px;
      letter-spacing: 0.4px;
    }

    .status {
      padding: 0 28px 24px 28px;
      color: var(--muted);
    }

    .status strong {
      color: var(--text);
    }

    .footnote {
      padding: 0 28px 32px 28px;
      color: #8190a3;
      font-size: 12px;
    }

    @keyframes fadeInUp {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .panel, .stat {
      animation: fadeInUp 0.4s ease both;
    }

    @media (max-width: 640px) {
      header h1 { font-size: 24px; }
      .chart { height: 200px; }
    }
  </style>
</head>
<body>
  <header>
    <div class=\"title\">
      <h1>Async <span>Resilience</span> Dashboard</h1>
    </div>
    <p>Інтерактивний огляд latency-метрик та відмовостійкості. Дані беруться зі сценаріїв бенчмарку.</p>
    <div class=\"chips\">
      <span class=\"chip\">Circuit Breaker</span>
      <span class=\"chip\">Adaptive Backpressure</span>
      <span class=\"chip\">Retry with Jitter</span>
      <span class=\"chip\">Chaos Mode</span>
    </div>
  </header>
  <div class=\"controls\">
    <button id=\"runBtn\">Запустити бенчмарк</button>
    <button id=\"reloadBtn\" class=\"secondary\">Оновити дані</button>
  </div>
  <div class=\"layout\">
    <section class=\"panel\">
      <h2>Ключові показники</h2>
      <div class=\"hero\">
        <div class=\"stat\">
          <h3>Сценарії</h3>
          <span id=\"statScenarios\">-</span>
        </div>
        <div class=\"stat\">
          <h3>Середня латентність</h3>
          <span id=\"statAvg\">-</span>
        </div>
        <div class=\"stat\">
          <h3>Успішні запити</h3>
          <span id=\"statSuccess\">-</span>
        </div>
        <div class=\"stat\">
          <h3>Помилки</h3>
          <span id=\"statFailure\">-</span>
        </div>
      </div>
    </section>
    <section class=\"panel\">
      <h2>Latency (avg)</h2>
      <div class=\"chart\"><canvas id=\"avgChart\"></canvas></div>
    </section>
    <section class=\"panel\">
      <h2>Latency (p95)</h2>
      <div class=\"chart\"><canvas id=\"p95Chart\"></canvas></div>
    </section>
    <section class=\"panel\">
      <h2>Success / Failure</h2>
      <div class=\"chart\"><canvas id=\"successChart\"></canvas></div>
      <div class=\"legend\">
        <span><i style=\"background: #23b5d3;\"></i> Success</span>
        <span><i style=\"background: #ff6b6b;\"></i> Failure</span>
      </div>
    </section>
    <section class=\"panel\">
      <h2>Деталі сценаріїв</h2>
      <div class=\"table-wrap\">
        <table class=\"table\" id=\"resultsTable\"></table>
      </div>
    </section>
  </div>
  <div class=\"status\" id=\"status\">Статус: <strong>Очікування</strong></div>
  <div class=\"footnote\">Рекомендація: запускай бенчмарк кілька разів для стабільної середньої.</div>

  <script>
    const runBtn = document.getElementById('runBtn');
    const reloadBtn = document.getElementById('reloadBtn');
    const statusEl = document.getElementById('status');

    const avgChart = document.getElementById('avgChart');
    const p95Chart = document.getElementById('p95Chart');
    const successChart = document.getElementById('successChart');
    const table = document.getElementById('resultsTable');
    const statScenarios = document.getElementById('statScenarios');
    const statAvg = document.getElementById('statAvg');
    const statSuccess = document.getElementById('statSuccess');
    const statFailure = document.getElementById('statFailure');

    function setStatus(text) {
      statusEl.innerHTML = `Статус: <strong>${text}</strong>`;
    }

    function formatLabel(label) {
      const map = {
        'full_stack': 'full',
        'resilience_chaos': 'chaos'
      };
      if (map[label]) {
        return map[label];
      }
      return label.replace('_only', '').replace('_', ' ');
    }

    function drawBarChart(canvas, labels, values, color) {
      const ctx = canvas.getContext('2d');
      const width = canvas.width = canvas.clientWidth * window.devicePixelRatio;
      const height = canvas.height = canvas.clientHeight * window.devicePixelRatio;
      ctx.clearRect(0, 0, width, height);

      const padding = 48 * window.devicePixelRatio;
      const labelOffset = 10 * window.devicePixelRatio;
      const maxValue = Math.max(...values, 1);
      const barWidth = (width - padding * 2) / values.length * 0.7;
      const gap = (width - padding * 2) / values.length * 0.3;

      ctx.font = `${11 * window.devicePixelRatio}px Segoe UI`;
      ctx.fillStyle = '#a2afc0';
      ctx.textAlign = 'center';

      values.forEach((value, index) => {
        const x = padding + index * (barWidth + gap) + gap / 2;
        const barHeight = (height - padding * 2) * (value / maxValue);
        ctx.fillStyle = color;
        ctx.fillRect(x, height - padding - barHeight, barWidth, barHeight);
        ctx.fillStyle = '#a2afc0';
        ctx.save();
        ctx.translate(x + barWidth / 2 + labelOffset, height - padding / 2);
        ctx.rotate(-Math.PI / 6);
        ctx.fillText(formatLabel(labels[index]), 0, 0);
        ctx.restore();
      });
    }

    function drawStackedChart(canvas, labels, successValues, failureValues) {
      const ctx = canvas.getContext('2d');
      const width = canvas.width = canvas.clientWidth * window.devicePixelRatio;
      const height = canvas.height = canvas.clientHeight * window.devicePixelRatio;
      ctx.clearRect(0, 0, width, height);

      const padding = 48 * window.devicePixelRatio;
      const labelOffset = 10 * window.devicePixelRatio;
      const totals = labels.map((_, idx) => successValues[idx] + failureValues[idx]);
      const maxValue = Math.max(...totals, 1);
      const barWidth = (width - padding * 2) / labels.length * 0.7;
      const gap = (width - padding * 2) / labels.length * 0.3;

      ctx.font = `${11 * window.devicePixelRatio}px Segoe UI`;
      ctx.textAlign = 'center';

      labels.forEach((label, index) => {
        const x = padding + index * (barWidth + gap) + gap / 2;
        const total = totals[index];
        const successHeight = (height - padding * 2) * (successValues[index] / maxValue);
        const failureHeight = (height - padding * 2) * (failureValues[index] / maxValue);

        ctx.fillStyle = '#23b5d3';
        ctx.fillRect(x, height - padding - successHeight, barWidth, successHeight);
        ctx.fillStyle = '#ff6b6b';
        ctx.fillRect(x, height - padding - successHeight - failureHeight, barWidth, failureHeight);

        ctx.fillStyle = '#a2afc0';
        ctx.save();
        ctx.translate(x + barWidth / 2 + labelOffset, height - padding / 2);
        ctx.rotate(-Math.PI / 6);
        ctx.fillText(formatLabel(label), 0, 0);
        ctx.restore();
      });
    }

    function renderTable(results) {
      table.innerHTML = '';
      const header = document.createElement('tr');
      ['scenario', 'avg (ms)', 'p50 (ms)', 'p95 (ms)', 'p99 (ms)', 'success', 'failure'].forEach(text => {
        const th = document.createElement('th');
        th.textContent = text;
        header.appendChild(th);
      });
      table.appendChild(header);

      Object.entries(results).forEach(([name, metrics]) => {
        const row = document.createElement('tr');
        const values = [
          name,
          (metrics.avg * 1000).toFixed(2),
          (metrics.p50 * 1000).toFixed(2),
          (metrics.p95 * 1000).toFixed(2),
          (metrics.p99 * 1000).toFixed(2),
          metrics.success.toFixed(0),
          metrics.failure.toFixed(0)
        ];
        values.forEach(value => {
          const td = document.createElement('td');
          td.textContent = value;
          row.appendChild(td);
        });
        table.appendChild(row);
      });
    }

    async function loadResults() {
      setStatus('Завантаження...');
      const response = await fetch('/results');
      if (!response.ok) {
        setStatus('Немає даних');
        return;
      }
      const data = await response.json();
      if (!data.results) {
        setStatus('Немає даних');
        return;
      }
      render(data.results);
      setStatus(`Оновлено ${data.generated_at}`);
    }

    function render(results) {
      const labels = Object.keys(results);
      const avgValues = labels.map(label => results[label].avg * 1000);
      const p95Values = labels.map(label => results[label].p95 * 1000);
      const successValues = labels.map(label => results[label].success);
      const failureValues = labels.map(label => results[label].failure);

      drawBarChart(avgChart, labels, avgValues, '#23b5d3');
      drawBarChart(p95Chart, labels, p95Values, '#f6c945');
      drawStackedChart(successChart, labels, successValues, failureValues);
      renderTable(results);

      const totalAvg = avgValues.reduce((a, b) => a + b, 0) / Math.max(avgValues.length, 1);
      const totalSuccess = successValues.reduce((a, b) => a + b, 0);
      const totalFailure = failureValues.reduce((a, b) => a + b, 0);
      statScenarios.textContent = labels.length.toString();
      statAvg.textContent = `${totalAvg.toFixed(2)} ms`;
      statSuccess.textContent = totalSuccess.toFixed(0);
      statFailure.textContent = totalFailure.toFixed(0);
    }

    runBtn.addEventListener('click', async () => {
      runBtn.disabled = true;
      setStatus('Запуск бенчмарку...');
      const response = await fetch('/run', { method: 'POST' });
      const data = await response.json();
      render(data.results);
      setStatus(`Готово ${data.generated_at}`);
      runBtn.disabled = false;
    });

    reloadBtn.addEventListener('click', loadResults);
    window.addEventListener('resize', () => loadResults());

    loadResults().catch(() => setStatus('Очікування запуску бенчмарку'));
  </script>
</body>
</html>
"""


async def index(_: web.Request) -> web.Response:
    return web.Response(text=HTML_PAGE, content_type="text/html")


async def run_handler(request: web.Request) -> web.Response:
  try:
    results = await run_benchmark(DEFAULT_SCENARIOS)
    payload = {
      "generated_at": datetime.now(timezone.utc).isoformat(),
      "results": results,
    }
    request.app["state"]["last_results"] = payload
    return web.json_response(payload)
  except asyncio.CancelledError:
    logger.warning("Dashboard run cancelled")
    raise
  except Exception as exc:
    logger.error("Dashboard run failed: %s", exc, exc_info=True)
    return web.json_response({"error": str(exc)}, status=500)


async def results_handler(request: web.Request) -> web.Response:
    payload = request.app["state"].get("last_results")
    if not payload:
        return web.json_response({"results": None}, status=404)
    return web.json_response(payload)


def create_app() -> web.Application:
    app = web.Application()
    app["state"] = {"last_results": None}
    app.router.add_get("/", index)
    app.router.add_post("/run", run_handler)
    app.router.add_get("/results", results_handler)
    return app


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    app = create_app()
    web.run_app(app, host="127.0.0.1", port=8080)


if __name__ == "__main__":
    main()
