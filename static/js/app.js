/* ============================================================
   DB Tuner Pro — Frontend Logic (Optimized)
   ============================================================ */
'use strict';

// ── Global State ────────────────────────────────────────────
let _lastStats    = null;
let _lastRecs     = null;
let _explainCache = {};
let _filterChart  = null;
let _joinChart    = null;
let _activeStream = null;   // current EventSource — prevent leaks
let _analyzing    = false;  // debounce guard

// ── CSS Variables (cyberpunk aliases used in JS) ─────────────
const CSS = {
  blue:    'var(--neon-blue)',
  pink:    'var(--neon-pink)',
  purple:  'var(--neon-purple)',
  green:   'var(--neon-green)',
  amber:   'var(--neon-amber)',
  muted:   'var(--muted)',
  success: 'var(--neon-green)',
  warning: 'var(--neon-amber)',
  mono:    'var(--font-mono)',
};

// ── Shared HTML escape (single definition) ───────────────────
function escapeHTML(str) {
  return String(str)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;');
}

// ── Global Banner ────────────────────────────────────────────
function showGlobalBanner(type, message) {
  const el = document.getElementById('globalBanner');
  if (!el) return;
  const icons = {
    error:   '<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>',
    success: '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
    info:    '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>',
  };
  el.innerHTML = `
    <div class="banner ${type}">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="flex-shrink:0;">
        ${icons[type] || ''}
      </svg>
      <span>${message}</span>
      <button onclick="this.parentElement.remove()"
        style="margin-left:auto;background:none;border:none;color:inherit;cursor:pointer;opacity:0.7;font-size:18px;line-height:1;padding:0 2px;"
        aria-label="Dismiss">&times;</button>
    </div>`;
}

function showError(msg) {
  document.getElementById('loadingState')?.classList.add('hidden');
  document.getElementById('emptyState')?.classList.remove('hidden');
  const runBtn = document.getElementById('runAnalysisBtn');
  const bmBtn  = document.getElementById('benchmarkBtn');
  if (runBtn) runBtn.disabled = false;
  if (bmBtn)  bmBtn.disabled  = false;
  _analyzing = false;
  showGlobalBanner('error', msg);
}

// ── Modal ────────────────────────────────────────────────────
function openConnectionModal() {
  document.getElementById('connectionModal')?.classList.remove('hidden');
}

// ── Engine / Port auto-switch ─────────────────────────────────
function initEngineSwitch() {
  const engineSel = document.getElementById('dbEngine');
  const portInput = document.getElementById('dbPort');
  if (!engineSel || !portInput) return;
  engineSel.addEventListener('change', () => {
    portInput.value = engineSel.value === 'postgres' ? 5432 : 3306;
  });
}

// ── Export click from sidebar ─────────────────────────────────
function handleExportClick(e) {
  e.preventDefault();
  if (!_lastStats) { showGlobalBanner('info', 'Run an analysis first.'); return; }
  exportHTML();
}

// ── Connection Form ───────────────────────────────────────────
function initConnectionForm() {
  const form       = document.getElementById('connectionForm');
  const btnClose   = document.getElementById('btnCloseModal');
  const connResult = document.getElementById('connResult');
  const modal      = document.getElementById('connectionModal');
  const btnConnect = document.getElementById('btnConnect');

  if (btnClose && modal) {
    btnClose.addEventListener('click', () => modal.classList.add('hidden'));
  }
  // Close on overlay click
  if (modal) {
    modal.addEventListener('click', e => {
      if (e.target === modal) modal.classList.add('hidden');
    });
  }

  if (!form || !btnConnect) return;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (btnConnect.disabled) return; // prevent double-submit
    const originalHTML = btnConnect.innerHTML;
    btnConnect.innerHTML = '<div class="spinner-ring" style="width:13px;height:13px;border-width:2px;margin:0;"></div> Connecting…';
    btnConnect.disabled  = true;
    if (connResult) connResult.innerHTML = '';

    const creds = {
      host:         document.getElementById('dbHost')?.value?.trim()  || 'localhost',
      port:         parseInt(document.getElementById('dbPort')?.value) || 3306,
      user:         document.getElementById('dbUser')?.value?.trim()  || 'root',
      password:     document.getElementById('dbPass')?.value          || '',
      database:     document.getElementById('dbName')?.value?.trim()  || '',
      engine:       document.getElementById('dbEngine')?.value        || 'mysql',
      workloadPath: document.getElementById('workloadPath')?.value?.trim() || 'queries.txt',
    };

    try {
      const res  = await fetch('/api/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(creds),
      });
      
      let data;
      try {
        data = await res.json();
      } catch (parseErr) {
        throw new Error(`Server returned ${res.status} without JSON.`);
      }

      if (res.ok && data.status === 'success') {
        if (connResult) connResult.innerHTML = '<div class="banner info">Connected! Redirecting…</div>';
        setTimeout(() => window.location.href = '/dashboard', 700);
      } else {
        if (connResult) connResult.innerHTML = `<div class="banner error">${escapeHTML(data.message || `HTTP ${res.status}: Connection failed.`)}</div>`;
        btnConnect.innerHTML = originalHTML;
        btnConnect.disabled  = false;
      }
    } catch (err) {
      if (connResult) connResult.innerHTML = `<div class="banner error">${escapeHTML(err.message || 'Could not reach server. Is Flask running?')}</div>`;
      btnConnect.innerHTML = originalHTML;
      btnConnect.disabled  = false;
    }
  });
}

// ── SSE helper: close active stream safely ─────────────────────
function closeActiveStream() {
  if (_activeStream) {
    try { _activeStream.close(); } catch (_) {}
    _activeStream = null;
  }
}

// ── Progress helpers ──────────────────────────────────────────
let _progressFrame = null;
let _pendingProgress = null;

function renderProgress() {
  if (!_pendingProgress) return;
  const { pct, label, count } = _pendingProgress;
  const bar = document.getElementById('progressBar');
  const txt = document.getElementById('progressText');
  const cnt = document.getElementById('progressCount');
  if (bar && pct !== undefined) bar.style.width = `${Math.max(0, Math.min(100, pct))}%`;
  if (txt && label !== undefined) txt.textContent = label;
  if (cnt && count !== undefined) cnt.textContent  = count;
  _pendingProgress = null;
  _progressFrame = null;
}

function setProgress(pct, label, count) {
  // Merge pending updates
  if (!_pendingProgress) _pendingProgress = {};
  if (pct !== undefined) _pendingProgress.pct = pct;
  if (label !== undefined) _pendingProgress.label = label;
  if (count !== undefined) _pendingProgress.count = count;

  if (!_progressFrame) {
    _progressFrame = requestAnimationFrame(renderProgress);
  }
}

function showLoadingState() {
  document.getElementById('emptyState')?.classList.add('hidden');
  document.getElementById('resultsDashboard')?.classList.add('hidden');
  document.getElementById('loadingState')?.classList.remove('hidden');
}

function showResultsState() {
  document.getElementById('loadingState')?.classList.add('hidden');
  document.getElementById('resultsDashboard')?.classList.remove('hidden');
}

// ── Run Analysis (SSE) ────────────────────────────────────────
function initRunAnalysis() {
  const runBtn    = document.getElementById('runAnalysisBtn');
  const pathInput = document.getElementById('dashboardWorkloadPath');
  if (!runBtn) return;

  // Restore pending path from home page
  const pending = sessionStorage.getItem('pendingWorkloadPath');
  if (pending && pathInput) { pathInput.value = pending; sessionStorage.removeItem('pendingWorkloadPath'); }

  // Check for pasted SQL
  const pasted = sessionStorage.getItem('pastedSQL');
  if (pasted) {
    sessionStorage.removeItem('pastedSQL');
    // Trigger analysis automatically with pasted SQL  
    // (backend reads from session, just trigger now)
    setTimeout(() => runBtn.click(), 100);
  }

  runBtn.addEventListener('click', () => {
    if (_analyzing) return; // debounce
    _analyzing = true;
    closeActiveStream();

    showLoadingState();
    runBtn.disabled = true;
    _lastStats = null; _lastRecs = null; _explainCache = {};
    setProgress(0, 'Reading workload file…', '0 queries processed');

    const path = pathInput?.value?.trim() || '';
    const url  = '/api/analyze/stream' + (path ? `?workloadPath=${encodeURIComponent(path)}` : '');

    let es;
    try { es = new EventSource(url); }
    catch(err) { showError('Could not open SSE connection: ' + err.message); return; }
    _activeStream = es;

    es.onmessage = (event) => {
      let payload;
      try { payload = JSON.parse(event.data); } catch { return; }

      if (payload.type === 'progress') {
        const done = payload.done || 0;
        const pct  = Math.min(88, Math.round((done / Math.max(done + 500, 2000)) * 88));
        setProgress(pct, 'Parsing queries and detecting anti-patterns…', `${done.toLocaleString()} queries processed`);

      } else if (payload.type === 'progress_label') {
        setProgress(undefined, payload.label || 'Processing…', undefined);

      } else if (payload.type === 'result') {
        setProgress(100, 'Building recommendations…', undefined);
        closeActiveStream();
        _lastStats    = payload.stats;
        _lastRecs     = payload.recommendations;
        _explainCache = payload.explain_cache || {};
        setTimeout(() => {
          populateDashboard(payload.stats, payload.recommendations, payload.fingerprint);
          showResultsState();
          runBtn.disabled = false;
          _analyzing = false;
        }, 200);

      } else if (payload.type === 'done') {
        closeActiveStream();
        runBtn.disabled = false;
        _analyzing = false;

      } else if (payload.type === 'error') {
        closeActiveStream();
        showError(payload.message || 'Unknown server error.');
      }
    };

    es.onerror = () => { closeActiveStream(); showError('Connection to analysis stream lost. Check server logs.'); };
  });
}

// ── Dashboard Population ──────────────────────────────────────
function populateDashboard(stats, recs, fingerprint) {
  if (!stats) return;
  const totalIssues = Object.values(stats.anti_patterns || {}).reduce((a, b) => a + b, 0);

  const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  setVal('valTotalQueries', (stats.total_queries || 0).toLocaleString());
  setVal('valTotalIssues',  totalIssues.toLocaleString());
  setVal('valTotalRecs',    (recs?.length || 0).toLocaleString());

  const issuesBadge = document.getElementById('issuesBadge');
  if (issuesBadge) issuesBadge.textContent = `${totalIssues} issue${totalIssues !== 1 ? 's' : ''}`;

  const recBadge = document.getElementById('recBadge');
  if (recBadge) recBadge.textContent = `${recs?.length || 0} action${(recs?.length || 0) !== 1 ? 's' : ''}`;

  renderCharts(stats);
  renderHealthPanel(stats.anti_patterns || {});
  renderRecommendations(recs || []);
  if (fingerprint) renderFingerprintSection(fingerprint);
}

// ── Charts ────────────────────────────────────────────────────
function renderCharts(stats) {
  const makeChart = (canvasId, data, badgeId, isPurple) => {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const top10  = (data || []).slice(0, 10);
    const labels = top10.map(d => d.col);
    const vals   = top10.map(d => d.count);

    const badge = document.getElementById(badgeId);
    if (badge) badge.textContent = top10.length ? `top ${top10.length}` : 'no data';

    // Destroy existing chart cleanly
    if (canvasId === 'filterChart' && _filterChart) { _filterChart.destroy(); _filterChart = null; }
    if (canvasId === 'joinChart'   && _joinChart)   { _joinChart.destroy();   _joinChart   = null; }

    const bar  = isPurple ? 'rgba(157,0,255,0.65)'  : 'rgba(0,240,255,0.65)';
    const bord = isPurple ? 'rgba(157,0,255,0.9)'   : 'rgba(0,240,255,0.9)';
    const hov  = isPurple ? 'rgba(157,0,255,0.9)'   : 'rgba(0,240,255,0.9)';

    const instance = new Chart(canvas, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          data: vals,
          backgroundColor: bar,
          borderColor: bord,
          borderWidth: 1,
          borderRadius: 4,
          hoverBackgroundColor: hov,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 350 },
        plugins: { legend: { display: false } },
        scales: {
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(0,240,255,0.05)' },
            ticks: { color: '#3a5c78', stepSize: 1, font: { size: 11, family: 'JetBrains Mono' } },
          },
          x: {
            grid: { display: false },
            ticks: { color: '#3a5c78', font: { size: 11, family: 'JetBrains Mono' } },
          },
        },
      },
    });

    if (canvasId === 'filterChart') _filterChart = instance;
    else                             _joinChart   = instance;
  };

  makeChart('filterChart', stats.where, 'filterBadge', false);
  makeChart('joinChart',   stats.join,  'joinBadge',   true);
}

// ── Health Panel ──────────────────────────────────────────────
const HEALTH_CHECKS = [
  { key: 'select_star',           sev: 'critical', label: 'Wasteful SELECT *',                   desc: 'Fetches all columns — unnecessary disk I/O and memory pressure. Specify only needed columns.' },
  { key: 'leading_wildcard',      sev: 'critical', label: 'Leading Wildcard in LIKE',            desc: "LIKE '%term' prevents B-Tree index usage — results in a full table scan." },
  { key: 'order_by_rand',         sev: 'critical', label: 'ORDER BY RAND()',                     desc: 'Forces a filesort on the full result set. Extremely slow on large tables.' },
  { key: 'missing_where',         sev: 'critical', label: 'UPDATE/DELETE without WHERE',         desc: 'Modifies or deletes every row. Almost certainly unintentional.' },
  { key: 'not_in_subquery',       sev: 'warning',  label: 'NOT IN (subquery) Anti-Pattern',      desc: 'NULL-unsafe and forces a correlated scan. Replace with NOT EXISTS.' },
  { key: 'implicit_cross_join',   sev: 'warning',  label: 'Implicit Cross Join',                 desc: 'Comma-separated table list without a join condition produces a cartesian product.' },
  { key: 'function_on_col_where', sev: 'warning',  label: 'Function on Column in WHERE',        desc: 'WHERE YEAR(col) = … defeats index usage. Rewrite as a range condition.' },
  { key: 'offset_no_limit',       sev: 'warning',  label: 'OFFSET without LIMIT',               desc: 'OFFSET has no effect without LIMIT and can mislead query planners.' },
  { key: 'cartesian_product',     sev: 'warning',  label: 'Potential Cartesian Product',        desc: 'Multiple tables in FROM with no WHERE or JOIN — exponential row multiplication.' },
];

function renderHealthPanel(patterns) {
  const list    = document.getElementById('healthItemsList');
  const perfect = document.getElementById('healthPerfect');
  if (!list) return;
  list.innerHTML = '';
  perfect?.classList.add('hidden');

  let hasIssues = false;

  HEALTH_CHECKS.forEach(c => {
    const count = patterns[c.key] || 0;
    if (!count) return;
    hasIssues = true;
    const isCritical = c.sev === 'critical';
    const icon   = isCritical ? '🔴' : '🟡';
    const badge  = isCritical ? 'badge-red' : 'badge-neutral';
    const chip   = count > 50 ? 'CRITICAL' : isCritical ? 'HIGH' : 'WARN';

    const el = document.createElement('div');
    el.className = `issue-card ${c.sev}`;
    el.innerHTML = `
      <div class="issue-header" onclick="this.parentElement.classList.toggle('open')">
        <span class="issue-icon" aria-hidden="true">${icon}</span>
        <span class="issue-label">${escapeHTML(c.label)}</span>
        <span class="issue-count" style="color:var(--muted);font-family:var(--font-mono);font-size:11px;">${count.toLocaleString()}×</span>
        <span class="badge ${badge}">${chip}</span>
        <svg class="issue-chevron" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="flex-shrink:0;transition:transform 0.2s;">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </div>
      <div class="issue-body" style="padding:0 16px 0;max-height:0;overflow:hidden;transition:max-height 0.3s ease,padding 0.3s ease;">
        <p style="font-size:12.5px;color:var(--muted);padding:10px 0;">${escapeHTML(c.desc)}</p>
      </div>`;

    // Toggle open/close properly
    el.querySelector('.issue-header').addEventListener('click', () => {
      const body    = el.querySelector('.issue-body');
      const chevron = el.querySelector('.issue-chevron');
      const isOpen  = el.classList.toggle('open');
      body.style.maxHeight  = isOpen ? body.scrollHeight + 20 + 'px' : '0';
      body.style.padding    = isOpen ? '' : '0 16px 0';
      chevron.style.transform = isOpen ? 'rotate(180deg)' : '';
    });

    list.appendChild(el);
  });

  if (!hasIssues) perfect?.classList.remove('hidden');
}

// ── Recommendations ───────────────────────────────────────────
const TYPE_MAP = {
  'INDEX':     { badge: 'badge-cyan',   label: 'Index' },
  'VIEW':      { badge: 'badge-purple', label: 'View' },
  'PARTITION': { badge: 'badge-neutral',label: 'Partition' },
};

function renderRecommendations(recs) {
  const list = document.getElementById('recommendationsList');
  if (!list) return;
  list.innerHTML = '';

  if (!recs.length) {
    list.innerHTML = '<p style="font-size:13px;color:var(--muted);padding:16px 0;">No optimizations needed — workload is healthy. 🎉</p>';
    return;
  }

  const frag = document.createDocumentFragment();
  recs.forEach((rec, i) => {
    const t       = TYPE_MAP[rec.type] || { badge: 'badge-neutral', label: rec.type || '?' };
    const cost    = rec.maintenance_cost || 'N/A';
    const costClr = cost === 'Low' ? 'var(--neon-green)' : 'var(--neon-amber)';
    const scoreH  = rec.score
      ? `<div class="rec-meta-item"><span>Score </span><strong>${rec.score}</strong></div>` : '';
    const speedH  = rec.speedup && rec.speedup !== 'N/A' && !rec.speedup.includes('Schema')
      ? `<div class="rec-meta-item"><span>Est. Speedup </span><strong style="color:var(--neon-green);">${escapeHTML(rec.speedup)}</strong></div>` : '';

    const el = document.createElement('div');
    el.className = 'rec-card';
    el.innerHTML = `
      <div class="rec-header">
        <div class="rec-summary">
          <div class="rec-main-label">${escapeHTML(rec.table || '')} <span style="color:var(--muted);font-weight:400;">(${escapeHTML((rec.columns || []).join(', '))})</span></div>
          <div class="rec-sub-label" style="font-size:11.5px;color:var(--muted);margin-top:2px;">${escapeHTML(rec.reason || '')}</div>
        </div>
        <div style="display:flex;gap:5px;align-items:center;flex-shrink:0;">
          <span class="badge ${t.badge}">${t.label}</span>
          <span class="badge badge-neutral">${escapeHTML(rec.index_type || '')}</span>
        </div>
        <svg class="rec-chevron" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="flex-shrink:0;transition:transform 0.2s;">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </div>
      <div class="rec-body" style="padding:0 16px;max-height:0;overflow:hidden;transition:max-height 0.35s ease,padding 0.35s ease;">
        <p style="font-size:12.5px;color:var(--muted);padding:10px 0 8px;">${escapeHTML(rec.explanation || '')}</p>
        <div class="rec-meta" style="display:flex;gap:14px;font-size:12px;margin-bottom:10px;">
          <div><span style="color:var(--muted);">Overhead </span><strong style="color:${costClr};">${escapeHTML(cost)}</strong></div>
          ${scoreH}${speedH}
        </div>
        <div class="sql-block" id="sqlBlock_${i}" style="position:relative;padding-right:40px;">
          <span class="sql-text">${escapeHTML(rec.sql || '')}</span>
          <button class="btn btn-ghost btn-sm copy-sql-btn"
            style="position:absolute;top:6px;right:6px;padding:4px 8px;"
            onclick="copySQL('sqlBlock_${i}', this)" title="Copy SQL" aria-label="Copy SQL">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
            </svg>
          </button>
        </div>
      </div>`;

    // Toggle expand
    el.querySelector('.rec-header').addEventListener('click', () => {
      const body    = el.querySelector('.rec-body');
      const chevron = el.querySelector('.rec-chevron');
      const isOpen  = el.classList.toggle('open');
      body.style.maxHeight = isOpen ? body.scrollHeight + 40 + 'px' : '0';
      body.style.padding   = isOpen ? '' : '0 16px';
      chevron.style.transform = isOpen ? 'rotate(180deg)' : '';
    });

    frag.appendChild(el);
  });
  list.appendChild(frag);
}

// ── Copy SQL ─────────────────────────────────────────────────
function copySQL(blockId, btn) {
  const block = document.getElementById(blockId);
  const sql   = block?.querySelector('.sql-text')?.textContent?.trim() || '';
  if (!sql) return;
  navigator.clipboard.writeText(sql).then(() => {
    btn.innerHTML = '✓';
    btn.style.color = 'var(--neon-green)';
    setTimeout(() => {
      btn.innerHTML = '<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
      btn.style.color = '';
    }, 1500);
  }).catch(() => {
    btn.innerHTML = '✗';
    setTimeout(() => { btn.innerHTML = 'Copy'; }, 1500);
  });
}

// ── Export ────────────────────────────────────────────────────
function exportJSON() {
  if (!_lastStats) { showGlobalBanner('info', 'Run an analysis first.'); return; }
  const data = { stats: _lastStats, recommendations: _lastRecs, explain_cache: _explainCache, exportedAt: new Date().toISOString() };
  downloadFile('db_tuner_report.json', JSON.stringify(data, null, 2), 'application/json');
}

function exportHTML() {
  if (!_lastStats) { showGlobalBanner('info', 'Run an analysis first.'); return; }
  const ts = new Date().toLocaleString();
  const issueRows = Object.entries(_lastStats.anti_patterns || {})
    .filter(([,v]) => v > 0)
    .map(([k,v]) => `<tr><td>${k.replace(/_/g,' ')}</td><td style="color:#f59e0b;">${v}</td></tr>`).join('');
  const recRows = (_lastRecs || []).map(r =>
    `<tr><td>${r.type}</td><td>${escapeHTML(r.table)}</td><td>${escapeHTML((r.columns||[]).join(', '))}</td><td><code>${escapeHTML(r.sql)}</code></td></tr>`
  ).join('');
  const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>DB Tuner Report</title>
<style>body{font-family:sans-serif;padding:32px;background:#0a0f1e;color:#e8f4ff}
h1{color:#00f0ff}h2{color:#5a7a99;font-size:14px;margin-top:24px}
table{width:100%;border-collapse:collapse;margin-top:8px}
th,td{text-align:left;padding:8px 12px;border-bottom:1px solid #1a2a3a;font-size:13px}
th{background:#0d1829;color:#5a7a99}code{font-family:monospace;font-size:11px;color:#00f0ff}</style>
</head><body>
<h1>DB Tuner Pro — Analysis Report</h1>
<p style="color:#3a5c78;font-size:13px;">Generated: ${ts}</p>
<h2>SUMMARY</h2>
<table><tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total Queries</td><td>${_lastStats.total_queries}</td></tr>
<tr><td>Recommendations</td><td>${(_lastRecs||[]).length}</td></tr></table>
<h2>ANTI-PATTERNS</h2>
<table><tr><th>Pattern</th><th>Count</th></tr>${issueRows || '<tr><td colspan="2">None detected</td></tr>'}</table>
<h2>RECOMMENDATIONS</h2>
<table><tr><th>Type</th><th>Table</th><th>Columns</th><th>SQL</th></tr>${recRows || '<tr><td colspan="4">No recommendations</td></tr>'}</table>
</body></html>`;
  downloadFile('db_tuner_report.html', html, 'text/html');
}

function downloadFile(name, content, type) {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const a   = Object.assign(document.createElement('a'), { href: url, download: name });
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);   // prevent memory leak
}

// ── Fingerprint Section ───────────────────────────────────────
const SEV_COLOR  = { critical: 'var(--neon-pink)', warning: 'var(--neon-amber)', ok: 'var(--neon-green)' };
const SCAN_BADGE = {
  'ALL': 'badge-red', 'Seq Scan': 'badge-red',
  'index': 'badge-neutral', 'range': 'badge-cyan',
  'ref': 'badge-cyan', 'eq_ref': 'badge-cyan', 'const': 'badge-green',
};

function renderFingerprintSection(fpData) {
  if (!fpData?.top_groups?.length) return;
  document.getElementById('fingerprintSection')?.classList.remove('hidden');

  const s = fpData.summary || {};
  const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  setEl('fpBadge',           `${s.unique_templates || 0} templates`);
  setEl('fpUniqueTemplates', (s.unique_templates || 0).toLocaleString());
  setEl('fpTotalExec',       (s.total_executions  || 0).toLocaleString());
  setEl('fpTotalRows',       (s.total_rows_est    || 0).toLocaleString());

  const list = document.getElementById('fpTemplatesList');
  if (!list) return;
  const frag = document.createDocumentFragment();

  fpData.top_groups.forEach((g, idx) => {
    const sevColor  = SEV_COLOR[g.severity]  || 'var(--muted)';
    const scanClass = SCAN_BADGE[g.scan_type] || 'badge-neutral';

    const card = document.createElement('div');
    card.className = 'card';
    card.style.cssText = `padding:12px 16px;border-left:2px solid ${sevColor};margin-bottom:6px;`;
    card.innerHTML = `
      <div style="display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap;">
        <span style="font-size:10px;font-weight:700;color:var(--text-subtle);min-width:22px;padding-top:1px;">#${idx+1}</span>
        <code style="font-family:var(--font-mono);font-size:11px;color:var(--foreground);flex:1;white-space:pre-wrap;word-break:break-word;">${escapeHTML(g.template || '')}</code>
        <div style="display:flex;gap:5px;flex-shrink:0;flex-wrap:wrap;align-items:center;">
          ${g.scan_type ? `<span class="badge ${scanClass}">${escapeHTML(g.scan_type)}</span>` : ''}
          <span class="badge badge-neutral">${(g.count||0)}×</span>
          <span class="badge badge-purple">${(g.rows_total||0).toLocaleString()} rows</span>
        </div>
      </div>`;
    frag.appendChild(card);
  });
  list.innerHTML = '';
  list.appendChild(frag);
}

// ── Benchmark ─────────────────────────────────────────────────
function openBenchmarkPicker() {
  document.getElementById('benchmarkPicker')?.classList.toggle('hidden');
}
function closeBenchmarkPicker() {
  document.getElementById('benchmarkPicker')?.classList.add('hidden');
}

function runBenchmark(groupId) {
  if (_analyzing) return;
  _analyzing = true;
  closeBenchmarkPicker();
  closeActiveStream();

  const runBtn = document.getElementById('runAnalysisBtn');
  const bmBtn  = document.getElementById('benchmarkBtn');
  const setBtn = (disabled) => {
    if (runBtn) runBtn.disabled = disabled;
    if (bmBtn)  bmBtn.disabled  = disabled;
  };

  showLoadingState();
  setBtn(true);
  _lastStats = null; _lastRecs = null; _explainCache = {};
  setProgress(0, 'Loading benchmark queries…', '');

  const url = '/api/benchmark/stream' + (groupId ? `?group_id=${encodeURIComponent(groupId)}` : '');
  let es;
  try { es = new EventSource(url); }
  catch(err) { showError('Could not open benchmark stream: ' + err.message); return; }
  _activeStream = es;

  es.onmessage = (event) => {
    let payload;
    try { payload = JSON.parse(event.data); } catch { return; }

    if (payload.type === 'progress') {
      const done = payload.done || 0;
      setProgress(Math.min(75, done * 5), undefined, `${done.toLocaleString()} queries parsed`);

    } else if (payload.type === 'progress_label') {
      setProgress(80, payload.label, undefined);

    } else if (payload.type === 'result') {
      setProgress(100, undefined, undefined);
      closeActiveStream();
      _lastStats    = payload.stats;
      _lastRecs     = payload.recommendations;
      _explainCache = payload.explain_cache || {};
      setTimeout(() => {
        populateDashboard(payload.stats, payload.recommendations, payload.fingerprint);
        showResultsState();
        setBtn(false);
        _analyzing = false;
        if (payload.is_benchmark) {
          showGlobalBanner('info',
            `⚡ Benchmark complete — EXPLAIN data for ${Object.keys(_explainCache).length} queries collected.`);
        }
      }, 200);

    } else if (payload.type === 'done') {
      closeActiveStream();
      setBtn(false);
      _analyzing = false;

    } else if (payload.type === 'error') {
      closeActiveStream();
      showError(payload.message || 'Benchmark stream error.');
    }
  };
  es.onerror = () => {
    closeActiveStream();
    showError('Benchmark stream disconnected.');
    setBtn(false);
    _analyzing = false;
  };
}

// ── DOMContentLoaded ──────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initEngineSwitch();
  initConnectionForm();
  initRunAnalysis();

  // Notify cyber-effects that DOM is ready for re-init
  window.dispatchEvent(new CustomEvent('analysisComplete'));
});

// ── Close stream on page unload (prevent memory leaks) ────────
window.addEventListener('beforeunload', closeActiveStream);
