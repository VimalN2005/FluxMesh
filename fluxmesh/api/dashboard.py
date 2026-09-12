"""Embedded web dashboard for FluxMesh cluster observability."""

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FluxMesh - Distributed Orchestration Dashboard</title>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --text: #c9d1d9;
      --text-bright: #f0f6fc;
      --text-muted: #8b949e;
      --accent: #58a6ff;
      --accent-glow: rgba(88, 166, 255, 0.15);
      --critical: #f85149;
      --high: #d29922;
      --default: #388bfd;
      --low: #8b949e;
      --success: #3fb950;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif; }
    body { background-color: var(--bg); color: var(--text); padding: 24px; min-height: 100vh; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; }
    .logo { display: flex; align-items: center; gap: 12px; }
    .logo-icon { width: 32px; height: 32px; background: linear-gradient(135deg, #1f6feb, #8957e5); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: bold; color: #fff; font-size: 18px; }
    .logo h1 { font-size: 22px; color: var(--text-bright); font-weight: 600; letter-spacing: -0.5px; }
    .tag { background: #238636; color: #fff; font-size: 11px; padding: 2px 8px; border-radius: 12px; font-weight: 600; text-transform: uppercase; }
    .btn { background: #238636; color: #fff; border: 1px solid rgba(240,246,252,0.1); padding: 8px 16px; border-radius: 6px; cursor: pointer; font-weight: 500; font-size: 14px; transition: all 0.2s; }
    .btn:hover { background: #2ea043; }
    .btn-secondary { background: #21262d; border-color: var(--border); color: var(--text-bright); }
    .btn-secondary:hover { background: #30363d; }
    .btn-replay { background: #1f6feb; font-size: 12px; padding: 4px 10px; }
    .grid-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 18px; }
    .stat-title { font-size: 13px; color: var(--text-muted); font-weight: 500; margin-bottom: 8px; }
    .stat-val { font-size: 30px; font-weight: 700; color: var(--text-bright); display: flex; align-items: center; gap: 8px; }
    .pulse-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--success); box-shadow: 0 0 8px var(--success); }
    
    .section-title { font-size: 16px; font-weight: 600; color: var(--text-bright); margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
    .cards-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
    @media (max-width: 900px) { .cards-row { grid-template-columns: 1fr; } }
    .panel { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }
    
    /* Queue Meters */
    .queue-item { margin-bottom: 14px; }
    .queue-label { display: flex; justify-content: space-between; font-size: 13px; font-weight: 500; margin-bottom: 6px; }
    .queue-bar-bg { height: 8px; background: #21262d; border-radius: 4px; overflow: hidden; }
    .queue-bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s ease; }
    .bar-critical { background: var(--critical); }
    .bar-high { background: var(--high); }
    .bar-default { background: var(--default); }
    .bar-low { background: var(--low); }
    
    /* Tables */
    table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }
    th { text-align: left; padding: 10px 12px; color: var(--text-muted); border-bottom: 1px solid var(--border); font-weight: 600; }
    td { padding: 12px; border-bottom: 1px solid var(--border); color: var(--text); }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .badge { padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-block; }
    .badge-PENDING { background: #388bfd22; color: #58a6ff; border: 1px solid #388bfd44; }
    .badge-RUNNING { background: #d2992222; color: #e3b341; border: 1px solid #d2992244; }
    .badge-COMPLETED { background: #23863622; color: #3fb950; border: 1px solid #23863644; }
    .badge-DEAD_LETTER { background: #da363322; color: #f85149; border: 1px solid #da363344; }
    .badge-SCHEDULED { background: #a371f722; color: #bc8cff; border: 1px solid #a371f744; }
    
    .progress-track { width: 90px; height: 6px; background: #21262d; border-radius: 3px; overflow: hidden; display: inline-block; vertical-align: middle; margin-right: 6px; }
    .progress-thumb { height: 100%; background: var(--success); }
    
    /* Dispatch Form */
    .form-group { margin-bottom: 12px; }
    label { display: block; font-size: 12px; color: var(--text-muted); margin-bottom: 4px; }
    input, select, textarea { width: 100%; background: #0d1117; border: 1px solid var(--border); color: var(--text-bright); padding: 8px 12px; border-radius: 6px; font-size: 13px; }
    input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-glow); }
  </style>
</head>
<body>

  <div class="header">
    <div class="logo">
      <div class="logo-icon">F</div>
      <div>
        <h1>FluxMesh Cluster Dashboard</h1>
        <div style="font-size: 12px; color: var(--text-muted);">Distributed Job Orchestration & Fault-Tolerant Scheduling</div>
      </div>
      <span class="tag">Active</span>
    </div>
    <div style="display: flex; gap: 8px;">
      <button class="btn btn-secondary" onclick="fetchData()">Refresh</button>
      <button class="btn" onclick="openDispatchModal()">+ Submit Job</button>
    </div>
  </div>

  <div class="grid-stats">
    <div class="stat-card">
      <div class="stat-title">Active Workers</div>
      <div class="stat-val" id="stat-workers">0 <span class="pulse-dot"></span></div>
    </div>
    <div class="stat-card">
      <div class="stat-title">Pending Jobs in Queue</div>
      <div class="stat-val" id="stat-pending">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-title">Scheduled / Delayed</div>
      <div class="stat-val" id="stat-delayed">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-title">Dead Letter Queue (DLQ)</div>
      <div class="stat-val" id="stat-dlq" style="color: var(--critical);">0</div>
    </div>
  </div>

  <div class="cards-row">
    <div class="panel">
      <div class="section-title">Priority Queue Depths</div>
      <div class="queue-item">
        <div class="queue-label"><span>Critical (P0)</span><span id="depth-critical">0</span></div>
        <div class="queue-bar-bg"><div class="queue-bar-fill bar-critical" id="bar-critical" style="width: 0%;"></div></div>
      </div>
      <div class="queue-item">
        <div class="queue-label"><span>High (P1)</span><span id="depth-high">0</span></div>
        <div class="queue-bar-bg"><div class="queue-bar-fill bar-high" id="bar-high" style="width: 0%;"></div></div>
      </div>
      <div class="queue-item">
        <div class="queue-label"><span>Default (P2)</span><span id="depth-default">0</span></div>
        <div class="queue-bar-bg"><div class="queue-bar-fill bar-default" id="bar-default" style="width: 0%;"></div></div>
      </div>
      <div class="queue-item">
        <div class="queue-label"><span>Low (P3)</span><span id="depth-low">0</span></div>
        <div class="queue-bar-bg"><div class="queue-bar-fill bar-low" id="bar-low" style="width: 0%;"></div></div>
      </div>
    </div>

    <div class="panel">
      <div class="section-title">Quick Dispatch Job</div>
      <form id="dispatch-form" onsubmit="handleDispatch(event)">
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
          <div class="form-group">
            <label>Task Name</label>
            <select id="f-task">
              <option value="batch_process">batch_process</option>
              <option value="webhook_delivery">webhook_delivery</option>
              <option value="heavy_computation">heavy_computation</option>
              <option value="flaky_pipeline">flaky_pipeline</option>
              <option value="failing_task">failing_task</option>
            </select>
          </div>
          <div class="form-group">
            <label>Priority</label>
            <select id="f-priority">
              <option value="0">CRITICAL (P0)</option>
              <option value="1">HIGH (P1)</option>
              <option value="2" selected>DEFAULT (P2)</option>
              <option value="3">LOW (P3)</option>
            </select>
          </div>
        </div>
        <div class="form-group">
          <label>Payload (JSON)</label>
          <input id="f-kwargs" value='{"items": ["item1", "item2", "item3"]}' />
        </div>
        <button type="submit" class="btn" style="width: 100%;">Dispatch to Cluster</button>
      </form>
    </div>
  </div>

  <div class="panel" style="margin-bottom: 24px;">
    <div class="section-title">Registered Workers</div>
    <table>
      <thead>
        <tr>
          <th>Worker ID</th>
          <th>Hostname / PID</th>
          <th>Concurrency</th>
          <th>Active</th>
          <th>Completed</th>
          <th>Failed</th>
          <th>Uptime</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody id="workers-body">
        <tr><td colspan="8" style="text-align: center; color: var(--text-muted);">No workers active</td></tr>
      </tbody>
    </table>
  </div>

  <div class="panel" style="margin-bottom: 24px;">
    <div class="section-title">Dead Letter Queue (DLQ)</div>
    <table>
      <thead>
        <tr>
          <th>Job ID</th>
          <th>Task</th>
          <th>Attempts</th>
          <th>Error</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody id="dlq-body">
        <tr><td colspan="5" style="text-align: center; color: var(--text-muted);">DLQ is clear (0 failures)</td></tr>
      </tbody>
    </table>
  </div>

  <script>
    async function fetchData() {
      try {
        const [metricsRes, workersRes, dlqRes] = await Promise.all([
          fetch('/api/v1/metrics'),
          fetch('/api/v1/workers'),
          fetch('/api/v1/dlq')
        ]);
        const metrics = await metricsRes.json();
        const workers = await workersRes.json();
        const dlq = await dlqRes.json();

        // Update stats
        document.getElementById('stat-workers').innerHTML = `${workers.length} <span class="pulse-dot"></span>`;
        document.getElementById('stat-pending').innerText = metrics.pending_jobs;
        document.getElementById('stat-dlq').innerText = dlq.length;
        
        // Queue depths
        const q = metrics.queue_depths || {};
        const maxQ = Math.max(1, (q.critical||0) + (q.high||0) + (q.default||0) + (q.low||0));
        ['critical', 'high', 'default', 'low'].forEach(lvl => {
          const val = q[lvl] || 0;
          document.getElementById(`depth-${lvl}`).innerText = val;
          document.getElementById(`bar-${lvl}`).style.width = `${Math.min(100, (val / maxQ) * 100)}%`;
        });

        // Workers Table
        const wBody = document.getElementById('workers-body');
        if (workers.length === 0) {
          wBody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted);">No workers active</td></tr>';
        } else {
          wBody.innerHTML = workers.map(w => `
            <tr>
              <td><code>${w.worker_id}</code></td>
              <td>${w.hostname} (pid:${w.pid})</td>
              <td>${w.concurrency}</td>
              <td><strong>${w.active_tasks}</strong></td>
              <td style="color: var(--success);">${w.completed_tasks}</td>
              <td style="color: var(--critical);">${w.failed_tasks}</td>
              <td>${w.uptime_seconds}s</td>
              <td><span class="badge badge-COMPLETED">healthy</span></td>
            </tr>
          `).join('');
        }

        // DLQ Table
        const dlqBody = document.getElementById('dlq-body');
        if (dlq.length === 0) {
          dlqBody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">DLQ is clear (0 failures)</td></tr>';
        } else {
          dlqBody.innerHTML = dlq.map(j => `
            <tr>
              <td><code>${j.id.slice(0, 8)}...</code></td>
              <td><strong>${j.task}</strong></td>
              <td>${j.attempts} / ${j.max_retries}</td>
              <td style="color: var(--critical);">${(j.error || 'Unknown error').slice(0, 60)}</td>
              <td><button class="btn btn-replay" onclick="replayJob('${j.id}')">Replay</button></td>
            </tr>
          `).join('');
        }

      } catch (err) {
        console.error('Failed to fetch dashboard data:', err);
      }
    }

    async function handleDispatch(e) {
      e.preventDefault();
      const task = document.getElementById('f-task').value;
      const priority = parseInt(document.getElementById('f-priority').value);
      let kwargs = {};
      try {
        kwargs = JSON.parse(document.getElementById('f-kwargs').value);
      } catch (err) {
        alert('Invalid JSON in payload');
        return;
      }

      await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task, priority, kwargs })
      });
      fetchData();
    }

    async function replayJob(jobId) {
      await fetch(`/api/v1/dlq/${jobId}/replay`, { method: 'POST' });
      fetchData();
    }

    setInterval(fetchData, 2000);
    fetchData();
  </script>
</body>
</html>
"""
