from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, HTMLResponse
import asyncio
import logging
import yaml

import phos_queue as q
import control
import worker

app = FastAPI()
logger = logging.getLogger('phos-watch-web')
LOGFILE = 'phos_watch.log'


@app.get('/status')
async def status():
    return JSONResponse({'queue_length': q.qlen()})


@app.get('/config')
async def get_config():
    return JSONResponse(worker.load_config())


@app.post('/config')
async def post_config(req: Request):
    try:
        data = await req.json()
        if data is None:
            data = {}
        # Persist as YAML for worker consumption
        with open('config.yaml', 'w', encoding='utf-8') as f:
            yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
        return JSONResponse({'ok': True, 'config': data})
    except Exception as e:
        logger.exception('Failed to write config.yaml')
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)


@app.get('/queue')
async def get_queue():
    items = q.list_items()
    return JSONResponse({'count': len(items), 'items': items})


@app.get('/control')
async def get_control():
    return JSONResponse(control.get_state())


@app.post('/control')
async def post_control(req: Request):
    data = await req.json()
    paused = bool(data.get('paused', False))
    control.set_paused(paused)
    return JSONResponse(control.get_state())


@app.websocket('/ws/logs')
async def websocket_logs(ws: WebSocket):
        await ws.accept()
        try:
                try:
                        with open(LOGFILE, 'r', encoding='utf-8') as f:
                                lines = f.readlines()[-200:]
                except FileNotFoundError:
                        lines = []

                for line in lines:
                        await ws.send_text(line.rstrip('\n'))

                f = open(LOGFILE, 'r', encoding='utf-8')
                f.seek(0, 2)
                try:
                        while True:
                                where = f.tell()
                                line = f.readline()
                                if not line:
                                        await asyncio.sleep(0.5)
                                        f.seek(where)
                                        continue
                                await ws.send_text(line.rstrip('\n'))
                finally:
                        f.close()
        except WebSocketDisconnect:
                pass


@app.get('/')
async def index():
        html = '''
<html>
    <head>
        <title>phos-watch Admin</title>
        <meta charset="utf-8" />
        <style>
            body { font-family: Arial, sans-serif; margin: 16px; background: #f6f7fb; color: #1f2937; }
            .grid { display: grid; gap: 16px; grid-template-columns: 1fr; }
            .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,.04); }
            .muted { color: #6b7280; }
            pre, textarea { width: 100%; box-sizing: border-box; }
            pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 10px; min-height: 120px; overflow: auto; }
            textarea { min-height: 240px; font-family: Consolas, monospace; border-radius: 10px; border: 1px solid #cbd5e1; padding: 12px; }
            button { border: 0; background: #2563eb; color: white; padding: 8px 12px; border-radius: 8px; cursor: pointer; }
            button.secondary { background: #475569; }
            .row { display:flex; gap: 8px; flex-wrap: wrap; align-items: center; }
        </style>
    </head>
    <body>
        <h2>phos-watch Admin</h2>
        <p class="muted">監看路徑、可轉換格式與副檔名標準化都可在此調整。</p>
        <div class="grid">
            <div class="card">
                <div class="row">
                    <strong>Queue length:</strong> <span id="qlen">...</span>
                    <strong>Pause state:</strong> <span id="pausedState">...</span>
                    <button id="togglePause">Toggle Pause</button>
                    <button id="refreshQueue" class="secondary">Refresh Queue</button>
                </div>
            </div>

            <div class="card">
                <h3>Queue Items</h3>
                <pre id="queueItems">Loading...</pre>
            </div>

            <div class="card">
                <h3>Configuration</h3>
                <p class="muted">可調整 watch_paths、recursive、target_format、source_extensions、extension_aliases。</p>
                <form id="configForm">
                    <div style="margin-bottom:8px;">
                        <label>Watch paths (comma-separated):<br/>
                            <input type="text" id="watch_paths" style="width:100%" />
                        </label>
                    </div>
                    <div style="margin-bottom:8px;">
                        <label><input type="checkbox" id="recursive" /> Recursive watch</label>
                    </div>
                    <div style="margin-bottom:8px;">
                        <label>Source extensions (comma-separated):<br/>
                            <input type="text" id="source_extensions" style="width:100%" />
                        </label>
                    </div>
                    <div style="margin-bottom:8px;">
                        <label>Target format (e.g. jpg):<br/>
                            <input type="text" id="target_format" style="width:200px" />
                        </label>
                    </div>
                    <div style="margin-bottom:8px;">
                        <label>Extension aliases (JSON object):<br/>
                            <textarea id="extension_aliases" style="width:100%; min-height:80px; font-family: Consolas, monospace;"></textarea>
                        </label>
                    </div>
                    <div class="row">
                        <button type="button" id="loadConfig" class="secondary">Load Config</button>
                        <button type="button" id="saveConfig">Save Config</button>
                    </div>
                </form>
            </div>

            <div class="card">
                <h3>Logs</h3>
                <pre id="logs"></pre>
            </div>
        </div>
        <script>
            function pretty(obj) {
                try { return JSON.stringify(obj, null, 2); } catch (e) { return String(obj); }
            }

            async function updateQ() {
                try {
                    const res = await fetch('/status');
                    const j = await res.json();
                    document.getElementById('qlen').innerText = j.queue_length;
                } catch (e) { console.error(e); }
            }

            async function loadControl() {
                try {
                    const res = await fetch('/control');
                    const j = await res.json();
                    document.getElementById('pausedState').innerText = j.paused ? 'PAUSED' : 'RUNNING';
                } catch (e) { console.error(e); }
            }

            async function refreshQueue() {
                try {
                    const res = await fetch('/queue');
                    const j = await res.json();
                    document.getElementById('queueItems').textContent = pretty(j);
                    document.getElementById('qlen').innerText = j.count;
                } catch (e) { console.error(e); }
            }

            async function togglePause() {
                try {
                    const paused = document.getElementById('pausedState').innerText !== 'PAUSED';
                    const res = await fetch('/control', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({paused: paused})
                    });
                    const j = await res.json();
                    document.getElementById('pausedState').innerText = j.paused ? 'PAUSED' : 'RUNNING';
                } catch (e) { console.error(e); }
            }

            async function loadConfig() {
                try {
                    const res = await fetch('/config');
                    const j = await res.json();
                    // populate form fields
                    document.getElementById('watch_paths').value = (j.watch_paths || []).join(', ');
                    document.getElementById('recursive').checked = !!(j && j.recursive);
                    document.getElementById('source_extensions').value = (j.source_extensions || []).join(', ');
                    document.getElementById('target_format').value = j.target_format || '';
                    document.getElementById('extension_aliases').value = JSON.stringify(j.extension_aliases || {}, null, 2);
                } catch (e) { console.error(e); }
            }

            function parseCSVToList(s) {
                if (!s) return [];
                return s.split(',').map(x => x.trim()).filter(x => x.length>0);
            }

            async function saveConfig() {
                try {
                    const cfg = {};
                    cfg.watch_paths = parseCSVToList(document.getElementById('watch_paths').value);
                    cfg.recursive = document.getElementById('recursive').checked;
                    cfg.source_extensions = parseCSVToList(document.getElementById('source_extensions').value).map(e => e.replace(/^\./, '').toLowerCase());
                    cfg.target_format = (document.getElementById('target_format').value || '').trim();
                    try {
                        cfg.extension_aliases = JSON.parse(document.getElementById('extension_aliases').value || '{}');
                    } catch (e) {
                        alert('extension_aliases must be valid JSON');
                        return;
                    }

                    const res = await fetch('/config', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(cfg)
                    });
                    const j = await res.json();
                    if (!j.ok) {
                        alert('Save failed: ' + (j.error || 'unknown error'));
                        return;
                    }
                    alert('Saved');
                } catch (e) { console.error(e); }
            }

            document.getElementById('refreshQueue').addEventListener('click', refreshQueue);
            document.getElementById('togglePause').addEventListener('click', togglePause);
            document.getElementById('loadConfig').addEventListener('click', loadConfig);
            document.getElementById('saveConfig').addEventListener('click', saveConfig);

            setInterval(updateQ, 2000);
            setInterval(loadControl, 2500);
            setInterval(refreshQueue, 5000);
            updateQ();
            loadControl();
            refreshQueue();
            loadConfig();

            const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/logs');
            const logs = document.getElementById('logs');
            ws.onmessage = (ev) => { logs.textContent += ev.data + '\n'; logs.scrollTop = logs.scrollHeight; };
        </script>
    </body>
</html>
'''
        return HTMLResponse(html)
