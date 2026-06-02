from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import logging
import yaml

import phos_queue as q
import control
import worker

app = FastAPI()
logger = logging.getLogger('phos-watch-web')
LOGFILE = 'phos_watch.log'

with open(LOGFILE, 'w', encoding='utf-8') as f:
    pass

# serve static files (locales will live under static/locales)
app.mount('/static', StaticFiles(directory='static'), name='static')

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

@app.post('/queue/remove')
async def post_queue_remove(req: Request):
    try:
        data = await req.json()
        item_id = data.get('id')
        ok = q.remove(item_id)
        return JSONResponse({'ok': ok})
    except Exception as e:
        logger.exception('Failed to remove queue item')
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)

@app.post('/queue/requeue')
async def post_queue_requeue(req: Request):
    try:
        data = await req.json()
        item_id = data.get('id')
        ok = q.requeue(item_id)
        return JSONResponse({'ok': ok})
    except Exception as e:
        logger.exception('Failed to requeue item')
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)

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
            <title data-i18n="title">phos-watch Admin</title>
            <meta charset="utf-8" />
            <style>
                body { font-family: Arial, sans-serif; margin: 16px; background: #f6f7fb; color: #1f2937; }
                .grid { display: grid; gap: 16px; grid-template-columns: 1fr; }
                .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,.04); }
                .muted { color: #6b7280; }
                pre, textarea, input, select { width: 100%; box-sizing: border-box; }
                pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 10px; min-height: 120px; overflow: auto; }
                textarea { min-height: 240px; font-family: Consolas, monospace; border-radius: 10px; border: 1px solid #cbd5e1; padding: 12px; }
                input, select { border-radius: 10px; border: 1px solid #cbd5e1; padding: 10px 12px; }
                button { border: 0; background: #2563eb; color: white; padding: 8px 12px; border-radius: 8px; cursor: pointer; }
                button.secondary { background: #475569; }
                .row { display:flex; gap: 8px; flex-wrap: wrap; align-items: center; }
                .field { display: grid; gap: 8px; margin-bottom: 8px; }
                .field label { font-weight: 600; }
                .field-help { color: #64748b; font-size: 13px; }
                .list-items { display: flex; flex-wrap: wrap; gap: 8px; min-height: 44px; padding: 10px; border: 1px solid #cbd5e1; border-radius: 10px; background: #f8fafc; }
                .list-empty { color: #94a3b8; align-self: center; }
                .chip { display: inline-flex; align-items: center; gap: 8px; padding: 6px 10px; border-radius: 999px; background: #dbeafe; color: #1e3a8a; font-size: 14px; }
                .chip button { background: transparent; color: inherit; padding: 0; border-radius: 999px; line-height: 1; font-size: 16px; }
                button[disabled] { opacity: 0.6; cursor: not-allowed; }
                .spinner { display:inline-block; width:14px; height:14px; border:2px solid rgba(255,255,255,0.3); border-top-color: white; border-radius:50%; animation: spin 1s linear infinite; vertical-align:middle; margin-right:6px; }
                @keyframes spin { to { transform: rotate(360deg); } }
                .lang { margin-left: auto; }
            </style>
            <!-- i18next (CDN) -->
            <script src="https://unpkg.com/i18next@22.4.15/dist/umd/i18next.min.js"></script>
            <script src="https://unpkg.com/i18next-http-backend@4.0.0/i18nextHttpBackend.min.js"></script>
            <script src="https://unpkg.com/i18next-browser-languagedetector@6.1.6/i18nextBrowserLanguageDetector.min.js"></script>
        </head>
        <body>
            <div style="display:flex; align-items:center; gap:12px;">
                <h2 data-i18n="heading_admin">phos-watch Admin</h2>
                <select id="langSel" class="lang" aria-label="language selector">
                    <option value="zh-TW">中文 (繁體)</option>
                    <option value="en">English</option>
                </select>
            </div>
            <p class="muted" data-i18n="desc_watch_paths">監看路徑、可轉換格式與副檔名標準化都可在此調整。</p>
            <div class="grid">
                <div class="card">
                    <div class="row">
                        <strong data-i18n="queue_length_label">Queue length:</strong> <span id="qlen">...</span>
                        <strong data-i18n="pause_state_label">Pause state:</strong> <span id="pausedState">...</span>
                        <button id="togglePause" data-i18n="toggle_pause">Toggle Pause</button>
                        <button id="refreshQueue" class="secondary" data-i18n="refresh_queue">Refresh Queue</button>
                    </div>
                </div>

                <div class="card">
                    <h3 data-i18n="queue_items">Queue Items</h3>
                    <div id="queueItems" data-i18n="loading">Loading...</div>
                </div>

                <div class="card">
                    <h3 data-i18n="configuration">Configuration</h3>
                    <p class="muted" data-i18n="config_desc">可調整 watch_paths、recursive、target_format、source_extensions、extension_aliases。</p>
                    <form id="configForm">
                        <div class="field">
                            <label for="watch_paths_input"><span data-i18n="watch_paths_label">Watch paths</span></label>
                            <div id="watch_paths_list" class="list-items"></div>
                            <div class="row">
                                <input type="text" id="watch_paths_input" data-i18n-placeholder="watch_paths_placeholder" placeholder="Add one watched folder at a time" />
                                <button type="button" id="watch_paths_add" class="secondary" data-i18n="add_item">Add</button>
                            </div>
                        </div>
                        <div class="field">
                            <label><input type="checkbox" id="recursive" /> <span data-i18n="recursive_watch">Recursive watch</span></label>
                        </div>
                        <div class="field">
                            <label for="source_extensions_select"><span data-i18n="source_extensions_label">Source extensions</span></label>
                            <div id="source_extensions_list" class="list-items"></div>
                            <div class="row">
                                <select id="source_extensions_select"></select>
                                <input type="text" id="source_extensions_custom" data-i18n-placeholder="source_extensions_placeholder" placeholder="Add a source extension" />
                                <button type="button" id="source_extensions_add" class="secondary" data-i18n="add_item">Add</button>
                            </div>
                        </div>
                        <div class="field">
                            <label for="target_format"><span data-i18n="target_format_label">Target suffix</span></label>
                            <select id="target_format"></select>
                            <div class="field-help" data-i18n="target_format_hint">Choose the exact output suffix, including case.</div>
                            <div class="row">
                                <input type="text" id="target_format_custom" data-i18n-placeholder="target_format_custom_placeholder" placeholder="Type a suffix such as JPG or jpeg" />
                                <button type="button" id="target_format_add" class="secondary" data-i18n="add_item">Add</button>
                            </div>
                        </div>
                        <div class="field">
                            <label for="extension_aliases"><span data-i18n="extension_aliases_label">Extension aliases (JSON object):</span></label>
                            <textarea id="extension_aliases" style="width:100%; min-height:120px; font-family: Consolas, monospace;"></textarea>
                        </div>
                        <div class="row">
                            <button type="button" id="loadConfig" class="secondary" data-i18n="load_config">Load Config</button>
                            <button type="button" id="saveConfig" data-i18n="save_config">Save Config</button>
                        </div>
                    </form>
                </div>

                <div class="card">
                    <h3 data-i18n="logs">Logs</h3>
                    <pre id="logs"></pre>
                </div>
            </div>
                <div id="toast" style="position:fixed; right:16px; bottom:16px; z-index:9999;"></div>

            <script>
                // i18next initialization
                i18next.use(i18nextHttpBackend).use(i18nextBrowserLanguageDetector).init({
                    fallbackLng: 'zh-TW',
                    debug: false,
                    backend: { loadPath: '/static/locales/{{lng}}.json' }
                }, function(err, t) {
                    if (err) console.error('i18next init error', err);
                    translatePage();
                });

                function translatePage() {
                    // set document title
                    try { document.title = i18next.t('title'); } catch(e){}
                    document.querySelectorAll('[data-i18n]').forEach(el => {
                        const key = el.getAttribute('data-i18n');
                        try { el.innerText = i18next.t(key); } catch(e){}
                    });
                    document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
                        const key = el.getAttribute('data-i18n-placeholder');
                        try { el.setAttribute('placeholder', i18next.t(key)); } catch(e){}
                    });
                }

                document.getElementById('langSel').addEventListener('change', (ev) => {
                    const lng = ev.target.value;
                    i18next.changeLanguage(lng).then(() => { localStorage.setItem('phos_lang', lng); translatePage(); });
                });

                // Persist selection from localStorage if present
                const saved = localStorage.getItem('phos_lang');
                if (saved) {
                    document.getElementById('langSel').value = saved;
                    i18next.changeLanguage(saved).then(translatePage).catch(()=>{});
                }

                function t(key) { try { return i18next.t(key); } catch(e) { return key; } }

                function normalizeTextList(value) {
                    if (Array.isArray(value)) return value.map(item => String(item).trim()).filter(Boolean);
                    if (typeof value === 'string') return value.split(',').map(item => item.trim()).filter(Boolean);
                    return [];
                }

                function normalizeSuffix(value) {
                    return String(value || '').trim().replace(/^\\./, '');
                }

                function getListValues(containerId) {
                    const container = document.getElementById(containerId);
                    if (!container) return [];
                    return Array.from(container.querySelectorAll('.chip')).map(chip => chip.dataset.value || '').filter(Boolean);
                }

                function renderList(containerId, values, formatter) {
                    const container = document.getElementById(containerId);
                    if (!container) return;
                    const normalized = [];
                    const seen = new Set();
                    normalizeTextList(values).forEach(value => {
                        const formatted = formatter ? formatter(value) : value;
                        if (!formatted || seen.has(formatted)) return;
                        seen.add(formatted);
                        normalized.push(formatted);
                    });

                    container.innerHTML = '';
                    if (!normalized.length) {
                        const empty = document.createElement('span');
                        empty.className = 'list-empty';
                        empty.textContent = t('list_empty');
                        container.appendChild(empty);
                        return;
                    }

                    normalized.forEach(value => {
                        const chip = document.createElement('span');
                        chip.className = 'chip';
                        chip.dataset.value = value;
                        const label = document.createElement('span');
                        label.textContent = value;
                        const removeBtn = document.createElement('button');
                        removeBtn.type = 'button';
                        removeBtn.setAttribute('aria-label', t('remove_item'));
                        removeBtn.textContent = '×';
                        removeBtn.onclick = () => {
                            const remaining = getListValues(containerId).filter(item => item !== value);
                            renderList(containerId, remaining, formatter);
                        };
                        chip.appendChild(label);
                        chip.appendChild(removeBtn);
                        container.appendChild(chip);
                    });
                }

                function addListItem(containerId, inputId, formatter, clearInput = true) {
                    const input = document.getElementById(inputId);
                    if (!input) return;
                    const value = formatter ? formatter(input.value) : String(input.value || '').trim();
                    if (!value) return;
                    const current = getListValues(containerId);
                    if (!current.includes(value)) {
                        renderList(containerId, current.concat([value]), formatter);
                    }
                    if (clearInput) input.value = '';
                    input.focus();
                }

                function collectSuffixOptions(cfg) {
                    const values = [];
                    const seen = new Set();
                    const push = (value) => {
                        const normalized = normalizeSuffix(value);
                        if (!normalized || seen.has(normalized)) return;
                        seen.add(normalized);
                        values.push(normalized);
                    };

                    ['jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'avif', 'tif', 'tiff', 'JPG', 'JPEG', 'PNG', 'WEBP', 'GIF', 'BMP', 'AVIF', 'TIF', 'TIFF'].forEach(push);
                    const aliases = (cfg && cfg.extension_aliases) ? cfg.extension_aliases : {};
                    Object.entries(aliases).forEach(([canonical, aliasList]) => {
                        push(canonical);
                        if (Array.isArray(aliasList)) {
                            aliasList.forEach(push);
                        }
                    });
                    push(cfg && cfg.target_format);
                    return values;
                }

                function collectSourceOptions(cfg) {
                    const values = [];
                    const seen = new Set();
                    const push = (value) => {
                        const normalized = normalizeSuffix(value).toLowerCase();
                        if (!normalized || seen.has(normalized)) return;
                        seen.add(normalized);
                        values.push(normalized);
                    };

                    ['jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'avif', 'tif', 'tiff'].forEach(push);
                    const aliases = (cfg && cfg.extension_aliases) ? cfg.extension_aliases : {};
                    Object.entries(aliases).forEach(([canonical, aliasList]) => {
                        push(canonical);
                        if (Array.isArray(aliasList)) {
                            aliasList.forEach(push);
                        }
                    });
                    push(cfg && cfg.target_format);
                    return values;
                }

                function renderSelect(selectId, values, selected) {
                    const select = document.getElementById(selectId);
                    if (!select) return;
                    select.innerHTML = '';
                    values.forEach(value => {
                        const option = document.createElement('option');
                        option.value = value;
                        option.textContent = value;
                        select.appendChild(option);
                    });
                    const normalizedSelected = normalizeSuffix(selected);
                    if (normalizedSelected && !values.includes(normalizedSelected)) {
                        const option = document.createElement('option');
                        option.value = normalizedSelected;
                        option.textContent = normalizedSelected;
                        select.appendChild(option);
                    }
                    select.value = normalizedSelected || values[0] || '';
                }

                function appendSuffixOption(selectId, value) {
                    const select = document.getElementById(selectId);
                    if (!select) return;
                    const normalized = normalizeSuffix(value);
                    if (!normalized) return;
                    const exists = Array.from(select.options).some(option => option.value === normalized);
                    if (!exists) {
                        const option = document.createElement('option');
                        option.value = normalized;
                        option.textContent = normalized;
                        select.appendChild(option);
                    }
                    select.value = normalized;
                }

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
                        document.getElementById('pausedState').innerText = j.paused ? t('paused') : t('running');
                    } catch (e) { console.error(e); }
                }

                async function refreshQueue() {
                    try {
                        const res = await fetch('/queue');
                        const j = await res.json();
                        document.getElementById('qlen').innerText = j.count;
                        const container = document.getElementById('queueItems');
                        container.innerHTML = '';
                        if (!j.items || j.items.length===0) {
                            container.textContent = t('queue_empty');
                            return;
                        }
                        j.items.forEach(item => {
                            const el = document.createElement('div');
                            el.style.padding = '6px 0';
                            el.style.borderBottom = '1px solid #eef2ff';
                            const path = document.createElement('div');
                            path.textContent = item.path || item['path'] || JSON.stringify(item);
                            path.style.fontFamily = 'monospace';
                            const meta = document.createElement('div');
                            meta.className = 'muted';
                            meta.textContent = t('id_label') + (item.id || item['id'] || '') ;
                            const btnRow = document.createElement('div');
                            btnRow.className = 'row';
                            const retryBtn = document.createElement('button');
                            retryBtn.textContent = t('requeue');
                            retryBtn.onclick = () => { requeueItem(item.id || item['id'], retryBtn); };
                            const removeBtn = document.createElement('button');
                            removeBtn.textContent = t('remove');
                            removeBtn.className = 'secondary';
                            removeBtn.onclick = () => { removeItem(item.id || item['id'], removeBtn); };
                            btnRow.appendChild(retryBtn);
                            btnRow.appendChild(removeBtn);
                            el.appendChild(path);
                            el.appendChild(meta);
                            el.appendChild(btnRow);
                            container.appendChild(el);
                        });
                    } catch (e) { console.error(e); }
                }

                async function removeItem(id, btn) {
                    const orig = btn.textContent;
                    try {
                        btn.disabled = true;
                        btn.innerHTML = '<span class="spinner"></span>' + orig;
                        const res = await fetch('/queue/remove', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})});
                        const j = await res.json();
                        if (!j.ok) {
                            showToast(t('remove_failed'), true);
                        } else {
                            showToast(t('removed'), false);
                        }
                        await refreshQueue();
                    } catch(e){ console.error(e); showToast(t('remove_error'), true); }
                    finally { try { btn.disabled = false; btn.textContent = orig; } catch(_){} }
                }

                async function requeueItem(id, btn) {
                    const orig = btn.textContent;
                    try {
                        btn.disabled = true;
                        btn.innerHTML = '<span class="spinner"></span>' + orig;
                        const res = await fetch('/queue/requeue', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})});
                        const j = await res.json();
                        if (!j.ok) {
                            showToast(t('requeue_failed'), true);
                        } else {
                            showToast(t('requeued'), false);
                        }
                        await refreshQueue();
                    } catch(e){ console.error(e); showToast(t('requeue_error'), true); }
                    finally { try { btn.disabled = false; btn.textContent = orig; } catch(_){} }
                }

                function showToast(msg, isError) {
                    const tdiv = document.createElement('div');
                    tdiv.textContent = msg;
                    tdiv.style.background = isError ? '#ef4444' : '#10b981';
                    tdiv.style.color = 'white';
                    tdiv.style.padding = '8px 12px';
                    tdiv.style.marginTop = '8px';
                    tdiv.style.borderRadius = '8px';
                    document.getElementById('toast').appendChild(tdiv);
                    setTimeout(() => { tdiv.style.transition = 'opacity 0.4s'; tdiv.style.opacity = '0'; setTimeout(()=>tdiv.remove(),400); }, 3000);
                }

                async function togglePause() {
                    try {
                        const paused = document.getElementById('pausedState').innerText !== t('paused');
                        const res = await fetch('/control', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({paused: paused})
                        });
                        const j = await res.json();
                        document.getElementById('pausedState').innerText = j.paused ? t('paused') : t('running');
                    } catch (e) { console.error(e); }
                }

                async function loadConfig() {
                    try {
                        const res = await fetch('/config');
                        const j = await res.json();
                        // populate form fields
                        renderList('watch_paths_list', j.watch_paths || [], value => String(value || '').trim());
                        document.getElementById('recursive').checked = !!(j && j.recursive);
                        renderList('source_extensions_list', j.source_extensions || [], value => normalizeSuffix(value).toLowerCase());
                        renderSelect('source_extensions_select', collectSourceOptions(j), (j.source_extensions && j.source_extensions[0]) || 'jpg');
                        renderSelect('target_format', collectSuffixOptions(j), j.target_format || '');
                        document.getElementById('extension_aliases').value = JSON.stringify(j.extension_aliases || {}, null, 2);
                    } catch (e) { console.error(e); }
                }

                async function saveConfig() {
                    const btn = document.getElementById('saveConfig');
                    const origText = btn.textContent;
                    try {
                        btn.disabled = true;
                        btn.innerHTML = '<span class="spinner"></span>' + origText;
                        const cfg = {};
                        cfg.watch_paths = getListValues('watch_paths_list');
                        cfg.recursive = document.getElementById('recursive').checked;
                        cfg.source_extensions = getListValues('source_extensions_list').map(e => normalizeSuffix(e).toLowerCase());
                        cfg.target_format = normalizeSuffix(document.getElementById('target_format').value || '');
                        try {
                            cfg.extension_aliases = JSON.parse(document.getElementById('extension_aliases').value || '{}');
                        } catch (e) {
                            showToast(t('extension_aliases_invalid_json'), true);
                            return;
                        }

                        const res = await fetch('/config', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(cfg)
                        });
                        const j = await res.json();
                        if (!j.ok) {
                            showToast(t('save_failed') + ': ' + (j.error || 'unknown error'), true);
                            return;
                        }
                        showToast(t('saved'), false);
                    } catch (e) { console.error(e); showToast(t('save_error'), true); }
                    finally { btn.disabled = false; try { btn.textContent = origText; } catch(_){} }
                }

                function bindListControls() {
                    const watchInput = document.getElementById('watch_paths_input');
                    const sourceInput = document.getElementById('source_extensions_custom');
                    const targetCustomInput = document.getElementById('target_format_custom');

                    if (watchInput) {
                        watchInput.addEventListener('keydown', (ev) => {
                            if (ev.key === 'Enter') {
                                ev.preventDefault();
                                addListItem('watch_paths_list', 'watch_paths_input', value => String(value || '').trim());
                            }
                        });
                    }
                    if (sourceInput) {
                        sourceInput.addEventListener('keydown', (ev) => {
                            if (ev.key === 'Enter') {
                                ev.preventDefault();
                                addListItem('source_extensions_list', 'source_extensions_custom', value => normalizeSuffix(value).toLowerCase());
                            }
                        });
                    }
                    if (targetCustomInput) {
                        targetCustomInput.addEventListener('keydown', (ev) => {
                            if (ev.key === 'Enter') {
                                ev.preventDefault();
                                appendSuffixOption('target_format', targetCustomInput.value);
                                targetCustomInput.value = '';
                            }
                        });
                    }

                    const watchBtn = document.getElementById('watch_paths_add');
                    const sourceBtn = document.getElementById('source_extensions_add');
                    const targetBtn = document.getElementById('target_format_add');
                    if (watchBtn) watchBtn.addEventListener('click', () => addListItem('watch_paths_list', 'watch_paths_input', value => String(value || '').trim()));
                    if (sourceBtn) sourceBtn.addEventListener('click', () => {
                        const customValue = sourceInput ? String(sourceInput.value || '').trim() : '';
                        const select = document.getElementById('source_extensions_select');
                        const selectedValue = customValue || (select ? select.value : '');
                        if (!selectedValue) return;
                        const normalized = normalizeSuffix(selectedValue).toLowerCase();
                        const current = getListValues('source_extensions_list');
                        if (!current.includes(normalized)) {
                            renderList('source_extensions_list', current.concat([normalized]), value => normalizeSuffix(value).toLowerCase());
                        }
                        if (sourceInput) sourceInput.value = '';
                        if (select) select.value = normalized;
                        sourceInput && sourceInput.focus();
                    });
                    if (targetBtn) targetBtn.addEventListener('click', () => {
                        appendSuffixOption('target_format', targetCustomInput ? targetCustomInput.value : '');
                        if (targetCustomInput) targetCustomInput.value = '';
                    });
                }

                document.getElementById('refreshQueue').addEventListener('click', refreshQueue);
                document.getElementById('togglePause').addEventListener('click', togglePause);
                document.getElementById('loadConfig').addEventListener('click', loadConfig);
                document.getElementById('saveConfig').addEventListener('click', saveConfig);
                bindListControls();

                setInterval(updateQ, 2000);
                setInterval(loadControl, 2500);
                setInterval(refreshQueue, 5000);
                updateQ();
                loadControl();
                refreshQueue();
                loadConfig();

                const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/logs');
                const logs = document.getElementById('logs');
                ws.onmessage = (ev) => { logs.textContent += ev.data + '\\n'; logs.scrollTop = logs.scrollHeight; };
            </script>
        </body>
    </html>
    '''
    return HTMLResponse(html)
