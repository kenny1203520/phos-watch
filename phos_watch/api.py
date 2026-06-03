from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import logging
import yaml
import os

from . import phos_queue as q
from . import control
from . import worker
from . import updater

app = FastAPI()
logger = logging.getLogger(__name__)
LOGFILE = os.getenv('PHOS_LOG_FILE', os.path.join('logs', 'phos_watch.log'))

# Check if log file exists, if not create it. Avoid truncating to preserve log history/rotation.
log_dir = os.path.dirname(LOGFILE)
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
if not os.path.exists(LOGFILE):
    with open(LOGFILE, 'w', encoding='utf-8') as f:
        pass

# serve static files (locales will live under static/locales)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
app.mount('/static', StaticFiles(directory=os.path.join(BASE_DIR, 'static')), name='static')

@app.on_event("startup")
async def startup_event():
    import threading
    from . import watcher
    
    # Start watcher loop thread
    watcher_thread = threading.Thread(target=watcher.start_watcher_loop, daemon=True)
    watcher_thread.start()
    
    # Start worker loop thread
    worker_thread = threading.Thread(target=worker.run_worker, daemon=True)
    worker_thread.start()
    
    # Start background update scheduler task
    asyncio.create_task(updater.updater_scheduler_loop())
    
    logger.info("Background watcher, worker, and updater scheduler threads started successfully.")

@app.get('/status')
async def status():
    st = control.get_state()
    return JSONResponse({
        'queue_length': q.qlen(),
        'paused': bool(st.get('paused', False)),
        'watcher_status': control.get_status('watcher'),
        'worker_status': control.get_status('worker')
    })

@app.get('/config')
async def get_config():
    return JSONResponse(worker.load_config())

@app.post('/config')
async def post_config(req: Request):
    try:
        data = await req.json()
        if data is None:
            data = {}

        # Validate and migrate incoming config to canonical shape
        def normalize_suffix(s):
            try:
                return str(s or '').strip().lstrip('.')
            except Exception:
                return ''

        def validate_and_migrate(cfg):
            if not isinstance(cfg, dict):
                return False, 'config must be an object'

            migrated = dict(cfg)  # shallow copy

            # watch_paths: allow legacy list of strings -> convert to list of objects
            wp = migrated.get('watch_paths', []) or []
            new_wp = []
            if isinstance(wp, list) and wp and all(isinstance(x, str) for x in wp):
                # legacy: single global recursive flag may exist
                global_recursive = bool(migrated.get('recursive', False))
                new_wp = [{'path': p.strip().strip("'\"").strip(), 'recursive': global_recursive} for p in wp if p]
            else:
                # ensure list of objects with path and recursive
                if isinstance(wp, list):
                    for item in wp:
                        if isinstance(item, str):
                            clean_p = item.strip().strip("'\"").strip()
                            if clean_p:
                                new_wp.append({'path': clean_p, 'recursive': False})
                        elif isinstance(item, dict):
                            path = item.get('path') or item.get('watch_path') or ''
                            clean_p = path.strip().strip("'\"").strip()
                            if clean_p:
                                new_wp.append({'path': clean_p, 'recursive': bool(item.get('recursive', False))})
            migrated['watch_paths'] = new_wp

            # validate watch paths for invalid characters
            for entry in new_wp:
                p_val = entry['path']
                if any(c in p_val for c in ['<', '>', '|', '?', '*'] if c):
                    return False, f"Invalid path contains illegal characters: {p_val}"

            # enable toggles: normalize to bool
            migrated['enable_conversion_schemes'] = bool(migrated.get('enable_conversion_schemes', True))
            migrated['enable_extension_aliases'] = bool(migrated.get('enable_extension_aliases', True))

            # source_extensions: normalize to list of lowercase suffixes
            se = migrated.get('source_extensions')
            if se is None:
                migrated['source_extensions'] = []
            elif isinstance(se, str):
                migrated['source_extensions'] = [x.strip().lower().lstrip('.') for x in se.split(',') if x.strip()]
            elif isinstance(se, list):
                migrated['source_extensions'] = [str(x).strip().lower().lstrip('.') for x in se if str(x).strip()]
            else:
                migrated['source_extensions'] = []

            # target_format: normalize
            tf = migrated.get('target_format')
            migrated['target_format'] = normalize_suffix(tf) or ''

            # delete_original flag: normalize to bool
            migrated['delete_original'] = bool(migrated.get('delete_original', False))

            # conversion_schemes: if absent but we have target_format or source_extensions, create a default scheme
            cs = migrated.get('conversion_schemes')
            if not cs:
                if migrated.get('target_format'):
                    migrated['conversion_schemes'] = [{
                        'name': 'default',
                        'source_extensions': migrated.get('source_extensions', []),
                        'target_format': migrated.get('target_format', ''),
                        'delete_original': migrated.get('delete_original', False),
                        'enabled': True
                    }]
                else:
                    migrated['conversion_schemes'] = []
            else:
                # normalize each scheme
                normalized_schemes = []
                if isinstance(cs, list):
                    for sc in cs:
                        if not isinstance(sc, dict):
                            continue
                        normalized_schemes.append({
                            'name': sc.get('name') or sc.get('id') or '',
                            'source_extensions': [str(x).strip().lower().lstrip('.') for x in (sc.get('source_extensions') or []) if str(x).strip()],
                            'target_format': normalize_suffix(sc.get('target_format') or sc.get('target') or ''),
                            'delete_original': bool(sc.get('delete_original', sc.get('remove_original', False))),
                            'enabled': bool(sc.get('enabled', True))
                        })
                migrated['conversion_schemes'] = normalized_schemes

            # extension_aliases: try to accept either dict or JSON string
            ea = migrated.get('extension_aliases')
            if isinstance(ea, str):
                try:
                    migrated['extension_aliases'] = yaml.safe_load(ea) or {}
                except Exception:
                    migrated['extension_aliases'] = {}
            elif isinstance(ea, dict):
                migrated['extension_aliases'] = ea
            else:
                migrated['extension_aliases'] = {}

            # log settings validation and normalization
            try:
                migrated['log_max_lines'] = max(0, int(cfg.get('log_max_lines', 0)))
            except (ValueError, TypeError):
                migrated['log_max_lines'] = 0

            try:
                migrated['log_max_size_kb'] = max(0.0, float(cfg.get('log_max_size_kb', 0.0)))
            except (ValueError, TypeError):
                migrated['log_max_size_kb'] = 0.0

            try:
                migrated['log_max_hours'] = max(0.0, float(cfg.get('log_max_hours', 0.0)))
            except (ValueError, TypeError):
                migrated['log_max_hours'] = 0.0

            try:
                migrated['log_backup_count'] = max(0, int(cfg.get('log_backup_count', 5)))
            except (ValueError, TypeError):
                migrated['log_backup_count'] = 5

            # updater settings validation and normalization
            migrated['update_check_on_startup'] = bool(cfg.get('update_check_on_startup', True))
            migrated['update_include_prerelease'] = bool(cfg.get('update_include_prerelease', False))
            migrated['update_check_frequency'] = str(cfg.get('update_check_frequency', 'daily')).strip().lower()
            if migrated['update_check_frequency'] not in ('none', 'hourly', 'daily', 'weekly', 'custom_hours', 'custom_days', 'specific_time'):
                migrated['update_check_frequency'] = 'daily'
            try:
                migrated['update_check_interval'] = max(1, int(cfg.get('update_check_interval', 1)))
            except (ValueError, TypeError):
                migrated['update_check_interval'] = 1
            migrated['update_check_time'] = str(cfg.get('update_check_time', '02:00')).strip()

            return True, migrated

        ok, result = validate_and_migrate(data)
        if not ok:
            return JSONResponse({'ok': False, 'error': result}, status_code=400)

        # Persist as YAML for worker consumption
        with open('config.yaml', 'w', encoding='utf-8') as f:
            yaml.safe_dump(result, f, sort_keys=False, allow_unicode=True)
        return JSONResponse({'ok': True, 'config': result})
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

        last_pos = 0
        last_ino = None
        last_dev = None

        if os.path.exists(LOGFILE):
            try:
                st = os.stat(LOGFILE)
                last_ino = st.st_ino
                last_dev = st.st_dev
                last_pos = st.st_size
            except Exception:
                pass

        while True:
            await asyncio.sleep(0.5)
            if not os.path.exists(LOGFILE):
                continue

            try:
                st = os.stat(LOGFILE)
                if st.st_ino != last_ino or st.st_dev != last_dev:
                    last_ino = st.st_ino
                    last_dev = st.st_dev
                    last_pos = 0

                with open(LOGFILE, 'r', encoding='utf-8') as f:
                    f.seek(last_pos)
                    while True:
                        line = f.readline()
                        if not line:
                            last_pos = f.tell()
                            break
                        await ws.send_text(line.rstrip('\n'))
            except Exception:
                pass
    except WebSocketDisconnect:
        pass

@app.get('/updater/status')
async def get_updater_status():
    return JSONResponse(updater.get_updater_status())

@app.post('/updater/check')
async def post_updater_check():
    cfg = worker.load_config()
    include_prerelease = bool(cfg.get('update_include_prerelease', False))
    updater.background_check_update(include_prerelease)
    return JSONResponse({'ok': True})

@app.post('/updater/run')
async def post_updater_run():
    cfg = worker.load_config()
    include_prerelease = bool(cfg.get('update_include_prerelease', False))
    updater.background_run_update(include_prerelease)
    return JSONResponse({'ok': True})

@app.get('/')
async def index():
    html = '''
    <html>
        <head>
            <title data-i18n="title">phos-watch Admin</title>
            <meta charset="utf-8" />
            <link rel="icon" type="image/x-icon" href="/static/favicon.ico" />
            <link rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png" />
            <link rel="icon" type="image/png" sizes="32x32" href="/static/favicon-32x32.png" />
            <link rel="icon" type="image/png" sizes="16x16" href="/static/favicon-16x16.png" />
            <style>
                body { font-family: Arial, sans-serif; margin: 16px; background: #f6f7fb; color: #1f2937; }
                .grid { display: grid; gap: 16px; grid-template-columns: 1fr; }
                .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,.04); }
                .muted { color: #6b7280; }
                pre, textarea, input, select { width: 100%; box-sizing: border-box; }
                pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 10px; min-height: 120px; max-height: 400px; overflow-y: auto; }
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
                .module { border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px; background: #f8fafc; margin-bottom: 12px; }
                .module h4 { margin: 0 0 8px 0; }
                .item-list { display: grid; gap: 8px; }
                .item-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; padding: 8px; border: 1px solid #dbeafe; border-radius: 8px; background: #ffffff; }
                .item-main { flex: 1 1 360px; min-width: 220px; }
                .mono { font-family: Consolas, monospace; }
                .badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #e2e8f0; color: #334155; }
                .status-badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: bold; text-decoration: none; }
                .status-normal { background: #d1fae5; color: #065f46; }
                .status-abnormal { background: #fee2e2; color: #991b1b; }
                .status-offline { background: #f3f4f6; color: #374151; }
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
                <!-- Update card -->
                <div class="card" id="updateCard" style="display:none; border: 1px solid #fed7aa; background: #fffbeb;">
                    <h3 style="margin-top:0;" data-i18n="update_card_title">應用程式更新</h3>
                    <div id="updateStatusMsg" style="font-weight: 500;">...</div>
                    <div id="updateNotesSection" style="margin-top:8px; display:none;">
                        <strong data-i18n="release_notes_label">釋出說明：</strong>
                        <pre id="updateReleaseNotes" style="white-space: pre-wrap; background: #fafaf9; color: #44403c; border: 1px solid #e7e5e4; min-height: 50px; max-height: 150px; margin-top: 4px; padding: 8px; font-family: sans-serif;"></pre>
                    </div>
                    <div id="updateProgressContainer" style="display:none; width: 100%; background: #e2e8f0; border-radius: 999px; height: 10px; margin-top: 10px; overflow: hidden;">
                        <div id="updateProgressBar" style="width: 0%; height: 100%; background: #f97316; transition: width 0.3s;"></div>
                    </div>
                    <div class="row" style="margin-top:12px;" id="updateCardActions">
                        <button type="button" id="confirmUpdateBtn" data-i18n="confirm_update_btn">安裝更新</button>
                        <button type="button" id="closeUpdateCardBtn" class="secondary" data-i18n="close_btn">關閉</button>
                    </div>
                </div>

                <div class="card">
                    <div class="row">
                        <strong data-i18n="queue_length_label">Queue length:</strong> <span id="qlen">...</span>
                        <strong data-i18n="pause_state_label">Pause state:</strong> <span id="pausedState">...</span>
                        <strong data-i18n="watcher_status_label">Watcher status:</strong> <span id="watcherStatus" class="status-badge status-offline">...</span>
                        <strong data-i18n="worker_status_label">Worker status:</strong> <span id="workerStatus" class="status-badge status-offline">...</span>
                        <strong data-i18n="app_version_label">Version:</strong> <span id="appVersion">...</span> <span id="updateBadge" style="display:none;" class="badge"></span>
                        <button type="button" id="manualCheckUpdateBtn" class="secondary" style="padding: 4px 8px; font-size: 12px; margin-left: 8px;" data-i18n="check_update_btn">檢查更新</button>
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
                    <p class="muted" data-i18n="config_desc">依照三個步驟設定：監聽路徑、轉檔方案、副檔名規範。</p>
                    <form id="configForm">
                        <div class="module">
                            <h4 data-i18n="module_watch_title">1) 監聽路徑與遞迴設定</h4>
                            <div class="field-help" data-i18n="module_watch_desc">每一列是一個路徑，可獨立設定是否遞迴監聽。</div>
                            <div id="watch_paths_rows" class="item-list"></div>
                            <div class="row" style="margin-top:8px;">
                                <input type="text" id="watch_path_input" data-i18n-placeholder="watch_paths_placeholder" placeholder="一次新增一個監看資料夾" />
                                <label><input type="checkbox" id="watch_path_recursive" /> <span data-i18n="recursive_watch">遞迴監看</span></label>
                                <button type="button" id="watch_path_add" class="secondary" data-i18n="add_item">新增</button>
                            </div>
                        </div>

                        <div class="module">
                            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
                                <h4 style="margin: 0;" data-i18n="module_scheme_title">2) 轉檔方案</h4>
                                <label style="font-weight: 600;"><input type="checkbox" id="enable_conversion_schemes" /> <span data-i18n="enable_module">啟用此功能</span></label>
                            </div>
                            <div class="field-help" data-i18n="module_scheme_desc">可建立多個方案。每個方案包含來源副檔名、目標副檔名與是否刪除原檔。</div>
                            <div id="schemes_rows" class="item-list"></div>
                            <div class="row" style="margin-top:8px;">
                                <input type="text" id="scheme_name" data-i18n-placeholder="scheme_name_placeholder" placeholder="方案名稱（例如：手機照片）" />
                            </div>
                            <div class="field" style="margin-top: 8px; display: flex; flex-direction: column; gap: 4px;">
                                <label style="font-weight: 600;" data-i18n="source_extensions_label">來源副檔名</label>
                                <div id="scheme_sources_checkboxes" style="display: flex; flex-wrap: wrap; gap: 10px; padding: 10px; background: #fff; border: 1px solid #cbd5e1; border-radius: 10px;">
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="jpg" /> jpg</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="jpeg" /> jpeg</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="png" /> png</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="webp" /> webp</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="gif" /> gif</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="bmp" /> bmp</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="tiff" /> tiff</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="tif" /> tif</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="heic" /> heic</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="heif" /> heif</label>
                                    <label style="font-weight: normal; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" name="src_ext" value="avif" /> avif</label>
                                </div>
                                <input type="text" id="scheme_sources_custom" data-i18n-placeholder="scheme_sources_custom_placeholder" placeholder="自訂其他副檔名，逗號分隔，例如 raw" style="margin-top: 4px;" />
                                <div class="field-help" style="color: #b45309; margin-top: 4px;" data-i18n="custom_ext_warning">⚠️ 提示：自訂副檔名不一定支援轉檔，僅限已知相容格式。</div>
                            </div>
                            <div class="row" style="margin-top: 8px;">
                                <select id="scheme_target"></select>
                                <input type="text" id="scheme_target_custom" data-i18n-placeholder="target_format_custom_placeholder" placeholder="輸入副檔名，例如 JPG 或 jpeg" />
                                <label><input type="checkbox" id="scheme_delete_original" /> <span data-i18n="delete_original_label">轉檔後刪除原檔</span></label>
                                <button type="button" id="scheme_add" class="secondary" data-i18n="add_item">新增</button>
                            </div>
                        </div>

                        <div class="module">
                            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
                                <h4 style="margin: 0;" data-i18n="module_alias_title">3) 副檔名規範</h4>
                                <label style="font-weight: 600;"><input type="checkbox" id="enable_extension_aliases" /> <span data-i18n="enable_module">啟用此功能</span></label>
                            </div>
                            <div class="field-help" data-i18n="module_alias_desc">設定同一格式可能出現的多種副檔名，系統會用來判斷與正規化。</div>
                            <div id="alias_rows" class="item-list"></div>
                            <div class="row" style="margin-top:8px;">
                                <input type="text" id="alias_canonical" data-i18n-placeholder="alias_canonical_placeholder" placeholder="標準副檔名，例如 jpg" />
                                <input type="text" id="alias_values" data-i18n-placeholder="alias_values_placeholder" placeholder="別名，逗號分隔，例如 jpeg,JPG,JPEG" />
                                <button type="button" id="alias_add" class="secondary" data-i18n="add_item">新增</button>
                            </div>
                        </div>

                        <div class="module">
                            <h4 data-i18n="module_log_title">4) 日誌備份與輪轉設定</h4>
                            <div class="field-help" data-i18n="module_log_desc">設定日誌檔案的保留上限，若超過限制將會自動輪轉備份。</div>
                            <div class="row" style="margin-top:8px; display: flex; gap: 16px; flex-wrap: wrap;">
                                <div class="field" style="flex: 1 1 180px;">
                                    <label for="log_max_lines" data-i18n="log_max_lines_label">最大行數 (0表示不限制)</label>
                                    <input type="number" id="log_max_lines" min="0" placeholder="例如：10000" />
                                </div>
                                <div class="field" style="flex: 1 1 180px;">
                                    <label for="log_max_size_kb" data-i18n="log_max_size_kb_label">最大大小 (KB, 0表示不限制)</label>
                                    <input type="number" id="log_max_size_kb" min="0" placeholder="例如：1024" />
                                </div>
                                <div class="field" style="flex: 1 1 180px;">
                                    <label for="log_max_hours" data-i18n="log_max_hours_label">最大時間 (小時, 0表示不限制)</label>
                                    <input type="number" id="log_max_hours" min="0" step="0.5" placeholder="例如：24" />
                                </div>
                                <div class="field" style="flex: 1 1 180px;">
                                    <label for="log_backup_count" data-i18n="log_backup_count_label">保留備份檔個數</label>
                                    <input type="number" id="log_backup_count" min="0" placeholder="例如：5" />
                                </div>
                            </div>
                        </div>

                        <div class="module">
                            <h4 data-i18n="module_update_title">5) 更新與排程設定</h4>
                            <div class="field-help" data-i18n="module_update_desc">設定自動檢查更新的排程與偏好。</div>
                            <div class="row" style="margin-top:8px; display: flex; gap: 16px; flex-wrap: wrap;">
                                <label style="font-weight: 600; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" id="update_check_on_startup" /> <span data-i18n="update_check_on_startup_label">啟用啟動時檢查更新</span></label>
                                <label style="font-weight: 600; display: inline-flex; align-items: center; gap: 4px;"><input type="checkbox" id="update_include_prerelease" /> <span data-i18n="update_include_prerelease_label">接收測試版/預發布版更新 (Beta/RC)</span></label>
                            </div>
                            <div class="row" style="margin-top:8px; display: flex; gap: 16px; flex-wrap: wrap;">
                                <div class="field" style="flex: 1 1 180px;">
                                    <label for="update_check_frequency" data-i18n="update_check_frequency_label">檢查頻率</label>
                                    <select id="update_check_frequency">
                                        <option value="none" data-i18n="freq_none">無 (不自動檢查)</option>
                                        <option value="hourly" data-i18n="freq_hourly">每隔幾小時</option>
                                        <option value="daily" data-i18n="freq_daily">每隔幾天</option>
                                        <option value="weekly" data-i18n="freq_weekly">每隔幾週</option>
                                        <option value="specific_time" data-i18n="freq_specific_time">每日特定時間</option>
                                    </select>
                                </div>
                                <div class="field" id="update_interval_field" style="flex: 1 1 180px;">
                                    <label for="update_check_interval" data-i18n="update_check_interval_label">檢查間隔數值</label>
                                    <input type="number" id="update_check_interval" min="1" value="1" />
                                </div>
                                <div class="field" id="update_time_field" style="flex: 1 1 180px; display:none;">
                                    <label for="update_check_time" data-i18n="update_check_time_label">每日檢查時間 (HH:MM)</label>
                                    <input type="text" id="update_check_time" placeholder="例如：02:00" />
                                </div>
                            </div>
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
                function mapLangKey(lng) {
                    if (!lng) return lng;
                    // map zh variants to our file name zh-TW.json
                    if (String(lng).toLowerCase().startsWith('zh')) return 'zh-TW';
                    // otherwise use primary language tag (e.g., 'en')
                    return String(lng).split(/[-_]/)[0];
                }

                i18next.use(i18nextHttpBackend).use(i18nextBrowserLanguageDetector).init({
                    fallbackLng: 'zh-TW',
                    debug: false,
                    backend: { loadPath: '/static/locales/{{lng}}.json' }
                }, function(err, t) {
                    if (err) console.error('i18next init error', err);
                    // if detected language uses a different tag (e.g. zh-TW) ensure we load the mapped file
                    try {
                        const detected = i18next.language;
                        const mapped = mapLangKey(detected);
                        if (mapped && mapped !== detected) {
                            i18next.changeLanguage(mapped).then(() => { translatePage(); }).catch(()=>{ translatePage(); });
                            return;
                        }
                    } catch(e){}
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

                    // Re-render dynamic modules so their buttons/labels use the latest translations.
                    try {
                        // If the render functions exist, use current DOM state to re-render.
                        if (typeof getWatchPaths === 'function' && typeof renderWatchPaths === 'function') {
                            renderWatchPaths(getWatchPaths());
                        }
                        if (typeof getSchemes === 'function' && typeof renderSchemes === 'function') {
                            renderSchemes(getSchemes());
                        }
                        if (typeof getAliases === 'function' && typeof renderAliases === 'function') {
                            const aliasObj = getAliases();
                            const aliasList = Object.entries(aliasObj).map(([canonical, aliases]) => ({ canonical, aliases }));
                            renderAliases(aliasList);
                        }
                        // For generic list containers created by renderList, re-run on their current values
                        if (typeof renderList === 'function') {
                            // re-render known containers if present
                            ['alias_rows','watch_paths_rows','schemes_rows'].forEach(cid => {
                                const el = document.getElementById(cid);
                                if (!el) return;
                                // try to preserve existing dataset-driven values by calling corresponding renderer above
                            });
                        }
                    } catch(e) { console.error('translatePage re-render error', e); }
                }

                document.getElementById('langSel').addEventListener('change', (ev) => {
                    const sel = ev.target.value;
                    const mapped = mapLangKey(sel);
                    i18next.changeLanguage(mapped).then(() => { localStorage.setItem('phos_lang', mapped); translatePage(); }).catch(()=>{});
                });

                // Persist selection from localStorage if present
                const saved = localStorage.getItem('phos_lang');
                if (saved) {
                    // saved may be mapped (zh-TW) or simple ('en') - set selector to a reasonable display value
                   i18next.changeLanguage(saved).then(translatePage).catch(()=>{});
                }

                function t(key) {
                    try {
                        if (typeof i18next !== 'undefined' && i18next.isInitialized && i18next.exists(key)) {
                            return i18next.t(key);
                        }
                    } catch(e) {}
                    const fallbacks = {
                        'paused': '已暫停',
                        'running': '運行中',
                        'status_normal': '正常',
                        'status_abnormal': '異常',
                        'status_offline': '下線',
                        'watcher_status_label': '監聽器狀態：',
                        'worker_status_label': '處理器狀態：'
                    };
                    return fallbacks[key] || key;
                }

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

                        const normalizeWatchPaths = (cfg) => {
                            const raw = cfg.watch_paths || [];
                            if (!Array.isArray(raw)) return [];
                            if (raw.length && typeof raw[0] === 'string') {
                                const globalRecursive = !!cfg.recursive;
                                return raw.map(path => ({ path: String(path || '').trim(), recursive: globalRecursive })).filter(item => item.path);
                            }
                            return raw.map(item => ({
                                path: String((item && (item.path || item.watch_path)) || '').trim(),
                                recursive: !!(item && item.recursive)
                            })).filter(item => item.path);
                        };

                        const normalizeSchemes = (cfg) => {
                            if (Array.isArray(cfg.conversion_schemes) && cfg.conversion_schemes.length) {
                                return cfg.conversion_schemes.map((sc, idx) => ({
                                    name: String((sc && (sc.name || sc.id)) || ('scheme-' + (idx + 1))).trim(),
                                    source_extensions: normalizeTextList(sc && sc.source_extensions).map(x => normalizeSuffix(x).toLowerCase()),
                                    target_format: normalizeSuffix(sc && sc.target_format),
                                    delete_original: !!(sc && sc.delete_original),
                                    enabled: sc && sc.enabled !== false
                                })).filter(sc => sc.target_format || sc.source_extensions.length);
                            }
                            return [{
                                name: 'default',
                                source_extensions: normalizeTextList(cfg.source_extensions).map(x => normalizeSuffix(x).toLowerCase()),
                                target_format: normalizeSuffix(cfg.target_format),
                                delete_original: !!cfg.delete_original,
                                enabled: true
                            }].filter(sc => sc.target_format || sc.source_extensions.length);
                        };

                        const normalizeAliases = (cfg) => {
                            const aliases = (cfg && cfg.extension_aliases && typeof cfg.extension_aliases === 'object') ? cfg.extension_aliases : {};
                            return Object.entries(aliases).map(([canonical, aliasList]) => ({
                                canonical: normalizeSuffix(canonical),
                                aliases: normalizeTextList(aliasList).map(x => normalizeSuffix(x))
                            })).filter(item => item.canonical);
                        };

                        renderSelect('scheme_target', collectSuffixOptions(j), j.target_format || 'jpg');
                        renderWatchPaths(normalizeWatchPaths(j));
                        renderSchemes(normalizeSchemes(j));
                        renderAliases(normalizeAliases(j));

                        // Load log settings
                        const lLines = document.getElementById('log_max_lines');
                        if (lLines) lLines.value = j.log_max_lines !== undefined ? j.log_max_lines : 0;
                        const lSize = document.getElementById('log_max_size_kb');
                        if (lSize) lSize.value = j.log_max_size_kb !== undefined ? j.log_max_size_kb : 0;
                        const lHours = document.getElementById('log_max_hours');
                        if (lHours) lHours.value = j.log_max_hours !== undefined ? j.log_max_hours : 0;
                        const lBackup = document.getElementById('log_backup_count');
                        if (lBackup) lBackup.value = j.log_backup_count !== undefined ? j.log_backup_count : 5;

                        // Load module enable toggles
                        const enSchemes = document.getElementById('enable_conversion_schemes');
                        if (enSchemes) enSchemes.checked = j.enable_conversion_schemes !== false;
                        const enAliases = document.getElementById('enable_extension_aliases');
                        if (enAliases) enAliases.checked = j.enable_extension_aliases !== false;

                        // Load updater settings
                        const upStartup = document.getElementById('update_check_on_startup');
                        if (upStartup) upStartup.checked = j.update_check_on_startup !== false;
                        const upPre = document.getElementById('update_include_prerelease');
                        if (upPre) upPre.checked = !!j.update_include_prerelease;
                        const upFreq = document.getElementById('update_check_frequency');
                        if (upFreq) upFreq.value = j.update_check_frequency || 'daily';
                        const upInterval = document.getElementById('update_check_interval');
                        if (upInterval) upInterval.value = j.update_check_interval !== undefined ? j.update_check_interval : 1;
                        const upTime = document.getElementById('update_check_time');
                        if (upTime) upTime.value = j.update_check_time || '02:00';
                        if (typeof updateUpdateFieldsUI === 'function') {
                            updateUpdateFieldsUI();
                        }

                        if (typeof updateModuleStatusUI === 'function') {
                            updateModuleStatusUI();
                        }
                    } catch (e) { console.error(e); }
                }

                function getWatchPaths() {
                    return Array.from(document.querySelectorAll('#watch_paths_rows .watch-row')).map(row => ({
                        path: String(row.dataset.path || '').trim(),
                        recursive: row.dataset.recursive === 'true'
                    })).filter(item => item.path);
                }

                function renderWatchPaths(items) {
                    const container = document.getElementById('watch_paths_rows');
                    if (!container) return;
                    container.innerHTML = '';
                    if (!items || !items.length) {
                        const empty = document.createElement('span');
                        empty.className = 'list-empty';
                        empty.textContent = t('list_empty');
                        container.appendChild(empty);
                        return;
                    }

                    items.forEach(item => {
                        const row = document.createElement('div');
                        row.className = 'item-row watch-row';
                        row.dataset.path = item.path;
                        row.dataset.recursive = item.recursive ? 'true' : 'false';

                        const main = document.createElement('div');
                        main.className = 'item-main mono';
                        main.textContent = item.path;

                        const recursiveBadge = document.createElement('span');
                        recursiveBadge.className = 'badge';
                        recursiveBadge.textContent = item.recursive ? t('recursive_yes') : t('recursive_no');

                        const toggleBtn = document.createElement('button');
                        toggleBtn.type = 'button';
                        toggleBtn.className = 'secondary';
                        toggleBtn.textContent = t('toggle_recursive');
                        toggleBtn.onclick = () => {
                            const next = !item.recursive;
                            const nextItems = getWatchPaths().map(x => x.path === item.path ? { path: x.path, recursive: next } : x);
                            renderWatchPaths(nextItems);
                        };

                        const removeBtn = document.createElement('button');
                        removeBtn.type = 'button';
                        removeBtn.textContent = t('remove_item');
                        removeBtn.onclick = () => {
                            const nextItems = getWatchPaths().filter(x => x.path !== item.path);
                            renderWatchPaths(nextItems);
                        };

                        row.appendChild(main);
                        row.appendChild(recursiveBadge);
                        row.appendChild(toggleBtn);
                        row.appendChild(removeBtn);
                        container.appendChild(row);
                    });
                }

                function getSchemes() {
                    return Array.from(document.querySelectorAll('#schemes_rows .scheme-row')).map(row => ({
                        name: String(row.dataset.name || '').trim(),
                        source_extensions: normalizeTextList(row.dataset.sources || '').map(x => normalizeSuffix(x).toLowerCase()),
                        target_format: normalizeSuffix(row.dataset.target || ''),
                        delete_original: row.dataset.deleteOriginal === 'true',
                        enabled: row.dataset.enabled !== 'false'
                    })).filter(item => item.target_format || item.source_extensions.length);
                }

                function renderSchemes(items) {
                    const container = document.getElementById('schemes_rows');
                    if (!container) return;
                    container.innerHTML = '';
                    if (!items || !items.length) {
                        const empty = document.createElement('span');
                        empty.className = 'list-empty';
                        empty.textContent = t('list_empty');
                        container.appendChild(empty);
                        return;
                    }

                    items.forEach((item, idx) => {
                        const row = document.createElement('div');
                        row.className = 'item-row scheme-row';
                        row.dataset.name = item.name || ('scheme-' + (idx + 1));
                        row.dataset.sources = (item.source_extensions || []).join(',');
                        row.dataset.target = item.target_format || '';
                        row.dataset.deleteOriginal = item.delete_original ? 'true' : 'false';
                        row.dataset.enabled = item.enabled === false ? 'false' : 'true';

                        const main = document.createElement('div');
                        main.className = 'item-main';
                        const name = document.createElement('div');
                        name.innerHTML = '<strong>' + (row.dataset.name || ('scheme-' + (idx + 1))) + '</strong>';
                        const details = document.createElement('div');
                        details.className = 'mono muted';
                        details.textContent = (item.source_extensions || []).join(', ') + ' -> ' + (item.target_format || '');
                        main.appendChild(name);
                        main.appendChild(details);

                        const delBadge = document.createElement('span');
                        delBadge.className = 'badge';
                        delBadge.textContent = item.delete_original ? t('delete_original_yes') : t('delete_original_no');

                        const toggleDelBtn = document.createElement('button');
                        toggleDelBtn.type = 'button';
                        toggleDelBtn.className = 'secondary';
                        toggleDelBtn.textContent = t('toggle_delete_original');
                        toggleDelBtn.onclick = () => {
                            const all = getSchemes();
                            all[idx].delete_original = !all[idx].delete_original;
                            renderSchemes(all);
                        };

                        const removeBtn = document.createElement('button');
                        removeBtn.type = 'button';
                        removeBtn.textContent = t('remove_item');
                        removeBtn.onclick = () => {
                            const all = getSchemes();
                            all.splice(idx, 1);
                            renderSchemes(all);
                        };

                        row.appendChild(main);
                        row.appendChild(delBadge);
                        row.appendChild(toggleDelBtn);
                        row.appendChild(removeBtn);
                        container.appendChild(row);
                    });
                }

                function getAliases() {
                    const out = {};
                    Array.from(document.querySelectorAll('#alias_rows .alias-row')).forEach(row => {
                        const canonical = normalizeSuffix(row.dataset.canonical || '');
                        if (!canonical) return;
                        out[canonical] = normalizeTextList(row.dataset.aliases || '').map(x => normalizeSuffix(x));
                    });
                    return out;
                }

                function renderAliases(items) {
                    const container = document.getElementById('alias_rows');
                    if (!container) return;
                    container.innerHTML = '';
                    if (!items || !items.length) {
                        const empty = document.createElement('span');
                        empty.className = 'list-empty';
                        empty.textContent = t('list_empty');
                        container.appendChild(empty);
                        return;
                    }

                    items.forEach((item, idx) => {
                        const row = document.createElement('div');
                        row.className = 'item-row alias-row';
                        row.dataset.canonical = item.canonical;
                        row.dataset.aliases = (item.aliases || []).join(',');

                        const main = document.createElement('div');
                        main.className = 'item-main';
                        const title = document.createElement('div');
                        title.innerHTML = '<strong class="mono">' + item.canonical + '</strong>';
                        const details = document.createElement('div');
                        details.className = 'mono muted';
                        details.textContent = (item.aliases || []).join(', ');
                        main.appendChild(title);
                        main.appendChild(details);

                        const removeBtn = document.createElement('button');
                        removeBtn.type = 'button';
                        removeBtn.textContent = t('remove_item');
                        removeBtn.onclick = () => {
                            const all = Object.entries(getAliases()).map(([canonical, aliases]) => ({ canonical, aliases }));
                            all.splice(idx, 1);
                            renderAliases(all);
                        };

                        row.appendChild(main);
                        row.appendChild(removeBtn);
                        container.appendChild(row);
                    });
                }

                async function saveConfig() {
                    const btn = document.getElementById('saveConfig');
                    const origText = btn.textContent;
                    try {
                        btn.disabled = true;
                        btn.innerHTML = '<span class="spinner"></span>' + origText;
                        const cfg = {};

                        cfg.watch_paths = getWatchPaths();
                        cfg.conversion_schemes = getSchemes();
                        cfg.extension_aliases = getAliases();
                        cfg.enable_conversion_schemes = document.getElementById('enable_conversion_schemes').checked;
                        cfg.enable_extension_aliases = document.getElementById('enable_extension_aliases').checked;

                        // Save log settings
                        cfg.log_max_lines = parseInt(document.getElementById('log_max_lines')?.value || '0', 10);
                        cfg.log_max_size_kb = parseFloat(document.getElementById('log_max_size_kb')?.value || '0');
                        cfg.log_max_hours = parseFloat(document.getElementById('log_max_hours')?.value || '0');
                        cfg.log_backup_count = parseInt(document.getElementById('log_backup_count')?.value || '5', 10);

                        // Save updater settings
                        cfg.update_check_on_startup = !!document.getElementById('update_check_on_startup')?.checked;
                        cfg.update_include_prerelease = !!document.getElementById('update_include_prerelease')?.checked;
                        cfg.update_check_frequency = document.getElementById('update_check_frequency')?.value || 'daily';
                        cfg.update_check_interval = parseInt(document.getElementById('update_check_interval')?.value || '1', 10);
                        cfg.update_check_time = document.getElementById('update_check_time')?.value || '02:00';

                        // Backward-compatible fields for current worker logic
                        const active = cfg.conversion_schemes.find(sc => sc.enabled !== false) || cfg.conversion_schemes[0] || null;
                        cfg.source_extensions = active ? (active.source_extensions || []) : [];
                        cfg.target_format = active ? normalizeSuffix(active.target_format || '') : '';
                        cfg.delete_original = !!(active && active.delete_original);
                        cfg.recursive = cfg.watch_paths.some(x => x.recursive);

                        if (!cfg.watch_paths.length) {
                            showToast(t('watch_paths_required'), true);
                            return;
                        }
                        if (cfg.enable_conversion_schemes && !cfg.conversion_schemes.length) {
                            showToast(t('conversion_scheme_required'), true);
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

                function bindConfigEditors() {
                    const watchInput = document.getElementById('watch_path_input');
                    const watchRecursive = document.getElementById('watch_path_recursive');
                    const watchBtn = document.getElementById('watch_path_add');
                    const schemeBtn = document.getElementById('scheme_add');
                    const aliasBtn = document.getElementById('alias_add');

                    if (watchBtn) watchBtn.addEventListener('click', () => {
                        let path = String(watchInput && watchInput.value || '').trim();
                        // Auto-correct: strip leading/trailing quotes
                        path = path.replace(/^["']|["']$/g, '').trim();
                        if (!path) return;

                        // Validate path characters
                        const illegalChars = /[<>|?*"]/;
                        if (illegalChars.test(path)) {
                            showToast(t('invalid_path_error'), true);
                            return;
                        }

                        const next = getWatchPaths();
                        if (!next.some(x => x.path === path)) {
                            next.push({ path, recursive: !!(watchRecursive && watchRecursive.checked) });
                            renderWatchPaths(next);
                        }
                        if (watchInput) watchInput.value = '';
                    });

                    if (watchInput) {
                        watchInput.addEventListener('keydown', (ev) => {
                            if (ev.key === 'Enter') {
                                ev.preventDefault();
                                watchBtn && watchBtn.click();
                            }
                        });
                    }

                    if (schemeBtn) schemeBtn.addEventListener('click', () => {
                        const nameEl = document.getElementById('scheme_name');
                        const srcCustomEl = document.getElementById('scheme_sources_custom');
                        const tgtEl = document.getElementById('scheme_target');
                        const tgtCustomEl = document.getElementById('scheme_target_custom');
                        const delEl = document.getElementById('scheme_delete_original');

                        const name = String(nameEl && nameEl.value || '').trim() || ('scheme-' + (getSchemes().length + 1));
                        
                        // Collect checked checkboxes
                        const checkedExts = Array.from(document.querySelectorAll('input[name="src_ext"]:checked')).map(chk => chk.value);
                        // Collect custom list
                        const customExts = normalizeTextList(srcCustomEl && srcCustomEl.value || '').map(x => normalizeSuffix(x).toLowerCase());
                        const source_extensions = Array.from(new Set([...checkedExts, ...customExts]));

                        if (!source_extensions.length) {
                            showToast(t('source_extensions_label') + ' ' + t('list_empty'), true);
                            return;
                        }

                        const target_format = normalizeSuffix((tgtCustomEl && tgtCustomEl.value) || (tgtEl && tgtEl.value) || '');
                        if (!target_format) return;

                        const next = getSchemes();
                        next.push({
                            name,
                            source_extensions,
                            target_format,
                            delete_original: !!(delEl && delEl.checked),
                            enabled: true
                        });
                        renderSchemes(next);
                        if (nameEl) nameEl.value = '';
                        if (srcCustomEl) srcCustomEl.value = '';
                        // Uncheck checkboxes
                        document.querySelectorAll('input[name="src_ext"]').forEach(chk => chk.checked = false);
                        if (tgtCustomEl) tgtCustomEl.value = '';
                    });

                    if (aliasBtn) aliasBtn.addEventListener('click', () => {
                        const canonicalEl = document.getElementById('alias_canonical');
                        const valuesEl = document.getElementById('alias_values');
                        const canonical = normalizeSuffix(canonicalEl && canonicalEl.value || '');
                        if (!canonical) return;
                        const aliases = normalizeTextList(valuesEl && valuesEl.value || '').map(x => normalizeSuffix(x));

                        const map = getAliases();
                        map[canonical] = aliases;
                        const next = Object.entries(map).map(([k, v]) => ({ canonical: k, aliases: v }));
                        renderAliases(next);

                        if (canonicalEl) canonicalEl.value = '';
                        if (valuesEl) valuesEl.value = '';
                    });
                }

                document.getElementById('refreshQueue').addEventListener('click', refreshQueue);
                document.getElementById('togglePause').addEventListener('click', togglePause);
                document.getElementById('loadConfig').addEventListener('click', loadConfig);
                document.getElementById('saveConfig').addEventListener('click', saveConfig);
                bindConfigEditors();

                function updateModuleStatusUI() {
                    const enSchemes = document.getElementById('enable_conversion_schemes');
                    const enAliases = document.getElementById('enable_extension_aliases');
                    const enableSchemes = enSchemes ? enSchemes.checked : true;
                    const enableAliases = enAliases ? enAliases.checked : true;

                    const schemeInputs = [
                        'scheme_name', 'scheme_sources_custom', 'scheme_target', 'scheme_target_custom', 'scheme_delete_original', 'scheme_add'
                    ];
                    schemeInputs.forEach(id => {
                        const el = document.getElementById(id);
                        if (el) el.disabled = !enableSchemes;
                    });
                    document.querySelectorAll('input[name="src_ext"]').forEach(chk => {
                        chk.disabled = !enableSchemes;
                    });

                    const aliasInputs = [
                        'alias_canonical', 'alias_values', 'alias_add'
                    ];
                    aliasInputs.forEach(id => {
                        const el = document.getElementById(id);
                        if (el) el.disabled = !enableAliases;
                    });
                }

                const toggleSchemesEl = document.getElementById('enable_conversion_schemes');
                if (toggleSchemesEl) toggleSchemesEl.addEventListener('change', updateModuleStatusUI);
                const toggleAliasesEl = document.getElementById('enable_extension_aliases');
                if (toggleAliasesEl) toggleAliasesEl.addEventListener('change', updateModuleStatusUI);

                function updateComponentStatus(elementId, statusInfo) {
                    const el = document.getElementById(elementId);
                    if (!el) return;
                    if (!statusInfo) {
                        el.className = 'status-badge status-offline';
                        el.innerText = t('status_offline');
                        el.title = '';
                        return;
                    }
                    const status = statusInfo.status || 'offline';
                    const error = statusInfo.error || '';
                    
                    el.className = 'status-badge status-' + status;
                    el.innerText = t('status_' + status);
                    if (error) {
                        el.title = error;
                    } else {
                        el.title = '';
                    }
                }

                // Serialized polling loops to avoid overlapping fetches
                async function pollStatus() {
                    while (true) {
                        try {
                            const res = await fetch('/status');
                            const j = await res.json();
                            document.getElementById('qlen').innerText = j.queue_length;
                            document.getElementById('pausedState').innerText = j.paused ? t('paused') : t('running');
                            updateComponentStatus('watcherStatus', j.watcher_status);
                            updateComponentStatus('workerStatus', j.worker_status);
                        } catch (e) { console.error(e); }
                        await new Promise(r => setTimeout(r, 2000));
                    }
                }

                async function pollQueue() {
                    while (true) {
                        try {
                            await refreshQueue();
                        } catch (e) { console.error(e); }
                        await new Promise(r => setTimeout(r, 5000));
                    }
                }

                // start polling and initial load
                pollStatus();
                pollQueue();
                loadConfig();

                // Updater dynamic script integration
                function updateUpdateFieldsUI() {
                    const freqEl = document.getElementById('update_check_frequency');
                    if (!freqEl) return;
                    const freq = freqEl.value;
                    const intervalField = document.getElementById('update_interval_field');
                    const timeField = document.getElementById('update_time_field');
                    
                    if (freq === 'none') {
                        intervalField.style.display = 'none';
                        timeField.style.display = 'none';
                    } else if (freq === 'specific_time') {
                        intervalField.style.display = 'none';
                        timeField.style.display = 'block';
                    } else {
                        intervalField.style.display = 'block';
                        timeField.style.display = 'none';
                    }
                }
                document.getElementById('update_check_frequency').addEventListener('change', updateUpdateFieldsUI);

                let updaterPolling = false;
                let isUpdating = false;
                let isManualCheck = false;

                async function checkUpdaterStatusOnce() {
                    try {
                        const res = await fetch('/updater/status');
                        const j = await res.json();
                        
                        document.getElementById('appVersion').innerText = j.current_version;
                        
                        const updateBadge = document.getElementById('updateBadge');
                        if (j.update_available) {
                            updateBadge.style.display = 'inline-block';
                            updateBadge.style.background = '#fef08a';
                            updateBadge.style.color = '#854d0e';
                            updateBadge.innerText = t('new_version_available') + ' (' + j.latest_version + ')';
                        } else {
                            updateBadge.style.display = 'none';
                        }
                        
                        const updateCard = document.getElementById('updateCard');
                        const updateStatusMsg = document.getElementById('updateStatusMsg');
                        const progressContainer = document.getElementById('updateProgressContainer');
                        const progressBar = document.getElementById('updateProgressBar');
                        const actions = document.getElementById('updateCardActions');
                        const confirmBtn = document.getElementById('confirmUpdateBtn');
                        const checkBtn = document.getElementById('manualCheckUpdateBtn');
                        
                        if (j.status === 'idle') {
                            isUpdating = false;
                            checkBtn.disabled = false;
                            
                            if (j.update_available) {
                                updateCard.style.display = 'block';
                                updateStatusMsg.innerText = t('update_prompt') + ' ' + j.latest_version;
                                actions.style.display = 'block';
                                confirmBtn.style.display = 'inline-block';
                                
                                const notesSection = document.getElementById('updateNotesSection');
                                const notesText = document.getElementById('updateReleaseNotes');
                                if (j.release_notes) {
                                    notesSection.style.display = 'block';
                                    notesText.textContent = j.release_notes;
                                } else {
                                    notesSection.style.display = 'none';
                                }
                            } else {
                                updateCard.style.display = 'none';
                                if (isManualCheck) {
                                    showToast(t('already_latest'), false);
                                    isManualCheck = false;
                                }
                            }
                        } else if (j.status === 'error') {
                            isUpdating = false;
                            isManualCheck = false;
                            checkBtn.disabled = false;
                            updateCard.style.display = 'block';
                            updateStatusMsg.innerText = t('update_status_error') + ': ' + j.error_message;
                            progressContainer.style.display = 'none';
                            actions.style.display = 'block';
                            confirmBtn.style.display = 'none';
                        } else {
                            // status is 'checking', 'downloading', 'applying', 'done'
                            isUpdating = true;
                            checkBtn.disabled = true;
                            updateCard.style.display = 'block';
                            actions.style.display = 'none';
                            
                            if (j.status === 'checking') {
                                updateStatusMsg.innerText = t('update_status_checking');
                                progressContainer.style.display = 'none';
                            } else if (j.status === 'downloading') {
                                updateStatusMsg.innerText = t('update_status_downloading') + ' (' + j.progress + '%)';
                                progressContainer.style.display = 'block';
                                progressBar.style.width = j.progress + '%';
                            } else if (j.status === 'applying') {
                                updateStatusMsg.innerText = t('update_status_applying');
                                progressContainer.style.display = 'block';
                                progressBar.style.width = '100%';
                            } else if (j.status === 'done') {
                                updateStatusMsg.innerText = t('update_status_done');
                                progressContainer.style.display = 'none';
                                setTimeout(() => {
                                    alert(t('restarting_msg'));
                                    location.reload();
                                }, 3000);
                            }
                            
                            if (!updaterPolling) {
                                startUpdaterPolling();
                            }
                        }
                    } catch(e) {
                        console.error("Error checking updater status:", e);
                    }
                }

                async function startUpdaterPolling() {
                    if (updaterPolling) return;
                    updaterPolling = true;
                    while (isUpdating) {
                        await checkUpdaterStatusOnce();
                        await new Promise(r => setTimeout(r, 1000));
                    }
                    updaterPolling = false;
                }

                document.getElementById('manualCheckUpdateBtn').addEventListener('click', async () => {
                    const btn = document.getElementById('manualCheckUpdateBtn');
                    const origText = btn.textContent;
                    btn.disabled = true;
                    btn.innerText = t('checking_btn_text');
                    isManualCheck = true;
                    try {
                        const res = await fetch('/updater/check', { method: 'POST' });
                        const j = await res.json();
                        await new Promise(r => setTimeout(r, 500));
                        isUpdating = true;
                        await checkUpdaterStatusOnce();
                        startUpdaterPolling();
                    } catch(e) {
                        showToast(t('check_failed'), true);
                        isManualCheck = false;
                    } finally {
                        btn.disabled = false;
                        btn.textContent = origText;
                    }
                });

                document.getElementById('confirmUpdateBtn').addEventListener('click', async () => {
                    try {
                        await fetch('/updater/run', { method: 'POST' });
                        isUpdating = true;
                        checkUpdaterStatusOnce();
                        startUpdaterPolling();
                    } catch(e) {
                        showToast(t('update_failed'), true);
                    }
                });

                document.getElementById('closeUpdateCardBtn').addEventListener('click', () => {
                    document.getElementById('updateCard').style.display = 'none';
                    isUpdating = false;
                });

                // Run check on page load
                checkUpdaterStatusOnce();

                const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/logs');
                const logs = document.getElementById('logs');
                ws.onmessage = (ev) => { logs.textContent += ev.data + '\\n'; logs.scrollTop = logs.scrollHeight; };
            </script>
        </body>
    </html>
    '''
    return HTMLResponse(html)
