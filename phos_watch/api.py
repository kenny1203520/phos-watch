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
    html = r'''
    <!DOCTYPE html>
    <html lang="zh-TW">
        <head>
            <title data-i18n="title">phos-watch 管理介面</title>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
            <link rel="icon" type="image/x-icon" href="/static/favicon.ico" />
            <link rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png" />
            <link rel="icon" type="image/png" sizes="32x32" href="/static/favicon-32x32.png" />
            <link rel="icon" type="image/png" sizes="16x16" href="/static/favicon-16x16.png" />
            <style>
                /* Google Fonts & Tailwind-like CSS resets */
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

                :root {
                    --font-sans: 'Inter', system-ui, -apple-system, sans-serif;
                    --font-mono: 'JetBrains Mono', Consolas, monospace;

                    /* Light Theme */
                    --bg-app: #f8fafc;
                    --bg-card: #ffffff;
                    --border-color: #e2e8f0;
                    --border-hover: #cbd5e1;
                    --text-primary: #0f172a;
                    --text-secondary: #475569;
                    --text-muted: #64748b;

                    --accent: #4f46e5;
                    --accent-hover: #4338ca;
                    --accent-light: #e0e7ff;
                    --accent-text: #3730a3;

                    --bg-input: #ffffff;
                    --bg-module: #f8fafc;
                    
                    --success: #10b981;
                    --success-light: #d1fae5;
                    --success-text: #065f46;
                    --success-rgb: 16, 185, 129;

                    --danger: #ef4444;
                    --danger-light: #fee2e2;
                    --danger-text: #991b1b;
                    --danger-rgb: 239, 68, 68;

                    --warning: #f59e0b;
                    --warning-light: #fef3c7;
                    --warning-text: #854d0e;
                    --warning-rgb: 245, 158, 11;

                    --info: #0ea5e9;
                    --info-light: #e0f2fe;
                    --info-text: #0369a1;

                    --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.05), 0 1px 2px -1px rgb(0 0 0 / 0.05);
                    --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.05), 0 2px 4px -2px rgb(0 0 0 / 0.05);
                    --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.05), 0 4px 6px -4px rgb(0 0 0 / 0.05);
                    
                    --radius-lg: 12px;
                    --radius-md: 8px;
                    --transition-fast: 0.15s cubic-bezier(0.4, 0, 0.2, 1);
                    --transition-normal: 0.2s cubic-bezier(0.4, 0, 0.2, 1);
                }

                body.dark-mode {
                    /* Dark Theme */
                    --bg-app: #0b0f19;
                    --bg-card: #151d30;
                    --border-color: #22314d;
                    --border-hover: #2e4266;
                    --text-primary: #f8fafc;
                    --text-secondary: #94a3b8;
                    --text-muted: #64748b;

                    --accent: #6366f1;
                    --accent-hover: #4f46e5;
                    --accent-light: #1e1b4b;
                    --accent-text: #c7d2fe;

                    --bg-input: #0f172a;
                    --bg-module: #0f172a;

                    --success: #10b981;
                    --success-light: #064e3b;
                    --success-text: #a7f3d0;

                    --danger: #ef4444;
                    --danger-light: #7f1d1d;
                    --danger-text: #fca5a5;

                    --warning: #f59e0b;
                    --warning-light: #78350f;
                    --warning-text: #fde68a;

                    --info: #38bdf8;
                    --info-light: #0c4a6e;
                    --info-text: #e0f2fe;

                    --shadow: 0 4px 6px -1px rgb(0 0 0 / 0.2), 0 2px 4px -2px rgb(0 0 0 / 0.2);
                    --shadow-md: 0 10px 15px -3px rgb(0 0 0 / 0.3), 0 4px 6px -4px rgb(0 0 0 / 0.3);
                    --shadow-lg: 0 20px 25px -5px rgb(0 0 0 / 0.3), 0 8px 10px -6px rgb(0 0 0 / 0.3);
                }

                * {
                    box-sizing: border-box;
                    margin: 0;
                    padding: 0;
                }

                body {
                    font-family: var(--font-sans);
                    background-color: var(--bg-app);
                    color: var(--text-primary);
                    line-height: 1.5;
                    padding-bottom: 100px;
                    transition: background-color var(--transition-normal), color var(--transition-normal);
                }

                /* Layout structure */
                .topbar {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    background: var(--bg-card);
                    border-bottom: 1px solid var(--border-color);
                    padding: 14px 24px;
                    position: sticky;
                    top: 0;
                    z-index: 50;
                    box-shadow: var(--shadow);
                    transition: background-color var(--transition-normal), border-color var(--transition-normal);
                }

                .brand {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .brand h1 {
                    font-size: 18px;
                    font-weight: 700;
                    letter-spacing: -0.025em;
                }

                .brand-badge {
                    background: var(--accent-light);
                    color: var(--accent-text);
                    font-size: 11px;
                    font-weight: 600;
                    padding: 2px 6px;
                    border-radius: 6px;
                }

                .controls-right {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .lang-select {
                    background: var(--bg-input);
                    color: var(--text-primary);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                    padding: 6px 12px;
                    font-size: 13px;
                    font-weight: 500;
                    outline: none;
                    cursor: pointer;
                    transition: border-color var(--transition-fast);
                }
                .lang-select:hover {
                    border-color: var(--border-hover);
                }

                .theme-toggle {
                    background: var(--bg-input);
                    border: 1px solid var(--border-color);
                    color: var(--text-primary);
                    width: 34px;
                    height: 34px;
                    border-radius: var(--radius-md);
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    cursor: pointer;
                    transition: border-color var(--transition-fast), background-color var(--transition-fast);
                }
                .theme-toggle:hover {
                    border-color: var(--border-hover);
                    background: var(--bg-module);
                }

                .dashboard {
                    display: grid;
                    grid-template-columns: 1fr;
                    gap: 20px;
                    max-width: 1400px;
                    margin: 0 auto;
                    padding: 20px;
                }

                @media (min-width: 1024px) {
                    .dashboard {
                        grid-template-columns: 420px 1fr;
                    }
                }

                .column {
                    display: flex;
                    flex-direction: column;
                    gap: 20px;
                }

                /* Cards & Modules styling */
                .card {
                    background: var(--bg-card);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-lg);
                    padding: 20px;
                    box-shadow: var(--shadow);
                    transition: background-color var(--transition-normal), border-color var(--transition-normal), box-shadow var(--transition-normal);
                }

                .card-title {
                    font-size: 15px;
                    font-weight: 700;
                    margin-bottom: 16px;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    letter-spacing: -0.01em;
                }

                .card-subtitle {
                    color: var(--text-muted);
                    font-size: 13px;
                    margin-top: -12px;
                    margin-bottom: 16px;
                }

                .btn {
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    background: var(--accent);
                    color: #ffffff;
                    border: 0;
                    border-radius: var(--radius-md);
                    padding: 8px 14px;
                    font-size: 13px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: background-color var(--transition-fast), transform 0.1s ease;
                    outline: none;
                    gap: 6px;
                }
                .btn:hover {
                    background: var(--accent-hover);
                }
                .btn:active {
                    transform: scale(0.98);
                }
                .btn.secondary {
                    background: var(--bg-input);
                    color: var(--text-primary);
                    border: 1px solid var(--border-color);
                }
                .btn.secondary:hover {
                    border-color: var(--border-hover);
                    background: var(--bg-module);
                }
                .btn.danger {
                    background: var(--danger);
                }
                .btn.danger:hover {
                    background: #dc2626;
                }
                .btn.sm {
                    padding: 4px 10px;
                    font-size: 12px;
                    border-radius: 6px;
                }
                .btn:disabled {
                    opacity: 0.5;
                    cursor: not-allowed;
                    transform: none !important;
                }

                /* Status Indicators */
                .status-group {
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }

                .status-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 10px 14px;
                    background: var(--bg-module);
                    border-radius: var(--radius-md);
                    border: 1px solid var(--border-color);
                }

                .status-label {
                    font-size: 13px;
                    font-weight: 600;
                    color: var(--text-secondary);
                }

                .badge-dot {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    font-size: 12px;
                    font-weight: 700;
                    padding: 3px 10px;
                    border-radius: 999px;
                }

                .dot {
                    width: 7px;
                    height: 7px;
                    border-radius: 50%;
                }

                .status-normal {
                    background: var(--success-light);
                    color: var(--success-text);
                }
                .status-normal .dot {
                    background: var(--success);
                    box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
                    animation: pulse-success 2s infinite;
                }

                .status-abnormal {
                    background: var(--danger-light);
                    color: var(--danger-text);
                }
                .status-abnormal .dot {
                    background: var(--danger);
                    box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.7);
                    animation: pulse-danger 2s infinite;
                }

                .status-offline {
                    background: var(--border-color);
                    color: var(--text-muted);
                }
                .status-offline .dot {
                    background: var(--text-muted);
                }

                @keyframes pulse-success {
                    0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
                    70% { transform: scale(1); box-shadow: 0 0 0 5px rgba(16, 185, 129, 0); }
                    100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
                }

                @keyframes pulse-danger {
                    0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.7); }
                    70% { transform: scale(1); box-shadow: 0 0 0 5px rgba(239, 68, 68, 0); }
                    100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }
                }

                /* Queue style */
                .queue-container {
                    max-height: 250px;
                    overflow-y: auto;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    padding-right: 4px;
                }

                .queue-empty {
                    text-align: center;
                    color: var(--text-muted);
                    font-size: 13px;
                    padding: 24px 0;
                }

                .queue-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 8px 12px;
                    background: var(--bg-module);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                }

                .queue-meta {
                    display: flex;
                    flex-direction: column;
                    gap: 2px;
                }

                .queue-filename {
                    font-size: 13px;
                    font-weight: 600;
                    word-break: break-all;
                }

                .queue-status {
                    font-size: 11px;
                    color: var(--text-muted);
                    font-family: var(--font-mono);
                }

                /* Settings forms styling */
                .settings-module {
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                    padding: 16px;
                    background: var(--bg-module);
                    margin-bottom: 16px;
                    display: flex;
                    flex-direction: column;
                    gap: 14px;
                }
                .settings-module:last-child {
                    margin-bottom: 0;
                }

                .settings-module-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    border-bottom: 1px solid var(--border-color);
                    padding-bottom: 8px;
                }

                .settings-module-header h4 {
                    font-size: 14px;
                    font-weight: 700;
                }

                .form-group {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .form-group label {
                    font-size: 12px;
                    font-weight: 600;
                    color: var(--text-secondary);
                }

                .input-text, select {
                    background: var(--bg-input);
                    color: var(--text-primary);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                    padding: 8px 12px;
                    font-size: 13px;
                    outline: none;
                    width: 100%;
                    transition: border-color var(--transition-fast);
                }
                .input-text:focus, select:focus {
                    border-color: var(--accent);
                }

                /* Extension alias / Paths Chip elements */
                .chips-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 8px;
                    min-height: 40px;
                    padding: 10px;
                    background: var(--bg-input);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                }

                .chip {
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                    padding: 4px 10px;
                    border-radius: 999px;
                    background: var(--accent-light);
                    color: var(--accent-text);
                    font-size: 12px;
                    font-weight: 500;
                    animation: chip-pop 0.2s cubic-bezier(0.175, 0.885, 0.32, 1.275);
                }

                .chip-desc {
                    opacity: 0.85;
                    font-size: 10px;
                    border-left: 1px solid currentColor;
                    padding-left: 6px;
                }

                .chip button {
                    background: transparent;
                    border: 0;
                    color: inherit;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: bold;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    width: 14px;
                    height: 14px;
                    border-radius: 50%;
                }
                .chip button:hover {
                    background: rgba(0, 0, 0, 0.1);
                }

                @keyframes chip-pop {
                    0% { transform: scale(0.8); opacity: 0; }
                    100% { transform: scale(1); opacity: 1; }
                }

                .list-empty-chip {
                    color: var(--text-muted);
                    font-size: 12px;
                    align-self: center;
                }

                .grid-2 {
                    display: grid;
                    grid-template-columns: 1fr;
                    gap: 12px;
                }

                @media (min-width: 640px) {
                    .grid-2 {
                        grid-template-columns: 1fr 1fr;
                    }
                    .grid-4 {
                        display: grid;
                        grid-template-columns: repeat(4, 1fr);
                        gap: 12px;
                    }
                }

                /* Checkbox Grid for sources list */
                .extension-checkbox-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(90px, 1fr));
                    gap: 8px;
                    padding: 12px;
                    background: var(--bg-input);
                    border: 1px solid var(--border-color);
                    border-radius: var(--radius-md);
                }

                .checkbox-card {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding: 6px 10px;
                    border-radius: var(--radius-md);
                    border: 1px solid var(--border-color);
                    background: var(--bg-card);
                    cursor: pointer;
                    user-select: none;
                    transition: border-color var(--transition-fast), background-color var(--transition-fast);
                }

                .checkbox-card:hover {
                    border-color: var(--border-hover);
                }

                .checkbox-card input {
                    cursor: pointer;
                }

                .checkbox-card.active {
                    background: var(--accent-light);
                    border-color: var(--accent);
                    color: var(--accent-text);
                }

                .checkbox-row {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    cursor: pointer;
                    user-select: none;
                    font-size: 13px;
                    font-weight: 500;
                }

                /* Terminal style logs */
                .terminal-card {
                    background: #090d16;
                    border: 1px solid #1e293b;
                    border-radius: var(--radius-lg);
                    color: #e2e8f0;
                    padding: 16px;
                    box-shadow: var(--shadow-lg);
                }

                .terminal-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    border-bottom: 1px solid #1e293b;
                    padding-bottom: 10px;
                    margin-bottom: 12px;
                }

                .terminal-title {
                    font-size: 13px;
                    font-weight: 700;
                    color: #94a3b8;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    font-family: var(--font-mono);
                }

                .terminal-status-light {
                    width: 6px;
                    height: 6px;
                    border-radius: 50%;
                    background: #10b981;
                    box-shadow: 0 0 8px #10b981;
                }

                .terminal-actions {
                    display: flex;
                    gap: 8px;
                }

                .terminal-btn {
                    background: #131b2e;
                    border: 1px solid #1e293b;
                    color: #94a3b8;
                    padding: 4px 8px;
                    font-size: 11px;
                    border-radius: 6px;
                    cursor: pointer;
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    font-weight: 500;
                    transition: border-color var(--transition-fast), color var(--transition-fast);
                }
                .terminal-btn:hover {
                    border-color: #3b82f6;
                    color: #f8fafc;
                }
                .terminal-btn.active {
                    background: #1e1b4b;
                    border-color: #6366f1;
                    color: #a5b4fc;
                }

                .terminal-body {
                    background: #05070c;
                    border-radius: var(--radius-md);
                    padding: 12px;
                    height: 250px;
                    overflow-y: auto;
                    font-family: var(--font-mono);
                    font-size: 12px;
                    line-height: 1.6;
                    white-space: pre-wrap;
                    word-break: break-all;
                    border: 1px solid #111827;
                }

                /* Sticky Action Bar */
                .sticky-action-bar {
                    position: fixed;
                    bottom: 0;
                    left: 0;
                    right: 0;
                    background: var(--bg-card);
                    border-top: 1px solid var(--border-color);
                    padding: 16px 24px;
                    box-shadow: 0 -10px 15px -3px rgb(0 0 0 / 0.05);
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    z-index: 100;
                    transform: translateY(100%);
                    transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1), background-color var(--transition-normal), border-color var(--transition-normal);
                }
                .sticky-action-bar.active {
                    transform: translateY(0);
                }

                .sticky-info {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    color: var(--text-secondary);
                    font-size: 13px;
                    font-weight: 500;
                }

                .sticky-info-dot {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    background: var(--warning);
                }

                .sticky-buttons {
                    display: flex;
                    gap: 10px;
                }

                /* Toast notification */
                .toast-box {
                    position: fixed;
                    bottom: 24px;
                    right: 24px;
                    z-index: 1000;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    pointer-events: none;
                }

                .toast-item {
                    pointer-events: auto;
                    background: var(--bg-card);
                    border: 1px solid var(--border-color);
                    border-left: 4px solid var(--accent);
                    color: var(--text-primary);
                    padding: 12px 18px;
                    border-radius: var(--radius-md);
                    box-shadow: var(--shadow-lg);
                    font-size: 13px;
                    font-weight: 500;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    min-width: 260px;
                    animation: toast-slide-in 0.3s cubic-bezier(0.16, 1, 0.3, 1);
                    transition: opacity 0.2s, transform 0.2s;
                }
                .toast-item.error {
                    border-left-color: var(--danger);
                }
                .toast-item.success {
                    border-left-color: var(--success);
                }

                @keyframes toast-slide-in {
                    0% { transform: translateX(100%) scale(0.9); opacity: 0; }
                    100% { transform: translateX(0) scale(1); opacity: 1; }
                }

                /* Inline Warning banner */
                .warning-banner {
                    display: flex;
                    gap: 8px;
                    padding: 10px 14px;
                    background: var(--warning-light);
                    color: var(--warning-text);
                    border-radius: var(--radius-md);
                    font-size: 12px;
                    font-weight: 500;
                }

                /* Update notice Card */
                .update-card-alert {
                    border: 1px dashed var(--warning);
                    background: var(--warning-light) !important;
                    color: var(--warning-text);
                }
                .update-card-alert h3 {
                    font-size: 15px;
                    font-weight: 700;
                    margin-bottom: 6px;
                }

                .progress-bar-container {
                    width: 100%;
                    background: rgba(0, 0, 0, 0.1);
                    border-radius: 999px;
                    height: 8px;
                    margin-top: 10px;
                    overflow: hidden;
                }
                .progress-bar-fill {
                    background: var(--warning);
                    height: 100%;
                    width: 0%;
                    transition: width 0.3s ease;
                }
                body.dark-mode .progress-bar-container {
                    background: rgba(255, 255, 255, 0.1);
                }
            </style>
            <!-- i18next & Language Setup -->
            <script src="https://unpkg.com/i18next@22.4.15/dist/umd/i18next.min.js"></script>
            <script src="https://unpkg.com/i18next-http-backend@4.0.0/i18nextHttpBackend.min.js"></script>
            <script src="https://unpkg.com/i18next-browser-languagedetector@6.1.6/i18nextBrowserLanguageDetector.min.js"></script>
        </head>
        <body>
            <!-- Top Navigation Bar -->
            <div class="topbar">
                <div class="brand">
                    <h1 data-i18n="heading_admin">phos-watch 管理介面</h1>
                    <span class="brand-badge" id="badgeAppVersion">v0.0.1</span>
                </div>
                <div class="controls-right">
                    <select id="langSel" class="lang-select" aria-label="language selector">
                        <option value="zh-TW">中文 (繁體)</option>
                        <option value="en">English</option>
                    </select>
                    <button class="theme-toggle" id="themeToggle" title="Toggle Theme" aria-label="toggle theme button">
                        <!-- Sun Icon -->
                        <svg id="sunIcon" style="display:none; width: 18px; height: 18px;" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364-6.364l-.707.707M6.343 17.657l-.707.707m0-12.728l.707.707m12.728 12.728l.707.707M12 8a4 4 0 100 8 4 4 0 000-8z"></path></svg>
                        <!-- Moon Icon -->
                        <svg id="moonIcon" style="display:none; width: 18px; height: 18px;" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"></path></svg>
                    </button>
                </div>
            </div>

            <div class="dashboard">
                <!-- Left Column (Status & Queue & Logs) -->
                <div class="column">
                    <!-- Status Card -->
                    <div class="card">
                        <div class="card-title">
                            <span data-i18n="status_dashboard">系統狀態</span>
                            <button type="button" class="btn secondary sm" id="togglePause" data-i18n="toggle_pause">切換暫停</button>
                        </div>
                        <div class="status-group">
                            <div class="status-item">
                                <span class="status-label" data-i18n="queue_length_label">佇列長度：</span>
                                <span id="qlen" style="font-weight: 700; font-size: 15px;">...</span>
                            </div>
                            <div class="status-item">
                                <span class="status-label" data-i18n="pause_state_label">暫停狀態：</span>
                                <span id="pausedState" style="font-weight: 700; font-size: 13px;">...</span>
                            </div>
                            <div class="status-item">
                                <span class="status-label" data-i18n="watcher_status_label">監聽器狀態：</span>
                                <span id="watcherStatus" class="badge-dot status-offline"><span class="dot"></span><span class="label-text">...</span></span>
                            </div>
                            <div class="status-item">
                                <span class="status-label" data-i18n="worker_status_label">處理器狀態：</span>
                                <span id="workerStatus" class="badge-dot status-offline"><span class="dot"></span><span class="label-text">...</span></span>
                            </div>
                        </div>
                        <div style="margin-top:16px; display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-size:12px; font-weight:600; color:var(--text-secondary); display:inline-flex; align-items:center; gap:6px;">
                                <span data-i18n="app_version_label">版本號：</span>
                                <span id="appVersion" style="font-family: var(--font-mono);">...</span>
                                <span id="updateBadge" style="display:none; padding: 2px 6px; font-size: 10px; font-weight: bold; border-radius: 999px; background: var(--warning-light); color: var(--warning-text);"></span>
                            </span>
                            <button type="button" class="btn secondary sm" id="manualCheckUpdateBtn" data-i18n="check_update_btn">手動檢查更新</button>
                        </div>
                    </div>

                    <!-- Update Alert Card -->
                    <div class="card update-card-alert" id="updateCard" style="display:none;">
                        <h3 data-i18n="update_card_title">應用程式更新</h3>
                        <div id="updateStatusMsg" style="font-size: 13px; font-weight: 600;">...</div>
                        
                        <div id="updateNotesSection" style="margin-top:10px; display:none;">
                            <strong style="font-size: 12px;" data-i18n="release_notes_label">釋出說明：</strong>
                            <pre id="updateReleaseNotes" style="white-space: pre-wrap; background: var(--bg-input); border: 1px solid var(--border-color); color: var(--text-primary); max-height: 120px; overflow-y: auto; padding: 8px; font-size: 12px; font-family: var(--font-sans); margin-top:4px; border-radius: 6px;"></pre>
                        </div>

                        <div id="updateProgressContainer" class="progress-bar-container" style="display:none;">
                            <div id="updateProgressBar" class="progress-bar-fill"></div>
                        </div>

                        <div style="margin-top:14px; display:flex; gap:8px;" id="updateCardActions">
                            <button type="button" class="btn sm" id="confirmUpdateBtn" data-i18n="confirm_update_btn">安裝更新</button>
                            <button type="button" class="btn secondary sm" id="closeUpdateCardBtn" data-i18n="close_btn">關閉</button>
                        </div>
                    </div>

                    <!-- Queue Card -->
                    <div class="card">
                        <div class="card-title">
                            <span data-i18n="queue_items">佇列項目</span>
                            <button type="button" class="btn secondary sm" id="refreshQueue" data-i18n="refresh_queue">重新整理佇列</button>
                        </div>
                        <div class="queue-container" id="queueItems">
                            <div class="queue-empty" data-i18n="loading">載入中...</div>
                        </div>
                    </div>

                    <!-- Interactive Terminal Logs -->
                    <div class="terminal-card">
                        <div class="terminal-header">
                            <div class="terminal-title">
                                <span class="terminal-status-light"></span>
                                <span data-i18n="logs">日誌</span>
                            </div>
                            <div class="terminal-actions">
                                <button type="button" class="terminal-btn active" id="terminalAutoScroll" title="Toggle Auto-Scroll">
                                    <svg style="width:12px; height:12px;" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 13l-7 7-7-7m14-6l-7 7-7-7"></path></svg>
                                    Auto
                                </button>
                                <button type="button" class="terminal-btn" id="terminalCopy" title="Copy Logs">
                                    <svg style="width:12px; height:12px;" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"></path></svg>
                                    Copy
                                </button>
                                <button type="button" class="terminal-btn" id="terminalClear" title="Clear Panel">
                                    <svg style="width:12px; height:12px;" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                                    Clear
                                </button>
                            </div>
                        </div>
                        <div class="terminal-body" id="logs"></div>
                    </div>
                </div>

                <!-- Right Column (Settings Form) -->
                <div class="column">
                    <div class="card">
                        <div class="card-title" data-i18n="configuration">設定</div>
                        <div class="card-subtitle" data-i18n="config_desc">可調整監看資料夾、來源副檔名、精確目標副檔名，以及副檔名對應。</div>
                        
                        <form id="configForm">
                            <!-- 1) Watch Paths Section -->
                            <div class="settings-module">
                                <div class="settings-module-header">
                                    <h4 data-i18n="module_watch_title">1) 監聽路徑與遞迴設定</h4>
                                </div>
                                <p class="card-subtitle" style="margin: 0;" data-i18n="module_watch_desc">每一列是一個路徑，可獨立設定是否遞迴監聽。</p>
                                
                                <div class="chips-container" id="watch_paths_rows">
                                    <span class="list-empty-chip" data-i18n="list_empty">尚無項目</span>
                                </div>

                                <div style="display:flex; gap:8px; align-items:center;">
                                    <input type="text" id="watch_path_input" class="input-text" data-i18n-placeholder="watch_paths_placeholder" placeholder="一次新增一個監看資料夾" style="flex:1;" />
                                    <label class="checkbox-row" style="white-space:nowrap;">
                                        <input type="checkbox" id="watch_path_recursive" />
                                        <span data-i18n="recursive_watch">遞迴監看</span>
                                    </label>
                                    <button type="button" id="watch_path_cancel" class="btn secondary" data-i18n="cancel" style="display:none;">取消</button>
                                    <button type="button" id="watch_path_add" class="btn secondary" data-i18n="add_item">新增</button>
                                </div>
                            </div>

                            <!-- 2) Conversion Schemes Section -->
                            <div class="settings-module">
                                <div class="settings-module-header">
                                    <h4 data-i18n="module_scheme_title">2) 轉檔方案</h4>
                                    <label class="checkbox-row">
                                        <input type="checkbox" id="enable_conversion_schemes" class="config-input" />
                                        <span data-i18n="enable_module">啟用此功能</span>
                                    </label>
                                </div>
                                <p class="card-subtitle" style="margin: 0;" data-i18n="module_scheme_desc">可建立多個方案。每個方案包含來源副檔名、目標副檔名與是否刪除原檔。</p>
                                
                                <div id="schemes_rows" style="display:flex; flex-direction:column; gap:8px;"></div>

                                <!-- Scheme add area -->
                                <div style="border: 1px dashed var(--border-color); padding: 12px; border-radius: var(--radius-md); display:flex; flex-direction:column; gap:12px; background: var(--bg-card);">
                                    <div class="form-group">
                                        <label for="scheme_name" data-i18n="scheme_name_label" style="font-weight:600;">方案名稱</label>
                                        <input type="text" id="scheme_name" class="input-text" data-i18n-placeholder="scheme_name_placeholder" placeholder="方案名稱（例如：手機照片）" />
                                    </div>

                                    <div class="form-group">
                                        <label data-i18n="source_extensions_label">來源副檔名</label>
                                        <div id="scheme_sources_checkboxes" class="extension-checkbox-grid">
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="jpg" /> <span>jpg</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="jpeg" /> <span>jpeg</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="png" /> <span>png</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="webp" /> <span>webp</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="gif" /> <span>gif</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="bmp" /> <span>bmp</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="tiff" /> <span>tiff</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="tif" /> <span>tif</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="heic" /> <span>heic</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="heif" /> <span>heif</span></label>
                                            <label class="checkbox-card"><input type="checkbox" name="src_ext" value="avif" /> <span>avif</span></label>
                                        </div>
                                        <input type="text" id="scheme_sources_custom" class="input-text" data-i18n-placeholder="scheme_sources_custom_placeholder" placeholder="自訂其他副檔名，逗號分隔，例如 raw" style="margin-top: 4px;" />
                                        <div class="warning-banner" style="margin-top: 4px;">
                                            <span data-i18n="custom_ext_warning">⚠️ 提示：自訂副檔名不一定支援轉檔，僅限已知相容格式。</span>
                                        </div>
                                    </div>

                                    <div class="grid-2">
                                        <div class="form-group">
                                            <label for="scheme_target" data-i18n="target_format_label">目標副檔名</label>
                                            <select id="scheme_target"></select>
                                        </div>
                                        <div class="form-group">
                                            <label for="scheme_target_custom" data-i18n="target_format_custom_placeholder">自訂目標格式</label>
                                            <input type="text" id="scheme_target_custom" class="input-text" data-i18n-placeholder="target_format_custom_placeholder" placeholder="輸入副檔名，例如 JPG 或 jpeg" />
                                        </div>
                                    </div>

                                    <div style="display:flex; justify-content:space-between; align-items:center; margin-top: 4px;">
                                        <label class="checkbox-row">
                                            <input type="checkbox" id="scheme_delete_original" />
                                            <span data-i18n="delete_original_label">轉檔後刪除原檔</span>
                                        </label>
                                        <div style="display:flex; gap:8px;">
                                            <button type="button" id="scheme_cancel" class="btn secondary" data-i18n="cancel" style="display:none;">取消</button>
                                            <button type="button" id="scheme_add" class="btn secondary" data-i18n="add_item">新增</button>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <!-- 3) Extension Aliases Section -->
                            <div class="settings-module">
                                <div class="settings-module-header">
                                    <h4 data-i18n="module_alias_title">3) 副檔名規範</h4>
                                    <label class="checkbox-row">
                                        <input type="checkbox" id="enable_extension_aliases" class="config-input" />
                                        <span data-i18n="enable_module">啟用此功能</span>
                                    </label>
                                </div>
                                <p class="card-subtitle" style="margin: 0;" data-i18n="module_alias_desc">設定同一格式可能出現的多種副檔名，系統會用來判斷與正規化。</p>

                                <div id="alias_rows" style="display:flex; flex-direction:column; gap:8px;"></div>

                                <div class="grid-2">
                                    <input type="text" id="alias_canonical" class="input-text" data-i18n-placeholder="alias_canonical_placeholder" placeholder="標準副檔名，例如 jpg" />
                                    <div style="display:flex; gap:8px;">
                                        <input type="text" id="alias_values" class="input-text" data-i18n-placeholder="alias_values_placeholder" placeholder="別名，逗號分隔，例如 jpeg,JPG,JPEG" style="flex:1;" />
                                        <button type="button" id="alias_cancel" class="btn secondary" data-i18n="cancel" style="display:none;">取消</button>
                                        <button type="button" id="alias_add" class="btn secondary" data-i18n="add_item">新增</button>
                                    </div>
                                </div>
                            </div>

                            <!-- 4) Log Settings Section -->
                            <div class="settings-module">
                                <div class="settings-module-header">
                                    <h4 data-i18n="module_log_title">4) 日誌備份與輪轉設定</h4>
                                </div>
                                <p class="card-subtitle" style="margin: 0;" data-i18n="module_log_desc">設定日誌檔案的保留上限，若超過限制將會自動輪轉備份。</p>
                                
                                <div class="grid-2">
                                    <div class="form-group">
                                        <label for="log_max_lines" data-i18n="log_max_lines_label">最大行數 (0表示不限制)</label>
                                        <input type="number" id="log_max_lines" class="input-text config-input" min="0" placeholder="例如：10000" />
                                    </div>
                                    <div class="form-group">
                                        <label for="log_max_size_kb" data-i18n="log_max_size_kb_label">最大大小 (KB, 0表示不限制)</label>
                                        <input type="number" id="log_max_size_kb" class="input-text config-input" min="0" placeholder="例如：1024" />
                                    </div>
                                    <div class="form-group">
                                        <label for="log_max_hours" data-i18n="log_max_hours_label">最大時間 (小時, 0表示不限制)</label>
                                        <input type="number" id="log_max_hours" class="input-text config-input" min="0" step="0.5" placeholder="例如：24" />
                                    </div>
                                    <div class="form-group">
                                        <label for="log_backup_count" data-i18n="log_backup_count_label">保留備份檔個數</label>
                                        <input type="number" id="log_backup_count" class="input-text config-input" min="0" placeholder="例如：5" />
                                    </div>
                                </div>
                            </div>

                            <!-- 5) Update Settings Section -->
                            <div class="settings-module">
                                <div class="settings-module-header">
                                    <h4 data-i18n="module_update_title">5) 更新與排程設定</h4>
                                </div>
                                <p class="card-subtitle" style="margin: 0;" data-i18n="module_update_desc">設定自動檢查更新的排程與偏好。</p>

                                <div class="grid-2">
                                    <label class="checkbox-row">
                                        <input type="checkbox" id="update_check_on_startup" class="config-input" />
                                        <span data-i18n="update_check_on_startup_label">啟用啟動時檢查更新</span>
                                    </label>
                                    <label class="checkbox-row">
                                        <input type="checkbox" id="update_include_prerelease" class="config-input" />
                                        <span data-i18n="update_include_prerelease_label">接收測試版/預發布版更新 (Beta/RC)</span>
                                    </label>
                                </div>

                                <div class="grid-2" style="margin-top:8px;">
                                    <div class="form-group">
                                        <label for="update_check_frequency" data-i18n="update_check_frequency_label">檢查頻率</label>
                                        <select id="update_check_frequency" class="config-input">
                                            <option value="none" data-i18n="freq_none">無 (不自動檢查)</option>
                                            <option value="hourly" data-i18n="freq_hourly">每隔幾小時</option>
                                            <option value="daily" data-i18n="freq_daily">每隔幾天</option>
                                            <option value="weekly" data-i18n="freq_weekly">每隔幾週</option>
                                            <option value="specific_time" data-i18n="freq_specific_time">每日特定時間</option>
                                        </select>
                                    </div>
                                    <div class="form-group" id="update_interval_field">
                                        <label for="update_check_interval" data-i18n="update_check_interval_label">檢查間隔數值</label>
                                        <input type="number" id="update_check_interval" class="input-text config-input" min="1" />
                                    </div>
                                    <div class="form-group" id="update_time_field" style="display:none;">
                                        <label for="update_check_time" data-i18n="update_check_time_label">每日檢查時間 (HH:MM)</label>
                                        <input type="text" id="update_check_time" class="input-text config-input" placeholder="例如：02:00" />
                                    </div>
                                </div>
                            </div>
                        </form>
                    </div>
                </div>
            </div>

            <!-- Toast alert box container -->
            <div class="toast-box" id="toastContainer"></div>

            <!-- Sticky Save Changes Bar -->
            <div class="sticky-action-bar" id="floatingSaveBar">
                <div class="sticky-info">
                    <span class="sticky-info-dot"></span>
                    <span data-i18n="unsaved_changes_alert">您有尚未儲存的設定變更。</span>
                </div>
                <div class="sticky-buttons">
                    <button type="button" class="btn secondary" id="discardConfigBtn" data-i18n="discard_btn" style="padding: 10px 18px;">放棄變更</button>
                    <button type="button" class="btn" id="saveConfigBtn" data-i18n="save_config" style="padding: 10px 22px;">儲存設定</button>
                </div>
            </div>

            <!-- Javascript Dashboard Application Logic -->
            <script>
                // i18next state & translator mapping
                function mapLangKey(lng) {
                    if (!lng) return lng;
                    if (String(lng).toLowerCase().startsWith('zh')) return 'zh-TW';
                    return String(lng).split(/[-_]/)[0];
                }

                // Global configs states
                let originalConfig = {};
                let currentConfig = {};
                let isConfigLoaded = false;
                let editingWatchPathIndex = null;
                let editingSchemeIndex = null;
                let editingAliasCanonical = null;

                i18next.use(i18nextHttpBackend).use(i18nextBrowserLanguageDetector).init({
                    fallbackLng: 'zh-TW',
                    debug: false,
                    backend: { loadPath: '/static/locales/{{lng}}.json' }
                }, function(err, t) {
                    if (err) console.error('i18next init error', err);
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
                    try { document.title = i18next.t('title'); } catch(e){}
                    document.querySelectorAll('[data-i18n]').forEach(el => {
                        const key = el.getAttribute('data-i18n');
                        try { el.innerText = i18next.t(key); } catch(e){}
                    });
                    document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
                        const key = el.getAttribute('data-i18n-placeholder');
                        try { el.setAttribute('placeholder', i18next.t(key)); } catch(e){}
                    });

                    // Re-render lists to reflect dynamic keys (e.g. deletion buttons, recursive text)
                    if (isConfigLoaded) {
                        renderWatchPaths();
                        renderSchemes();
                        renderAliases();
                        renderTargetSelectOptions();
                    }
                }

                document.getElementById('langSel').addEventListener('change', (ev) => {
                    const sel = ev.target.value;
                    const mapped = mapLangKey(sel);
                    i18next.changeLanguage(mapped).then(() => { 
                        localStorage.setItem('phos_lang', mapped); 
                        translatePage(); 
                    });
                });

                const savedLang = localStorage.getItem('phos_lang');
                if (savedLang) {
                    i18next.changeLanguage(savedLang).then(translatePage).catch(()=>{});
                    document.getElementById('langSel').value = savedLang;
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
                        'worker_status_label': '處理器狀態：',
                        'unsaved_changes_alert': '您有尚未儲存的設定變更。',
                        'discard_btn': '放棄變更'
                    };
                    return fallbacks[key] || key;
                }

                // Toast alerts helper
                function showToast(message, isError = false) {
                    const container = document.getElementById('toastContainer');
                    const toast = document.createElement('div');
                    toast.className = 'toast-item' + (isError ? ' error' : ' success');
                    
                    const icon = document.createElement('span');
                    icon.textContent = isError ? '❌' : '✨';
                    
                    const text = document.createElement('span');
                    text.textContent = message;
                    
                    toast.appendChild(icon);
                    toast.appendChild(text);
                    container.appendChild(toast);
                    
                    setTimeout(() => {
                        toast.style.opacity = '0';
                        toast.style.transform = 'translateY(-10px) scale(0.95)';
                        setTimeout(() => toast.remove(), 200);
                    }, 3000);
                }

                // Deep Comparison for detecting configurations modifications
                function checkConfigChanges() {
                    const changesDetected = JSON.stringify(originalConfig) !== JSON.stringify(currentConfig);
                    const bar = document.getElementById('floatingSaveBar');
                    if (changesDetected) {
                        bar.classList.add('active');
                    } else {
                        bar.classList.remove('active');
                    }
                }

                // Dark/Light Theme Switching logic
                const themeBtn = document.getElementById('themeToggle');
                const sunIcon = document.getElementById('sunIcon');
                const moonIcon = document.getElementById('moonIcon');

                function applyTheme(isDark) {
                    if (isDark) {
                        document.body.classList.add('dark-mode');
                        sunIcon.style.display = 'block';
                        moonIcon.style.display = 'none';
                        localStorage.setItem('theme', 'dark');
                    } else {
                        document.body.classList.remove('dark-mode');
                        sunIcon.style.display = 'none';
                        moonIcon.style.display = 'block';
                        localStorage.setItem('theme', 'light');
                    }
                }

                const savedTheme = localStorage.getItem('theme');
                const systemDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
                applyTheme(savedTheme === 'dark' || (!savedTheme && systemDark));

                themeBtn.addEventListener('click', () => {
                    applyTheme(!document.body.classList.contains('dark-mode'));
                });

                // Dual-binding configuration controls
                function syncFormInputsToState() {
                    document.querySelectorAll('.config-input').forEach(el => {
                        const prop = el.id;
                        if (!(prop in currentConfig)) return;

                        if (el.tagName === 'SELECT') {
                            el.value = currentConfig[prop];
                        } else if (el.type === 'checkbox') {
                            el.checked = !!currentConfig[prop];
                        } else if (el.type === 'number') {
                            el.value = currentConfig[prop] || 0;
                        } else {
                            el.value = currentConfig[prop] || '';
                        }
                    });
                    updateUpdateFieldsUI();
                }

                document.querySelectorAll('.config-input').forEach(el => {
                    el.addEventListener('input', (ev) => {
                        const prop = ev.target.id;
                        let val = ev.target.value;
                        
                        if (ev.target.type === 'checkbox') {
                            val = ev.target.checked;
                        } else if (ev.target.type === 'number') {
                            val = val === '' ? 0 : (ev.target.step ? parseFloat(val) : parseInt(val));
                        }
                        
                        currentConfig[prop] = val;
                        checkConfigChanges();
                        
                        if (prop === 'update_check_frequency') {
                            updateUpdateFieldsUI();
                        }
                    });
                });

                function updateUpdateFieldsUI() {
                    const freq = document.getElementById('update_check_frequency').value;
                    const intervalField = document.getElementById('update_interval_field');
                    const timeField = document.getElementById('update_time_field');
                    
                    if (freq === 'none') {
                        intervalField.style.display = 'none';
                        timeField.style.display = 'none';
                    } else if (freq === 'specific_time') {
                        intervalField.style.display = 'none';
                        timeField.style.display = 'flex';
                    } else {
                        intervalField.style.display = 'flex';
                        timeField.style.display = 'none';
                    }
                }

                // Render Dynamic Options for Target Extensions
                function renderTargetSelectOptions() {
                    const select = document.getElementById('scheme_target');
                    if (!select) return;
                    
                    const values = [];
                    const seen = new Set();
                    const push = (v) => {
                        const clean = String(v || '').trim().replace(/^\./, '');
                        if (clean && !seen.has(clean)) {
                            seen.add(clean);
                            values.push(clean);
                        }
                    };

                    ['jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'avif', 'tif', 'tiff', 'JPG', 'JPEG', 'PNG', 'WEBP', 'GIF', 'BMP', 'AVIF', 'TIF', 'TIFF'].forEach(push);
                    
                    if (currentConfig.extension_aliases) {
                        Object.entries(currentConfig.extension_aliases).forEach(([canonical, aliasList]) => {
                            push(canonical);
                            if (Array.isArray(aliasList)) aliasList.forEach(push);
                        });
                    }
                    
                    const origSelected = select.value;
                    select.innerHTML = '';
                    
                    values.forEach(v => {
                        const opt = document.createElement('option');
                        opt.value = v;
                        opt.textContent = '.' + v;
                        select.appendChild(opt);
                    });
                    
                    if (values.includes(origSelected)) {
                        select.value = origSelected;
                    }
                }

                // 1) Watch Paths Render & Actions
                function startEditWatchPath(index) {
                    editingWatchPathIndex = index;
                    const item = currentConfig.watch_paths[index];
                    document.getElementById('watch_path_input').value = item.path;
                    document.getElementById('watch_path_recursive').checked = item.recursive;
                    
                    const addBtn = document.getElementById('watch_path_add');
                    addBtn.textContent = t('save');
                    addBtn.removeAttribute('data-i18n');
                    
                    document.getElementById('watch_path_cancel').style.display = 'inline-flex';
                }

                function resetWatchPathEditState() {
                    editingWatchPathIndex = null;
                    document.getElementById('watch_path_input').value = '';
                    document.getElementById('watch_path_recursive').checked = false;
                    
                    const addBtn = document.getElementById('watch_path_add');
                    addBtn.textContent = t('add_item');
                    addBtn.setAttribute('data-i18n', 'add_item');
                    
                    document.getElementById('watch_path_cancel').style.display = 'none';
                }

                function renderWatchPaths() {
                    const container = document.getElementById('watch_paths_rows');
                    container.innerHTML = '';
                    
                    const paths = currentConfig.watch_paths || [];
                    if (paths.length === 0) {
                        const empty = document.createElement('span');
                        empty.className = 'list-empty-chip';
                        empty.setAttribute('data-i18n', 'list_empty');
                        empty.textContent = t('list_empty');
                        container.appendChild(empty);
                        return;
                    }
                    
                    paths.forEach((item, index) => {
                        const chip = document.createElement('span');
                        chip.className = 'chip';
                        
                        const text = document.createElement('span');
                        text.textContent = item.path;
                        
                        const desc = document.createElement('span');
                        desc.className = 'chip-desc';
                        desc.textContent = item.recursive ? t('recursive_yes') : t('recursive_no');
                        
                        const editBtn = document.createElement('button');
                        editBtn.type = 'button';
                        editBtn.textContent = '✏️';
                        editBtn.setAttribute('aria-label', t('edit'));
                        editBtn.style.background = 'transparent';
                        editBtn.style.border = '0';
                        editBtn.style.color = 'inherit';
                        editBtn.style.cursor = 'pointer';
                        editBtn.style.fontSize = '12px';
                        editBtn.style.display = 'flex';
                        editBtn.style.alignItems = 'center';
                        editBtn.style.justifyContent = 'center';
                        editBtn.style.width = '14px';
                        editBtn.style.height = '14px';
                        editBtn.style.borderRadius = '50%';
                        editBtn.onclick = () => {
                            startEditWatchPath(index);
                        };
                        
                        const delBtn = document.createElement('button');
                        delBtn.type = 'button';
                        delBtn.textContent = '×';
                        delBtn.setAttribute('aria-label', t('remove_item'));
                        delBtn.onclick = () => {
                            if (editingWatchPathIndex === index) {
                                resetWatchPathEditState();
                            } else if (editingWatchPathIndex > index) {
                                editingWatchPathIndex--;
                            }
                            currentConfig.watch_paths.splice(index, 1);
                            renderWatchPaths();
                            checkConfigChanges();
                        };
                        
                        chip.appendChild(text);
                        chip.appendChild(desc);
                        chip.appendChild(editBtn);
                        chip.appendChild(delBtn);
                        container.appendChild(chip);
                    });
                }

                document.getElementById('watch_path_add').addEventListener('click', () => {
                    const input = document.getElementById('watch_path_input');
                    const recursive = document.getElementById('watch_path_recursive');
                    const path = input.value.trim();
                    if (!path) return;

                    if (/[<>|?*]/.test(path)) {
                        showToast(t('invalid_path_error'), true);
                        return;
                    }

                    if (!currentConfig.watch_paths) currentConfig.watch_paths = [];
                    
                    if (editingWatchPathIndex !== null) {
                        currentConfig.watch_paths[editingWatchPathIndex] = {
                            path: path,
                            recursive: recursive.checked
                        };
                        resetWatchPathEditState();
                    } else {
                        currentConfig.watch_paths.push({
                            path: path,
                            recursive: recursive.checked
                        });
                        input.value = '';
                        recursive.checked = false;
                    }

                    renderWatchPaths();
                    checkConfigChanges();
                });

                document.getElementById('watch_path_cancel').addEventListener('click', () => {
                    resetWatchPathEditState();
                });

                // Checkbox Active State Visual Handler
                document.querySelectorAll('#scheme_sources_checkboxes input').forEach(cb => {
                    cb.addEventListener('change', (ev) => {
                        const label = ev.target.closest('.checkbox-card');
                        if (ev.target.checked) {
                            label.classList.add('active');
                        } else {
                            label.classList.remove('active');
                        }
                    });
                });

                // 2) Conversion Schemes Render & Actions
                function startEditScheme(index) {
                    editingSchemeIndex = index;
                    const sc = currentConfig.conversion_schemes[index];
                    document.getElementById('scheme_name').value = sc.name || '';
                    document.getElementById('scheme_delete_original').checked = !!sc.delete_original;
                    
                    const defaultExts = ['jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'tiff', 'tif', 'heic', 'heif', 'avif'];
                    document.querySelectorAll('#scheme_sources_checkboxes input').forEach(cb => {
                        const val = cb.value;
                        const isChecked = sc.source_extensions.includes(val);
                        cb.checked = isChecked;
                        const label = cb.closest('.checkbox-card');
                        if (isChecked) {
                            label.classList.add('active');
                        } else {
                            label.classList.remove('active');
                        }
                    });
                    
                    const customExts = sc.source_extensions.filter(ext => !defaultExts.includes(ext));
                    document.getElementById('scheme_sources_custom').value = customExts.join(', ');
                    
                    const select = document.getElementById('scheme_target');
                    const options = Array.from(select.options).map(opt => opt.value);
                    if (options.includes(sc.target_format)) {
                        select.value = sc.target_format;
                        document.getElementById('scheme_target_custom').value = '';
                    } else {
                        select.value = '';
                        document.getElementById('scheme_target_custom').value = sc.target_format;
                    }
                    
                    const addBtn = document.getElementById('scheme_add');
                    addBtn.textContent = t('save');
                    addBtn.removeAttribute('data-i18n');
                    
                    document.getElementById('scheme_cancel').style.display = 'inline-flex';
                }

                function resetSchemeEditState() {
                    editingSchemeIndex = null;
                    document.getElementById('scheme_name').value = '';
                    document.getElementById('scheme_sources_custom').value = '';
                    document.getElementById('scheme_target_custom').value = '';
                    document.getElementById('scheme_delete_original').checked = false;
                    document.querySelectorAll('input[name="src_ext"]').forEach(cb => {
                        cb.checked = false;
                        cb.closest('.checkbox-card').classList.remove('active');
                    });
                    
                    const addBtn = document.getElementById('scheme_add');
                    addBtn.textContent = t('add_item');
                    addBtn.setAttribute('data-i18n', 'add_item');
                    
                    document.getElementById('scheme_cancel').style.display = 'none';
                }

                function renderSchemes() {
                    const container = document.getElementById('schemes_rows');
                    container.innerHTML = '';
                    
                    const schemes = currentConfig.conversion_schemes || [];
                    schemes.forEach((sc, index) => {
                        const row = document.createElement('div');
                        row.className = 'status-item';
                        row.style.flexWrap = 'wrap';
                        row.style.gap = '8px';
                        
                        const left = document.createElement('div');
                        left.style.display = 'flex';
                        left.style.flexDirection = 'column';
                        left.style.gap = '2px';
                        
                        const title = document.createElement('strong');
                        title.textContent = sc.name || ('scheme-' + (index + 1));
                        title.style.fontSize = '13px';
                        
                        const details = document.createElement('span');
                        details.className = 'queue-status';
                        details.textContent = `${sc.source_extensions.map(x => '.' + x).join(', ')} ➔ .${sc.target_format} (${sc.delete_original ? t('delete_original_yes') : t('delete_original_no')})`;
                        
                        left.appendChild(title);
                        left.appendChild(details);
                        
                        const right = document.createElement('div');
                        right.style.display = 'flex';
                        right.style.alignItems = 'center';
                        right.style.gap = '10px';

                        const enabledLabel = document.createElement('label');
                        enabledLabel.className = 'checkbox-row';
                        enabledLabel.style.fontSize = '12px';
                        
                        const enabledCb = document.createElement('input');
                        enabledCb.type = 'checkbox';
                        enabledCb.checked = !!sc.enabled;
                        enabledCb.onchange = (ev) => {
                            sc.enabled = ev.target.checked;
                            checkConfigChanges();
                        };
                        enabledLabel.appendChild(enabledCb);
                        enabledLabel.appendChild(document.createTextNode(t('enable_module')));
                        
                        const editBtn = document.createElement('button');
                        editBtn.type = 'button';
                        editBtn.className = 'btn secondary sm';
                        editBtn.textContent = t('edit');
                        editBtn.onclick = () => {
                            startEditScheme(index);
                        };
                        
                        const delBtn = document.createElement('button');
                        delBtn.type = 'button';
                        delBtn.className = 'btn danger sm';
                        delBtn.textContent = t('remove');
                        delBtn.onclick = () => {
                            if (editingSchemeIndex === index) {
                                resetSchemeEditState();
                            } else if (editingSchemeIndex > index) {
                                editingSchemeIndex--;
                            }
                            currentConfig.conversion_schemes.splice(index, 1);
                            renderSchemes();
                            renderTargetSelectOptions();
                            checkConfigChanges();
                        };
                        
                        right.appendChild(enabledLabel);
                        right.appendChild(editBtn);
                        right.appendChild(delBtn);
                        
                        row.appendChild(left);
                        row.appendChild(right);
                        container.appendChild(row);
                    });
                }

                document.getElementById('scheme_add').addEventListener('click', () => {
                    const name = document.getElementById('scheme_name').value.trim();
                    const customSources = document.getElementById('scheme_sources_custom').value.trim();
                    const targetSelect = document.getElementById('scheme_target').value;
                    const targetCustom = document.getElementById('scheme_target_custom').value.trim();
                    const deleteOrig = document.getElementById('scheme_delete_original').checked;

                    // Collect selected source checkboxes
                    const checkedSources = Array.from(document.querySelectorAll('input[name="src_ext"]:checked')).map(cb => cb.value);
                    if (customSources) {
                        customSources.split(',').forEach(x => {
                            const clean = x.trim().toLowerCase().replace(/^\./, '');
                            if (clean && !checkedSources.includes(clean)) checkedSources.push(clean);
                        });
                    }

                    const target = (targetCustom || targetSelect || '').trim().replace(/^\./, '');
                    
                    if (!target || checkedSources.length === 0) {
                        showToast("Please provide source extensions and target format", true);
                        return;
                    }

                    if (!currentConfig.conversion_schemes) currentConfig.conversion_schemes = [];
                    
                    if (editingSchemeIndex !== null) {
                        const currentEnabled = !!currentConfig.conversion_schemes[editingSchemeIndex].enabled;
                        currentConfig.conversion_schemes[editingSchemeIndex] = {
                            name: name || ('scheme-' + (editingSchemeIndex + 1)),
                            source_extensions: checkedSources,
                            target_format: target,
                            delete_original: deleteOrig,
                            enabled: currentEnabled
                        };
                        resetSchemeEditState();
                    } else {
                        currentConfig.conversion_schemes.push({
                            name: name || ('scheme-' + (currentConfig.conversion_schemes.length + 1)),
                            source_extensions: checkedSources,
                            target_format: target,
                            delete_original: deleteOrig,
                            enabled: true
                        });
                        
                        // Clear fields
                        document.getElementById('scheme_name').value = '';
                        document.getElementById('scheme_sources_custom').value = '';
                        document.getElementById('scheme_target_custom').value = '';
                        document.getElementById('scheme_delete_original').checked = false;
                        document.querySelectorAll('input[name="src_ext"]:checked').forEach(cb => {
                            cb.checked = false;
                            cb.closest('.checkbox-card').classList.remove('active');
                        });
                    }

                    renderSchemes();
                    renderTargetSelectOptions();
                    checkConfigChanges();
                });

                document.getElementById('scheme_cancel').addEventListener('click', () => {
                    resetSchemeEditState();
                });

                // 3) Extension Aliases Render & Actions
                function startEditAlias(canonical) {
                    editingAliasCanonical = canonical;
                    document.getElementById('alias_canonical').value = canonical;
                    document.getElementById('alias_values').value = (currentConfig.extension_aliases[canonical] || []).join(', ');
                    
                    const addBtn = document.getElementById('alias_add');
                    addBtn.textContent = t('save');
                    addBtn.removeAttribute('data-i18n');
                    
                    document.getElementById('alias_cancel').style.display = 'inline-flex';
                }

                function resetAliasEditState() {
                    editingAliasCanonical = null;
                    document.getElementById('alias_canonical').value = '';
                    document.getElementById('alias_values').value = '';
                    
                    const addBtn = document.getElementById('alias_add');
                    addBtn.textContent = t('add_item');
                    addBtn.setAttribute('data-i18n', 'add_item');
                    
                    document.getElementById('alias_cancel').style.display = 'none';
                }

                function renderAliases() {
                    const container = document.getElementById('alias_rows');
                    container.innerHTML = '';
                    
                    const aliases = currentConfig.extension_aliases || {};
                    Object.entries(aliases).forEach(([canonical, aliasList]) => {
                        if (!aliasList || aliasList.length === 0) return;
                        
                        const row = document.createElement('div');
                        row.className = 'status-item';
                        
                        const left = document.createElement('div');
                        left.style.display = 'flex';
                        left.style.flexDirection = 'column';
                        left.style.gap = '2px';
                        
                        const title = document.createElement('strong');
                        title.textContent = '.' + canonical;
                        title.style.fontSize = '13px';
                        
                        const details = document.createElement('span');
                        details.className = 'queue-status';
                        details.textContent = aliasList.map(x => '.' + x).join(', ');
                        
                        left.appendChild(title);
                        left.appendChild(details);
                        
                        const actions = document.createElement('div');
                        actions.style.display = 'flex';
                        actions.style.gap = '6px';
                        
                        const editBtn = document.createElement('button');
                        editBtn.type = 'button';
                        editBtn.className = 'btn secondary sm';
                        editBtn.textContent = t('edit');
                        editBtn.onclick = () => {
                            startEditAlias(canonical);
                        };
                        
                        const delBtn = document.createElement('button');
                        delBtn.type = 'button';
                        delBtn.className = 'btn danger sm';
                        delBtn.textContent = t('remove');
                        delBtn.onclick = () => {
                            if (editingAliasCanonical === canonical) {
                                resetAliasEditState();
                            }
                            delete currentConfig.extension_aliases[canonical];
                            renderAliases();
                            renderTargetSelectOptions();
                            checkConfigChanges();
                        };
                        
                        actions.appendChild(editBtn);
                        actions.appendChild(delBtn);
                        
                        row.appendChild(left);
                        row.appendChild(actions);
                        container.appendChild(row);
                    });
                }

                document.getElementById('alias_add').addEventListener('click', () => {
                    const canonical = document.getElementById('alias_canonical').value.trim().toLowerCase().replace(/^\./, '');
                    const valuesStr = document.getElementById('alias_values').value.trim();
                    if (!canonical || !valuesStr) return;

                    const list = valuesStr.split(',').map(x => x.trim().replace(/^\./, '')).filter(Boolean);
                    if (list.length === 0) return;

                    if (!currentConfig.extension_aliases) currentConfig.extension_aliases = {};
                    
                    if (editingAliasCanonical !== null) {
                        delete currentConfig.extension_aliases[editingAliasCanonical];
                    }
                    currentConfig.extension_aliases[canonical] = list;
                    resetAliasEditState();

                    renderAliases();
                    renderTargetSelectOptions();
                    checkConfigChanges();
                });

                document.getElementById('alias_cancel').addEventListener('click', () => {
                    resetAliasEditState();
                });

                // Load configuration from API
                async function loadConfig() {
                    try {
                        const res = await fetch('/config');
                        const cfg = await res.json();
                        
                        originalConfig = JSON.parse(JSON.stringify(cfg));
                        currentConfig = JSON.parse(JSON.stringify(cfg));
                        
                        isConfigLoaded = true;
                        
                        syncFormInputsToState();
                        renderWatchPaths();
                        renderSchemes();
                        renderAliases();
                        renderTargetSelectOptions();
                        
                        checkConfigChanges();
                    } catch(e) {
                        console.error('Failed to load configuration:', e);
                        showToast('Failed to load configuration', true);
                    }
                }

                // Discard changes
                document.getElementById('discardConfigBtn').addEventListener('click', () => {
                    currentConfig = JSON.parse(JSON.stringify(originalConfig));
                    resetWatchPathEditState();
                    resetSchemeEditState();
                    resetAliasEditState();
                    syncFormInputsToState();
                    renderWatchPaths();
                    renderSchemes();
                    renderAliases();
                    renderTargetSelectOptions();
                    checkConfigChanges();
                    showToast('變更已放棄');
                });

                // Save configuration to API
                document.getElementById('saveConfigBtn').addEventListener('click', async () => {
                    const btn = document.getElementById('saveConfigBtn');
                    btn.disabled = true;
                    try {
                        const res = await fetch('/config', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(currentConfig)
                        });
                        const data = await res.json();
                        if (data.ok) {
                            originalConfig = JSON.parse(JSON.stringify(currentConfig));
                            checkConfigChanges();
                            showToast(t('saved'));
                        } else {
                            showToast(t('save_failed') + ': ' + data.error, true);
                        }
                    } catch(e) {
                        showToast(t('save_error'), true);
                    } finally {
                        btn.disabled = false;
                    }
                });

                // Status & control polling
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

                function updateComponentStatus(id, state) {
                    const badge = document.getElementById(id);
                    const dotText = badge.querySelector('.label-text');
                    badge.className = 'badge-dot';
                    
                    if (state === 'running' || state === 'idle' || state === 'normal' || state === 'active') {
                        badge.classList.add('status-normal');
                        dotText.innerText = t('status_normal');
                    } else if (state === 'abnormal' || state === 'error') {
                        badge.classList.add('status-abnormal');
                        dotText.innerText = t('status_abnormal');
                    } else {
                        badge.classList.add('status-offline');
                        dotText.innerText = t('status_offline');
                    }
                }

                async function refreshQueue() {
                    try {
                        const res = await fetch('/queue');
                        const data = await res.json();
                        const container = document.getElementById('queueItems');
                        
                        if (!data.items || data.items.length === 0) {
                            container.innerHTML = `<div class="queue-empty" data-i18n="queue_empty">${t('queue_empty')}</div>`;
                            return;
                        }

                        container.innerHTML = '';
                        data.items.forEach(item => {
                            const row = document.createElement('div');
                            row.className = 'queue-item';

                            const meta = document.createElement('div');
                            meta.className = 'queue-meta';

                            const filename = document.createElement('span');
                            filename.className = 'queue-filename';
                            filename.textContent = item.path;

                            const status = document.createElement('span');
                            status.className = 'queue-status';
                            status.textContent = `${item.status} | retries: ${item.retries}`;

                            meta.appendChild(filename);
                            meta.appendChild(status);

                            const actions = document.createElement('div');
                            actions.style.display = 'flex';
                            actions.style.gap = '6px';

                            const requeueBtn = document.createElement('button');
                            requeueBtn.type = 'button';
                            requeueBtn.className = 'btn sm secondary';
                            requeueBtn.textContent = t('requeue');
                            requeueBtn.onclick = async () => {
                                try {
                                    const r = await fetch('/queue/requeue', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ id: item.id })
                                    });
                                    const resData = await r.json();
                                    if (resData.ok) {
                                        showToast(t('requeued'));
                                        refreshQueue();
                                    } else {
                                        showToast(t('requeue_failed'), true);
                                    }
                                } catch(e) { showToast(t('requeue_error'), true); }
                            };

                            const removeBtn = document.createElement('button');
                            removeBtn.type = 'button';
                            removeBtn.className = 'btn sm danger';
                            removeBtn.textContent = t('remove');
                            removeBtn.onclick = async () => {
                                try {
                                    const r = await fetch('/queue/remove', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ id: item.id })
                                    });
                                    const resData = await r.json();
                                    if (resData.ok) {
                                        showToast(t('removed'));
                                        refreshQueue();
                                    } else {
                                        showToast(t('remove_failed'), true);
                                    }
                                } catch(e) { showToast(t('remove_error'), true); }
                            };

                            actions.appendChild(requeueBtn);
                            actions.appendChild(removeBtn);
                            row.appendChild(meta);
                            row.appendChild(actions);
                            container.appendChild(row);
                        });
                    } catch(e) { console.error(e); }
                }

                document.getElementById('refreshQueue').addEventListener('click', refreshQueue);

                async function pollQueue() {
                    while (true) {
                        try {
                            await refreshQueue();
                        } catch (e) { console.error(e); }
                        await new Promise(r => setTimeout(r, 5000));
                    }
                }

                document.getElementById('togglePause').addEventListener('click', async () => {
                    try {
                        const currPaused = document.getElementById('pausedState').innerText === t('paused');
                        const res = await fetch('/control', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ paused: !currPaused })
                        });
                        const j = await res.json();
                        document.getElementById('pausedState').innerText = j.paused ? t('paused') : t('running');
                    } catch(e) { console.error(e); }
                });

                // Updater functions
                let updaterPolling = false;
                let isUpdating = false;
                let isManualCheck = false;

                async function checkUpdaterStatusOnce() {
                    try {
                        const res = await fetch('/updater/status');
                        const j = await res.json();
                        
                        document.getElementById('appVersion').innerText = j.current_version;
                        document.getElementById('badgeAppVersion').innerText = 'v' + j.current_version;
                        
                        const updateBadge = document.getElementById('updateBadge');
                        if (j.update_available) {
                            updateBadge.style.display = 'inline-block';
                            updateBadge.innerText = t('new_version_available') + ' (' + (j.latest_version || '') + ')';
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
                                updateStatusMsg.innerText = t('update_prompt') + ' ' + (j.latest_version || '');
                                actions.style.display = 'flex';
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
                                    showToast(t('already_latest'));
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
                            actions.style.display = 'flex';
                            confirmBtn.style.display = 'none';
                        } else {
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
                    } catch(e) { console.error(e); }
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

                // Terminal Logs Interactive controls
                let autoScrollEnabled = true;
                const autoScrollBtn = document.getElementById('terminalAutoScroll');
                const copyBtn = document.getElementById('terminalCopy');
                const clearBtn = document.getElementById('terminalClear');
                const terminalLogs = document.getElementById('logs');

                autoScrollBtn.addEventListener('click', () => {
                    autoScrollEnabled = !autoScrollEnabled;
                    autoScrollBtn.classList.toggle('active', autoScrollEnabled);
                });

                clearBtn.addEventListener('click', () => {
                    terminalLogs.textContent = '';
                });

                copyBtn.addEventListener('click', () => {
                    const text = terminalLogs.textContent;
                    if (!text) return;
                    
                    navigator.clipboard.writeText(text).then(() => {
                        showToast('Logs copied to clipboard');
                    }).catch(() => {
                        // Fallback copy method
                        const textArea = document.createElement("textarea");
                        textArea.value = text;
                        document.body.appendChild(textArea);
                        textArea.select();
                        try {
                            document.execCommand('copy');
                            showToast('Logs copied to clipboard');
                        } catch (err) {
                            showToast('Failed to copy logs', true);
                        }
                        document.body.removeChild(textArea);
                    });
                });

                // WebSocket Terminal logs stream connection
                const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/logs');
                ws.onmessage = (ev) => {
                    terminalLogs.textContent += ev.data + '\n';
                    if (autoScrollEnabled) {
                        terminalLogs.scrollTop = terminalLogs.scrollHeight;
                    }
                };

                // Initialization triggers
                pollStatus();
                pollQueue();
                loadConfig();
                checkUpdaterStatusOnce();
            </script>
        </body>
    </html>
    '''
    return HTMLResponse(html)
