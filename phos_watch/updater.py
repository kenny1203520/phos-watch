import asyncio
import datetime
import json
import logging
import os
import platform
import subprocess
import sys
import threading
import urllib.request
import urllib.error
import zipfile

from . import __version__
from . import control

logger = logging.getLogger(__name__)

# Global updater state
status_lock = threading.Lock()
update_lock = threading.Lock()

status = {
    "status": "idle",             # "idle", "checking", "downloading", "applying", "done", "error"
    "current_version": __version__,
    "latest_version": None,
    "release_notes": None,
    "update_available": False,
    "progress": 0,
    "error_message": None
}

def get_updater_status():
    with status_lock:
        return status.copy()

def parse_version(v_str):
    """
    Parses version strings (e.g. 'v0.1.0-beta.1') into a comparable tuple.
    Handles release tags, alphas, betas, rcs, and maps them to weights:
    alpha=1, beta=2, rc=3, stable/no-prefix=4
    """
    v_str = str(v_str or '').lower().strip().lstrip('v')
    
    # Check for pre-release suffix
    if '-' in v_str:
        main_part, pre_part = v_str.split('-', 1)
    else:
        main_part, pre_part = v_str, None
        
    # Parse main numbers (e.g. '0.1.0' -> [0, 1, 0])
    main_numbers = []
    for p in main_part.split('.'):
        digits = []
        for c in p:
            if c.isdigit():
                digits.append(c)
            else:
                break
        main_numbers.append(int("".join(digits)) if digits else 0)
    while len(main_numbers) < 3:
        main_numbers.append(0)
        
    # Determine pre-release weight and sub-version number
    if pre_part is None:
        pre_val = (4, 0)
    else:
        label = "alpha"
        num = 0
        if "alpha" in pre_part:
            label = "alpha"
        elif "beta" in pre_part:
            label = "beta"
        elif "rc" in pre_part:
            label = "rc"
            
        # extract number from pre-release part (e.g. 'beta.1' -> 1)
        digits = []
        for c in pre_part:
            if c.isdigit():
                digits.append(c)
        if digits:
            num = int("".join(digits))
            
        weights = {"alpha": 1, "beta": 2, "rc": 3}
        pre_val = (weights.get(label, 0), num)
        
    return tuple(main_numbers[:3]) + pre_val

def check_update_sync(include_prerelease=False):
    """
    Synchronously checks for updates from GitHub Releases API.
    """
    global status
    with status_lock:
        if status["status"] in ("checking", "downloading", "applying"):
            return status.copy()
        status["status"] = "checking"
        status["error_message"] = None
        
    try:
        if include_prerelease:
            # Fetch list of releases, the first one is the newest (stable or prerelease)
            url = "https://api.github.com/repos/kenny1203520/phos-watch/releases"
        else:
            # GitHub /latest automatically returns the newest stable release
            url = "https://api.github.com/repos/kenny1203520/phos-watch/releases/latest"
            
        req = urllib.request.Request(url, headers={'User-Agent': 'phos-watch-updater'})
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                res_data = json.loads(response.read().decode('utf-8'))
                
            if isinstance(res_data, list):
                if not res_data:
                    raise Exception("No releases found in the repository list.")
                # Get the first one (most recent)
                data = res_data[0]
            else:
                data = res_data
                
            tag_name = data.get("tag_name", "v0.0.0")
            body = data.get("body", "")
            
            current_tuple = parse_version(__version__)
            latest_tuple = parse_version(tag_name)
            update_available = latest_tuple > current_tuple
            
            with status_lock:
                status["latest_version"] = tag_name
                status["release_notes"] = body
                status["update_available"] = update_available
                status["status"] = "idle"
                status["error_message"] = None
        except urllib.error.HTTPError as he:
            if he.code == 404:
                if include_prerelease:
                    raise Exception("GitHub repository not found or private (HTTP 404).")
                else:
                    logger.info("No stable releases found on GitHub (HTTP 404). Setting update_available = False.")
                    with status_lock:
                        status["latest_version"] = None
                        status["release_notes"] = ""
                        status["update_available"] = False
                        status["status"] = "idle"
                        status["error_message"] = None
            else:
                raise he
                
    except Exception as e:
        logger.exception("Failed to check for updates")
        with status_lock:
            status["status"] = "error"
            status["error_message"] = str(e)
            
    return get_updater_status()

def background_check_update(include_prerelease=False):
    threading.Thread(target=check_update_sync, args=(include_prerelease,), daemon=True).start()

def download_file_with_progress(url, dest_path):
    req = urllib.request.Request(url, headers={'User-Agent': 'phos-watch-updater'})
    with urllib.request.urlopen(req, timeout=30) as response:
        total_size = int(response.headers.get('content-length', 0))
        chunk_size = 64 * 1024
        downloaded = 0
        
        with open(dest_path, 'wb') as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = int(downloaded * 100 / total_size)
                    with status_lock:
                        status["progress"] = min(99, percent)

def extract_zip_overwrite(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        namelist = zip_ref.namelist()
        if not namelist:
            return
            
        # GitHub zipballs pack all files under a root folder (e.g. owner-repo-hash/)
        root_dir = namelist[0].split('/')[0] + '/'
        
        for member in zip_ref.infolist():
            if member.filename == root_dir:
                continue
                
            if member.filename.startswith(root_dir):
                rel_path = member.filename[len(root_dir):]
                if not rel_path:
                    continue
                    
                # Protect config files and logs from overwrites
                if rel_path in ("config.yaml", "control.json", "queue.log") or rel_path.startswith("logs/"):
                    continue
                    
                if member.is_dir():
                    os.makedirs(rel_path, exist_ok=True)
                else:
                    parent = os.path.dirname(rel_path)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    with zip_ref.open(member) as source, open(rel_path, 'wb') as target:
                        target.write(source.read())

def run_update_sync(include_prerelease=False):
    global status
    if not update_lock.acquire(blocking=False):
        return {"error": "Update already in progress"}
        
    try:
        with status_lock:
            status["status"] = "downloading"
            status["progress"] = 0
            status["error_message"] = None
            
        # 1. Fetch release info to get URLs
        if include_prerelease:
            url = "https://api.github.com/repos/kenny1203520/phos-watch/releases"
        else:
            url = "https://api.github.com/repos/kenny1203520/phos-watch/releases/latest"
            
        req = urllib.request.Request(url, headers={'User-Agent': 'phos-watch-updater'})
        with urllib.request.urlopen(req, timeout=10) as response:
            res_data = json.loads(response.read().decode('utf-8'))
            
        if isinstance(res_data, list):
            if not res_data:
                raise Exception("No releases found.")
            data = res_data[0]
        else:
            data = res_data
            
        tag_name = data.get("tag_name", "v0.0.0")
        
        # 2. Update execution
        if getattr(sys, 'frozen', False):
            # Compiled PyInstaller exe
            assets = data.get("assets", [])
            download_url = None
            asset_name = None
            
            sys_os = platform.system().lower()
            if sys_os == "windows":
                for asset in assets:
                    if asset.get("name", "").endswith(".exe"):
                        download_url = asset.get("browser_download_url")
                        asset_name = asset.get("name")
                        break
            else:
                for asset in assets:
                    name = asset.get("name", "")
                    # Match binary name phos-watch, avoid .zip or .exe
                    if name == "phos-watch" or (name.startswith("phos-watch-") and not name.endswith(".exe") and not name.endswith(".zip") and not name.endswith(".tar.gz")):
                        download_url = asset.get("browser_download_url")
                        asset_name = asset.get("name")
                        break
                        
            if not download_url:
                raise Exception(f"No suitable binary found in release {tag_name} for OS: {platform.system()}")
                
            current_exe = sys.executable
            temp_exe = current_exe + ".tmp"
            
            logger.info(f"Downloading binary from {download_url} to {temp_exe}...")
            download_file_with_progress(download_url, temp_exe)
            
            with status_lock:
                status["status"] = "applying"
                status["progress"] = 100
                
            old_exe = current_exe + ".old"
            if os.path.exists(old_exe):
                try:
                    os.remove(old_exe)
                except Exception:
                    pass
                    
            logger.info(f"Replacing running executable {current_exe}...")
            os.rename(current_exe, old_exe)
            os.rename(temp_exe, current_exe)
            
            with status_lock:
                status["status"] = "done"
                
            logger.info("Restarting application executable...")
            subprocess.Popen([current_exe] + sys.argv[1:])
            os._exit(0)
            
        else:
            # Developer mode (python scripts)
            if os.path.isdir(".git"):
                with status_lock:
                    status["status"] = "applying"
                logger.info("Executing git pull to update source files...")
                res = subprocess.run(["git", "pull"], capture_output=True, text=True, check=False)
                if res.returncode == 0:
                    with status_lock:
                        status["status"] = "done"
                    logger.info("git pull succeeded, restarting python script...")
                    subprocess.Popen([sys.executable, "app.py"] + sys.argv[1:])
                    os._exit(0)
                else:
                    raise Exception(f"git pull failed with code {res.returncode}: {res.stderr}")
            else:
                zipball_url = data.get("zipball_url")
                if not zipball_url:
                    raise Exception("No zipball_url found in release data.")
                    
                temp_zip = "phos-watch-update.zip"
                logger.info(f"Downloading zipball from {zipball_url} to {temp_zip}...")
                download_file_with_progress(zipball_url, temp_zip)
                
                with status_lock:
                    status["status"] = "applying"
                    status["progress"] = 100
                    
                logger.info("Extracting files...")
                extract_zip_overwrite(temp_zip)
                
                try:
                    os.remove(temp_zip)
                except Exception:
                    pass
                    
                with status_lock:
                    status["status"] = "done"
                    
                logger.info("Update complete. Restarting python script...")
                subprocess.Popen([sys.executable, "app.py"] + sys.argv[1:])
                os._exit(0)
                
    except Exception as e:
        logger.exception("Updater failed")
        with status_lock:
            status["status"] = "error"
            status["error_message"] = str(e)
    finally:
        update_lock.release()

def background_run_update(include_prerelease=False):
    threading.Thread(target=run_update_sync, args=(include_prerelease,), daemon=True).start()

def should_run_periodic_check(cfg):
    freq = cfg.get('update_check_frequency', 'daily')
    interval = int(cfg.get('update_check_interval', 1) or 1)
    check_time_str = cfg.get('update_check_time', '02:00')
    
    state = control._read_file()
    last_check_str = state.get('last_update_check')
    
    now = datetime.datetime.now()
    
    if not last_check_str:
        return True
        
    try:
        last_check = datetime.datetime.fromisoformat(last_check_str)
    except Exception:
        return True
        
    delta_seconds = (now - last_check).total_seconds()
    
    if freq == 'hourly':
        return delta_seconds >= 3600 * interval
    elif freq == 'daily':
        return delta_seconds >= 86400 * interval
    elif freq == 'weekly':
        return delta_seconds >= 86400 * 7 * interval
    elif freq == 'custom_hours':
        return delta_seconds >= 3600 * interval
    elif freq == 'custom_days':
        return delta_seconds >= 86400 * interval
    elif freq == 'specific_time':
        # Check if checked today
        if last_check.date() == now.date():
            return False
        try:
            h, m = map(int, check_time_str.split(':'))
            target_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
            return now >= target_time
        except Exception:
            return False
            
    return False

def record_check_time():
    state = control._read_file()
    state['last_update_check'] = datetime.datetime.now().isoformat()
    control._write_file(state)

async def updater_scheduler_loop():
    # Wait for startup to settle
    await asyncio.sleep(5)
    
    from . import worker
    cfg = worker.load_config()
    
    if cfg.get('update_check_on_startup', True):
        logger.info("Startup update check triggered...")
        check_update_sync(include_prerelease=bool(cfg.get('update_include_prerelease', False)))
        record_check_time()
        
    while True:
        try:
            cfg = worker.load_config()
            freq = cfg.get('update_check_frequency', 'daily')
            if freq != 'none':
                if should_run_periodic_check(cfg):
                    logger.info("Scheduled update check triggered...")
                    check_update_sync(include_prerelease=bool(cfg.get('update_include_prerelease', False)))
                    record_check_time()
        except Exception as e:
            logger.exception("Error in scheduler loop")
            
        await asyncio.sleep(60)
