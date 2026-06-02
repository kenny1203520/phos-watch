import os
import time
import logging
import subprocess
import shutil
import phos_queue as q
import control
import rules
import yaml
import threading

logger = logging.getLogger(__name__)


def load_config(path='config.yaml'):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


# Cached config + mtime to support hot-reload without restarting the worker
_cached_cfg = None
_cached_mtime = None


def load_config_if_changed(path='config.yaml'):
    """Return cached config unless file mtime changed, in which case reload.
    If the config file does not exist, return empty dict and reset cache.
    """
    global _cached_cfg, _cached_mtime
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        # config missing: clear cache
        if _cached_cfg is None:
            _cached_cfg = {}
        _cached_mtime = None
        return _cached_cfg or {}

    if _cached_mtime is None or mtime != _cached_mtime:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                _cached_cfg = yaml.safe_load(f) or {}
            _cached_mtime = mtime
            logger.info('Reloaded config from %s', path)
        except Exception:
            logger.exception('Failed to reload config; keeping previous config')
    return _cached_cfg or {}


def start_config_watchdog(path='config.yaml'):
    """Start a watchdog observer to reload config on change if watchdog is available.
    This is optional; if watchdog isn't installed, function is a no-op.
    """
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except Exception:
        logger.debug('watchdog not available; using mtime polling for config reload')
        return None

    class _Handler(FileSystemEventHandler):
        def on_modified(self, event):
            try:
                src = os.path.abspath(event.src_path)
                target = os.path.abspath(path)
                if src == target:
                    global _cached_mtime, _cached_cfg
                    # Force reload on next check by clearing mtime, then attempt immediate reload
                    _cached_mtime = None
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            _cached_cfg = yaml.safe_load(f) or {}
                        _cached_mtime = os.path.getmtime(path)
                        logger.info('Config reloaded by watchdog')
                    except Exception:
                        logger.exception('watchdog failed to reload config')
            except Exception:
                logger.exception('watchdog handler error')

    observer = Observer()
    watch_dir = os.path.dirname(os.path.abspath(path)) or '.'
    try:
        observer.schedule(_Handler(), watch_dir, recursive=False)
        observer.daemon = True
        observer.start()
        logger.info('Started watchdog observer for %s', path)
        return observer
    except Exception:
        logger.exception('Failed to start watchdog observer')
        return None


def _find_imagemagick_command():
    from shutil import which
    if which('magick'):
        return 'magick'
    conv = which('convert')
    if conv:
        # On Windows, convert.exe in system32 is a partition converter, NOT ImageMagick
        if os.name == 'nt' and 'system32' in conv.lower():
            return None
        return 'convert'
    return None


def _normalize_ext(value: str) -> str:
    return str(value or '').strip().lower().lstrip('.')


def _build_extension_map(cfg):
    aliases = cfg.get('extension_aliases', {}) or {}
    ext_map = {}

    def register(ext: str, canonical: str):
        normalized_ext = _normalize_ext(ext)
        normalized_canonical = _normalize_ext(canonical)
        if normalized_ext and normalized_canonical:
            ext_map[normalized_ext] = normalized_canonical

    for canonical, alias_list in aliases.items():
        register(canonical, canonical)
        if isinstance(alias_list, list):
            for alias in alias_list:
                register(alias, canonical)

    return ext_map


def _resolve_extension(value: str, ext_map: dict) -> str:
    normalized = _normalize_ext(value)
    return ext_map.get(normalized, normalized)


def _should_rename_only(src: str, target_format: str, cfg) -> bool:
    src_ext = _normalize_ext(os.path.splitext(src)[1])
    target_ext = _normalize_ext(target_format)
    if not src_ext or not target_ext:
        return False

    ext_map = _build_extension_map(cfg)
    return _resolve_extension(src_ext, ext_map) == _resolve_extension(target_ext, ext_map)


def _rename_output_path(src: str, out: str):
    if os.path.normcase(src) == os.path.normcase(out) and src != out:
        base_dir = os.path.dirname(out)
        temp_name = f".{os.path.basename(out)}.phos-renaming-{os.getpid()}-{int(time.time() * 1000)}"
        temp_path = os.path.join(base_dir, temp_name)
        os.replace(src, temp_path)
        os.replace(temp_path, out)
        return
    os.replace(src, out)


def _source_extension_allowed(path: str, cfg) -> bool:
    allowed = cfg.get('source_extensions')
    aliases = cfg.get('extension_aliases', {}) or {}

    ext = _normalize_ext(os.path.splitext(path)[1])
    if not ext:
        return False

    normalized_allowed = set()
    if isinstance(allowed, list):
        normalized_allowed.update(_normalize_ext(item) for item in allowed)
    elif isinstance(allowed, str):
        normalized_allowed.update(_normalize_ext(item) for item in allowed.split(','))

    for canonical, alias_list in aliases.items():
        normalized_allowed.add(_normalize_ext(canonical))
        if isinstance(alias_list, list):
            normalized_allowed.update(_normalize_ext(item) for item in alias_list)

    if not normalized_allowed:
        return True

    return ext in normalized_allowed


def process_item(item, cfg):
    src = item.get('path')
    if not src:
        logger.warning('Empty item received: %s', item)
        return False

    if not _source_extension_allowed(src, cfg):
        logger.info('Skipping unsupported source extension: %s', src)
        return False

    target_format = str(cfg.get('target_format', 'jpg') or 'jpg').strip().lstrip('.') or 'jpg'
    out = rules.normalize_output_path(src, target_format)
    
    if src == out:
        logger.info('File %s is already in target format and case-normalized', src)
        return True

    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    max_retries = int(cfg.get('max_retries', 3))
    backoff = float(cfg.get('retry_backoff', 1.0))

    for attempt in range(1, max_retries + 1):
        try:
            if not os.path.exists(src):
                logger.warning('Source file does not exist: %s', src)
                return False

            if _should_rename_only(src, target_format, cfg):
                _rename_output_path(src, out)
                logger.info('Renamed %s -> %s', src, out)
                return True

            cmd_base = _find_imagemagick_command()
            success_magick = False
            if cmd_base:
                try:
                    cmd = [cmd_base, src, out]
                    subprocess.check_call(cmd)
                    logger.info('Converted %s -> %s', src, out)
                    success_magick = True
                except subprocess.CalledProcessError as e:
                    logger.warning('ImageMagick conversion failed for %s: %s. Trying Pillow fallback...', src, e)

            if not success_magick:
                # Pillow fallback
                from PIL import Image
                with Image.open(src) as im:
                    im = im.convert('RGB')
                    im.save(out, quality=cfg.get('image_quality', 85))
                logger.info('Pillow fallback converted %s -> %s', src, out)
            return True
        except Exception as e:
            logger.warning('Attempt %d: failed to process %s: %s', attempt, src, e)
            if attempt < max_retries:
                time.sleep(backoff * attempt)
            else:
                logger.exception('All attempts failed for %s', src)
                return False


def run_worker(poll_interval=1):
    # configure logging: console + file so web UI can tail the file
    logging.basicConfig(level=logging.INFO)
    log_file = os.getenv('PHOS_LOG_FILE', 'phos_watch.log')
    # avoid adding multiple handlers if run multiple times
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(log_file) for h in logging.getLogger().handlers):
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        fh.setFormatter(fmt)
        logging.getLogger().addHandler(fh)
    # try to start watchdog (optional)
    try:
        _observer = start_config_watchdog()
    except Exception:
        _observer = None

    while True:
        try:
            control.update_status('worker', 'normal')
            cfg = load_config_if_changed()
            if control.is_paused():
                logger.info('Worker paused; sleeping %s seconds', poll_interval)
                time.sleep(poll_interval)
                continue
            item = q.dequeue(timeout=5)
            if item:
                try:
                    success = process_item(item, cfg)
                    if success:
                        # handle original file per config
                        try:
                            src_path = item.get('path')
                            target_format = str(cfg.get('target_format', 'jpg') or 'jpg').strip().lstrip('.') or 'jpg'
                            out_path = rules.normalize_output_path(src_path, target_format)
                            
                            # Only delete/archive if it is a different file on disk
                            if os.path.normcase(src_path) != os.path.normcase(out_path):
                                if cfg.get('delete_original'):
                                    if os.path.exists(src_path):
                                        os.remove(src_path)
                                        logger.info('Deleted original file: %s', src_path)
                                elif cfg.get('archive_dir'):
                                    archive_dir = cfg.get('archive_dir')
                                    os.makedirs(archive_dir, exist_ok=True)
                                    basename = os.path.basename(src_path)
                                    if os.path.exists(src_path):
                                        shutil.move(src_path, os.path.join(archive_dir, basename))
                                        logger.info('Archived original file %s to %s', src_path, archive_dir)
                        except Exception:
                            logger.exception('Failed post-processing on %s', item.get('path'))
                except Exception:
                    logger.exception('Error processing item %s', item)
            else:
                time.sleep(poll_interval)
        except Exception as e:
            logger.exception('Error in worker main loop')
            try:
                control.update_status('worker', 'abnormal', error=str(e))
            except Exception:
                pass
            time.sleep(poll_interval)


if __name__ == '__main__':
    run_worker()
