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
    if which('convert'):
        return 'convert'
    return None


def _normalize_ext(value: str) -> str:
    return str(value or '').strip().lower().lstrip('.')


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

    target_format = cfg.get('target_format', 'jpg')
    out = rules.normalize_output_path(src, target_format)
    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    cmd_base = _find_imagemagick_command()
    # If ImageMagick is not available, we'll use Pillow fallback below.
    if cmd_base:
        cmd = [cmd_base, src, out] if cmd_base == 'convert' else [cmd_base, src, out]
    else:
        cmd = None

    max_retries = int(cfg.get('max_retries', 2))
    backoff = float(cfg.get('retry_backoff', 1.0))

    if cmd is not None:
        for attempt in range(1, max_retries + 1):
            try:
                subprocess.check_call(cmd)
                logger.info('Converted %s -> %s', src, out)
                return True
            except subprocess.CalledProcessError as e:
                logger.warning('Attempt %d: conversion failed for %s: %s', attempt, src, e)
                if attempt < max_retries:
                    time.sleep(backoff * attempt)
                else:
                    logger.exception('All attempts failed for %s', src)
                    break

    # Either ImageMagick not present, or all attempts failed — try Pillow fallback
    try:
        from PIL import Image
        im = Image.open(src)
        im = im.convert('RGB')
        im.save(out, quality=cfg.get('image_quality', 85))
        logger.info('Pillow fallback converted %s -> %s', src, out)
        return True
    except Exception as e2:
        logger.exception('Pillow fallback failed for %s: %s', src, e2)
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
                        if cfg.get('delete_original'):
                            os.remove(item.get('path'))
                        elif cfg.get('archive_dir'):
                            archive_dir = cfg.get('archive_dir')
                            os.makedirs(archive_dir, exist_ok=True)
                            basename = os.path.basename(item.get('path'))
                            shutil.move(item.get('path'), os.path.join(archive_dir, basename))
                    except Exception:
                        logger.exception('Failed post-processing on %s', item.get('path'))
            except Exception:
                logger.exception('Error processing item %s', item)
        else:
            time.sleep(poll_interval)


if __name__ == '__main__':
    run_worker()
