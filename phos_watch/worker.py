import os
import time
import logging
import subprocess
import shutil
from pathlib import Path
from . import phos_queue as q
from . import control
from . import rules
import yaml
import threading
import hashlib
from logging.handlers import BaseRotatingHandler

logger = logging.getLogger(__name__)


class PhosRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, filename, mode='a', max_bytes=0, max_lines=0, max_hours=0, backupCount=5, encoding='utf-8', delay=False):
        self.max_bytes = max_bytes
        self.max_lines = max_lines
        self.max_hours = max_hours
        self.backupCount = backupCount
        self.last_rotation_time = time.time()
        self.last_failed_rotation_time = 0.0
        self.line_count = 0
        log_dir = os.path.dirname(filename)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if os.path.exists(filename):
            self.last_rotation_time = os.path.getmtime(filename)
            self._recount_lines(filename)
        super().__init__(filename, mode, encoding, delay)

    def _recount_lines(self, filename):
        try:
            with open(filename, 'rb') as f:
                self.line_count = sum(1 for _ in f)
        except Exception:
            self.line_count = 0

    def shouldRollover(self, record):
        # If rotation failed recently, don't try again immediately to avoid blocking
        if getattr(self, 'last_failed_rotation_time', 0.0) > 0.0:
            if time.time() - self.last_failed_rotation_time < 5.0:
                return False

        if self.stream is None:
            self.stream = self._open()
        
        # 1. Size check
        if self.max_bytes > 0:
            try:
                self.stream.seek(0, 2)
                msg_len = len(self.format(record).encode(self.encoding or 'utf-8'))
                if self.stream.tell() + msg_len >= self.max_bytes:
                    return True
            except Exception:
                pass
        
        # 2. Line check
        if self.max_lines > 0:
            if self.line_count >= self.max_lines:
                return True
                
        # 3. Time check (hours)
        if self.max_hours > 0:
            current_time = time.time()
            if (current_time - self.last_rotation_time) >= (self.max_hours * 3600):
                return True
                
        return False

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        success = True
        try:
            if self.backupCount > 0:
                for i in range(self.backupCount - 1, 0, -1):
                    sfn = self.rotation_filename(f"{self.baseFilename}.{i}")
                    dfn = self.rotation_filename(f"{self.baseFilename}.{i+1}")
                    if os.path.exists(sfn):
                        if os.path.exists(dfn):
                            for _ in range(5):
                                try:
                                    os.remove(dfn)
                                    break
                                except PermissionError:
                                    time.sleep(0.1)
                        rename_ok = False
                        for _ in range(5):
                            try:
                                os.rename(sfn, dfn)
                                rename_ok = True
                                break
                            except PermissionError:
                                time.sleep(0.1)
                        if not rename_ok:
                            success = False

                dfn = self.rotation_filename(f"{self.baseFilename}.1")
                if os.path.exists(dfn):
                    for _ in range(5):
                        try:
                            os.remove(dfn)
                            break
                        except PermissionError:
                            time.sleep(0.1)

                if os.path.exists(self.baseFilename):
                    rename_ok = False
                    for _ in range(5):
                        try:
                            os.rename(self.baseFilename, dfn)
                            rename_ok = True
                            break
                        except PermissionError:
                            time.sleep(0.1)
                    if not rename_ok:
                        success = False
        except Exception as e:
            import sys
            sys.stderr.write(f"Error during log rollover: {e}\n")
            success = False

        if not success:
            self.last_failed_rotation_time = time.time()
        else:
            self.last_failed_rotation_time = 0.0

        self.last_rotation_time = time.time()
        self.line_count = 0
        if not self.delay:
            self.stream = self._open()

    def emit(self, record):
        try:
            if self.shouldRollover(record):
                self.doRollover()
            msg = self.format(record) + self.terminator
            super().emit(record)
            self.line_count += msg.count('\n')
        except Exception:
            self.handleError(record)


def setup_phos_logging(cfg=None):
    if cfg is None:
        cfg = load_config()
    log_file = os.getenv('PHOS_LOG_FILE', os.path.join('logs', 'phos_watch.log'))
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    max_lines = int(cfg.get('log_max_lines', 0))
    max_size_kb = float(cfg.get('log_max_size_kb', 0))
    max_bytes = int(max_size_kb * 1024)
    max_hours = float(cfg.get('log_max_hours', 0))
    backup_count = int(cfg.get('log_backup_count', 5))

    root_logger = logging.getLogger()
    # Check if already added
    handler = None
    for h in root_logger.handlers:
        if isinstance(h, PhosRotatingFileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(log_file):
            handler = h
            break
            
    if handler is None:
        handler = PhosRotatingFileHandler(
            log_file,
            max_bytes=max_bytes,
            max_lines=max_lines,
            max_hours=max_hours,
            backupCount=backup_count,
            encoding='utf-8'
        )
        handler.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        handler.setFormatter(fmt)
        root_logger.addHandler(handler)
    else:
        # Update existing
        handler.max_lines = max_lines
        handler.max_bytes = max_bytes
        handler.max_hours = max_hours
        handler.backupCount = backup_count


# def file_md5(path):
#     h = hashlib.md5()
#     try:
#         with open(path, 'rb') as f:
#             for chunk in iter(lambda: f.read(8192), b''):
#                 h.update(chunk)
#         return h.hexdigest()
#     except Exception:
#         return None

def file_sha256(path):
    h = hashlib.sha256()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def is_same_file_content(path1, path2):
    if not os.path.exists(path1) or not os.path.exists(path2):
        return False
    try:
        if os.path.getsize(path1) != os.path.getsize(path2):
            return False
        return file_sha256(path1) == file_sha256(path2)
    except Exception:
        return False


def get_unique_output_path(src: str, out: str, is_rename_only: bool) -> str:
    if not os.path.exists(out):
        return out
    
    if is_rename_only:
        if is_same_file_content(src, out):
            return out
    else:
        # For conversion, if output exists and is newer than or equal to source,
        # it is considered already converted.
        if os.path.getmtime(out) >= os.path.getmtime(src):
            return out
            
    # Generate unique suffix _n
    base_dir = os.path.dirname(out)
    from pathlib import Path
    p = Path(out)
    stem = p.stem
    suffix = p.suffix
    
    n = 1
    while True:
        candidate = os.path.join(base_dir, f"{stem}_{n}{suffix}")
        if not os.path.exists(candidate):
            return candidate
        if is_rename_only:
            if is_same_file_content(src, candidate):
                return candidate
        else:
            if os.path.getmtime(candidate) >= os.path.getmtime(src):
                return candidate
        n += 1


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


def find_matching_scheme(src_path: str, cfg: dict):
    if not cfg.get('enable_conversion_schemes', True):
        return None

    schemes = cfg.get('conversion_schemes', []) or []
    src_ext = _normalize_ext(os.path.splitext(src_path)[1])
    if not src_ext:
        return None

    # Build alias map if aliases are enabled
    ext_map = {}
    if cfg.get('enable_extension_aliases', True):
        ext_map = _build_extension_map(cfg)

    resolved_src_ext = _resolve_extension(src_ext, ext_map)

    if not schemes:
        # fallback to legacy config structure if no schemes list is present
        legacy_src = cfg.get('source_extensions')
        if legacy_src is None:
            sc_srcs = [resolved_src_ext]
        elif isinstance(legacy_src, list):
            sc_srcs = legacy_src
            if not sc_srcs:
                sc_srcs = [resolved_src_ext]
        elif isinstance(legacy_src, str):
            sc_srcs = [x.strip() for x in legacy_src.split(',') if x.strip()]
            if not sc_srcs:
                sc_srcs = [resolved_src_ext]
        else:
            sc_srcs = [resolved_src_ext]

        schemes = [{
            'name': 'legacy-fallback',
            'source_extensions': sc_srcs,
            'target_format': cfg.get('target_format', 'jpg'),
            'delete_original': cfg.get('delete_original', False),
            'enabled': True
        }]

    for sc in schemes:
        if not sc.get('enabled', True):
            continue

        sc_allowed = sc.get('source_extensions', []) or []
        if isinstance(sc_allowed, str):
            sc_allowed = [x.strip() for x in sc_allowed.split(',') if x.strip()]

        resolved_allowed = set()
        for item in sc_allowed:
            resolved_allowed.add(_resolve_extension(_normalize_ext(item), ext_map))

        if resolved_src_ext in resolved_allowed:
            return sc

    return None


def _source_extension_allowed(path: str, cfg) -> bool:
    sc = find_matching_scheme(path, cfg)
    return sc is not None


def _cleanup_original(src: str, out: str, delete_original: bool, cfg: dict, ext_map: dict):
    if os.path.normcase(src) != os.path.normcase(out):
        if delete_original:
            if os.path.exists(src):
                os.remove(src)
                logger.info('Deleted original file: %s', src)
        elif cfg.get('archive_dir'):
            archive_dir = cfg.get('archive_dir')
            os.makedirs(archive_dir, exist_ok=True)
            basename = os.path.basename(src)
            if os.path.exists(src):
                shutil.move(src, os.path.join(archive_dir, basename))
                logger.info('Archived original file %s to %s', src, archive_dir)
        else:
            enable_aliases = cfg.get('enable_extension_aliases', True)
            if enable_aliases and os.path.exists(src):
                src_ext = _normalize_ext(os.path.splitext(src)[1])
                canonical_ext = _resolve_extension(src_ext, ext_map)
                if canonical_ext:
                    src_dir = os.path.dirname(src)
                    src_stem = Path(src).stem
                    normalized_src_path = os.path.join(src_dir, f"{src_stem}.{canonical_ext}")
                    if src != normalized_src_path:
                        _rename_output_path(src, normalized_src_path)
                        logger.info('Normalized original file extension: %s -> %s', src, normalized_src_path)


def process_item(item, cfg):
    src = item.get('path')
    if not src:
        logger.warning('Empty item received: %s', item)
        return False

    if not os.path.exists(src):
        logger.warning('Source file does not exist: %s', src)
        return False

    enable_conversion = cfg.get('enable_conversion_schemes', True)
    enable_aliases = cfg.get('enable_extension_aliases', True)
    ext_map = _build_extension_map(cfg) if enable_aliases else {}

    matched_scheme = find_matching_scheme(src, cfg)

    if matched_scheme:
        target_format = str(matched_scheme.get('target_format', 'jpg') or 'jpg').strip().lstrip('.') or 'jpg'
        delete_original = bool(matched_scheme.get('delete_original', False))

        out = rules.normalize_output_path(src, target_format)

        src_ext = _normalize_ext(os.path.splitext(src)[1])
        if enable_aliases:
            is_rename = (_resolve_extension(src_ext, ext_map) == _resolve_extension(target_format, ext_map))
        else:
            is_rename = (src_ext == _normalize_ext(target_format))

        unique_out = get_unique_output_path(src, out, is_rename)

        if src == unique_out:
            logger.info('File %s is already in target format and case-normalized', src)
            return True

        if os.path.exists(unique_out):
            if is_rename:
                if os.path.normcase(src) != os.path.normcase(unique_out) and is_same_file_content(src, unique_out):
                    logger.info('File %s is already renamed/present as %s', src, unique_out)
                    _cleanup_original(src, unique_out, delete_original, cfg, ext_map)
                    return True
            else:
                if os.path.getmtime(unique_out) >= os.path.getmtime(src):
                    logger.info('File %s is already converted/present as %s', src, unique_out)
                    _cleanup_original(src, unique_out, delete_original, cfg, ext_map)
                    return True

        out = unique_out
        out_dir = os.path.dirname(out)
        os.makedirs(out_dir, exist_ok=True)

        max_retries = int(cfg.get('max_retries', 3))
        backoff = float(cfg.get('retry_backoff', 1.0))

        success = False
        for attempt in range(1, max_retries + 1):
            try:
                if not os.path.exists(src):
                    logger.warning('Source file does not exist during processing: %s', src)
                    return False

                if is_rename:
                    _rename_output_path(src, out)
                    logger.info('Renamed %s -> %s', src, out)
                    success = True
                    break

                cmd_base = _find_imagemagick_command()
                success_magick = False
                if cmd_base:
                    try:
                        cmd = [cmd_base, src, '-auto-orient', out]
                        subprocess.check_call(cmd)
                        logger.info('Converted %s -> %s', src, out)
                        success_magick = True
                    except subprocess.CalledProcessError as e:
                        logger.warning('ImageMagick conversion failed for %s: %s. Trying Pillow fallback...', src, e)

                if not success_magick:
                    from PIL import Image, ImageOps
                    with Image.open(src) as im:
                        if hasattr(im, 'getexif'):
                            try:
                                im = ImageOps.exif_transpose(im)
                            except Exception:
                                pass
                        tgt_lower = target_format.lower()
                        if tgt_lower in ('jpg', 'jpeg'):
                            im = im.convert('RGB')
                        elif tgt_lower == 'bmp':
                            im = im.convert('RGB')
                        im.save(out, quality=cfg.get('image_quality', 85))
                    logger.info('Pillow fallback converted %s -> %s', src, out)

                success = True
                break
            except Exception as e:
                logger.warning('Attempt %d: failed to process %s: %s', attempt, src, e)
                if attempt < max_retries:
                    time.sleep(backoff * attempt)
                else:
                    logger.exception('All attempts failed for %s', src)
                    return False

        if success:
            _cleanup_original(src, out, delete_original, cfg, ext_map)
            return True

    elif enable_aliases:
        src_ext = _normalize_ext(os.path.splitext(src)[1])
        canonical_ext = _resolve_extension(src_ext, ext_map)
        if canonical_ext:
            src_dir = os.path.dirname(src)
            src_stem = Path(src).stem
            normalized_src_path = os.path.join(src_dir, f"{src_stem}.{canonical_ext}")
            if src != normalized_src_path:
                _rename_output_path(src, normalized_src_path)
                logger.info('Normalized file extension (no scheme matched): %s -> %s', src, normalized_src_path)
        return True

    return False


def _handle_failed_item(item, cfg):
    if not isinstance(item, dict) or not item.get('path'):
        return
    path = item.get('path')
    retry_count = item.get('retry_count', 0)
    max_queue_retries = int(cfg.get('max_queue_retries', 5))
    if retry_count < max_queue_retries:
        logger.warning('Item processing failed, re-enqueuing to the end (retry %d/%d): %s', 
                       retry_count + 1, max_queue_retries, path)
        q.enqueue(path, retry_count=retry_count + 1)
    else:
        logger.error('Max queue retries (%d) reached, discarding item: %s', max_queue_retries, path)


def run_worker(poll_interval=1):
    # configure logging: console + file so web UI can tail the file
    logging.basicConfig(level=logging.INFO)
    cfg = load_config()
    setup_phos_logging(cfg)
    # try to start watchdog (optional)
    try:
        _observer = start_config_watchdog()
    except Exception:
        _observer = None

    while True:
        try:
            control.update_status('worker', 'normal')
            cfg = load_config_if_changed()
            setup_phos_logging(cfg)
            if control.is_paused():
                logger.info('Worker paused; sleeping %s seconds', poll_interval)
                time.sleep(poll_interval)
                continue
            item = q.dequeue(timeout=5)
            if item:
                try:
                    success = process_item(item, cfg)
                    if success:
                        logger.info('Successfully processed item: %s', item.get('path'))
                    else:
                        _handle_failed_item(item, cfg)
                except Exception:
                    logger.exception('Error processing item %s', item)
                    _handle_failed_item(item, cfg)
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
