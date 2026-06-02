import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
import yaml

import phos_queue as q

logger = logging.getLogger(__name__)


def needs_processing(filepath: str, cfg: dict) -> bool:
    import worker
    import rules
    
    path_obj = Path(filepath)
    if path_obj.is_dir():
        return False
    if not worker._source_extension_allowed(filepath, cfg):
        return False
    
    target_format = str(cfg.get('target_format', 'jpg') or 'jpg').strip().lstrip('.') or 'jpg'
    out = rules.normalize_output_path(filepath, target_format)
    
    if filepath == out:
        return False
    return True


class _Handler(FileSystemEventHandler):
    def __init__(self, watch_root):
        super().__init__()
        self.watch_root = Path(watch_root)

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        
        import worker
        cfg = worker.load_config()
        if not needs_processing(str(path), cfg):
            return

        logger.info('Detected new file: %s', path)
        q.enqueue(str(path))

    def on_moved(self, event):
        if event.is_directory:
            return
        path = Path(event.dest_path)
        
        import worker
        cfg = worker.load_config()
        if not needs_processing(str(path), cfg):
            return

        logger.info('Detected moved file: %s', path)
        q.enqueue(str(path))


def _scan_and_enqueue(watch_path: str, recursive: bool, cfg: dict):
    try:
        p = Path(watch_path)
        if not p.exists():
            return
            
        pattern = '**/*' if recursive else '*'
        for filepath in p.glob(pattern):
            if needs_processing(str(filepath), cfg):
                logger.info('Startup scan: enqueuing %s', filepath)
                q.enqueue(str(filepath))
    except Exception:
        logger.exception('Failed during startup scan of %s', watch_path)


def start_watcher(paths):
    observer = Observer()
    for entry in paths:
        if isinstance(entry, dict):
            path = entry.get('path') or entry.get('watch_path')
            recursive = bool(entry.get('recursive', True))
        else:
            path = entry
            recursive = True

        if not path:
            continue
        handler = _Handler(path)
        observer.schedule(handler, path=path, recursive=recursive)
        logger.info('Watching path=%s recursive=%s', path, recursive)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def start_watcher_loop(config_path='config.yaml'):
    import control
    observer = None
    last_paths = None
    last_mtime = None

    while True:
        try:
            control.update_status('watcher', 'normal')
            mtime = os.path.getmtime(config_path) if os.path.exists(config_path) else None
            if mtime != last_mtime:
                # Reload config
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        cfg = yaml.safe_load(f) or {}
                except Exception:
                    cfg = {}
                
                # Extract paths
                paths = cfg.get('watch_paths', [{'path': './watched', 'recursive': True}])
                if isinstance(paths, list) and paths and isinstance(paths[0], str):
                    default_recursive = bool(cfg.get('recursive', True))
                    paths = [{'path': p, 'recursive': default_recursive} for p in paths]
                
                # Normalize paths for comparison
                normalized_paths = []
                for entry in paths:
                    if isinstance(entry, dict):
                        p_val = entry.get('path') or entry.get('watch_path')
                        r_val = bool(entry.get('recursive', True))
                    else:
                        p_val = entry
                        r_val = True
                    if p_val:
                        normalized_paths.append({'path': os.path.abspath(p_val), 'recursive': r_val})
                
                if normalized_paths != last_paths:
                    logger.info('Watch paths changed or initialized. Updating observer...')
                    if observer:
                        logger.info('Stopping old observer...')
                        try:
                            observer.stop()
                            observer.join()
                        except RuntimeError:
                            pass
                        observer = None
                    
                    if normalized_paths:
                        new_observer = Observer()
                        scheduled_count = 0
                        for entry in normalized_paths:
                            path = entry['path']
                            recursive = entry['recursive']
                            try:
                                if not os.path.exists(path):
                                    os.makedirs(path, exist_ok=True)
                                handler = _Handler(path)
                                new_observer.schedule(handler, path=path, recursive=recursive)
                                scheduled_count += 1
                                logger.info('Watching path=%s recursive=%s', path, recursive)
                            except Exception as pe:
                                logger.exception('Failed to setup watch path %s: %s', path, pe)
                        
                        if scheduled_count > 0:
                            try:
                                new_observer.start()
                                observer = new_observer
                                
                                # Perform initial scan of the directories
                                for entry in normalized_paths:
                                    try:
                                        if os.path.exists(entry['path']):
                                            logger.info('Performing initial scan of %s...', entry['path'])
                                            _scan_and_enqueue(entry['path'], entry['recursive'], cfg)
                                    except Exception:
                                        logger.exception('Failed during initial scan of %s', entry['path'])
                            except Exception as se:
                                logger.exception('Failed to start observer: %s', se)
                                try:
                                    new_observer.stop()
                                except Exception:
                                    pass
                                observer = None
                                raise se
                        else:
                            observer = None
                            logger.error('No watch paths could be scheduled.')
                    
                    last_paths = normalized_paths
                last_mtime = mtime
        except Exception as e:
            logger.exception('Error in watcher config reload loop')
            try:
                control.update_status('watcher', 'abnormal', error=str(e))
            except Exception:
                pass
        
        time.sleep(2)


if __name__ == '__main__':
    start_watcher(['./watched'])

