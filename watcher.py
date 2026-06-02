import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from queue import Empty

import phos_queue as q

logger = logging.getLogger(__name__)


class _Handler(FileSystemEventHandler):
    def __init__(self, watch_root):
        super().__init__()
        self.watch_root = Path(watch_root)

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        logger.info('Detected new file: %s', path)
        q.enqueue(str(path))


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


if __name__ == '__main__':
    start_watcher(['./watched'])
