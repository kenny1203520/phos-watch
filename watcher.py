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
    for p in paths:
        handler = _Handler(p)
        observer.schedule(handler, path=p, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    start_watcher(['./watched'])
