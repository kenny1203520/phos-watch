import os
import json
import logging
import uuid

logger = logging.getLogger(__name__)

# Allow users to explicitly disable Redis via env var PHOS_USE_REDIS (0/false to disable)
REDIS_URL = os.getenv('REDIS_URL')
_use_redis_env = os.getenv('PHOS_USE_REDIS')
USE_REDIS = False if REDIS_URL is None else True
if _use_redis_env is not None and _use_redis_env.lower() in ('0', 'false', 'no'):
    USE_REDIS = False

r = None
if USE_REDIS and REDIS_URL:
    try:
        import redis
        r = redis.from_url(REDIS_URL)
    except Exception as e:
        logger.debug('Redis not available or failed to connect: %s', e)
        r = None


def enqueue(path: str):
    item_obj = {'id': uuid.uuid4().hex, 'path': path}
    item = json.dumps(item_obj)
    if r:
        try:
            r.lpush('phos:queue', item)
            return
        except Exception as e:
            logger.warning('Redis enqueue failed, falling back: %s', e)
    # fallback: write to local file
    qfile = 'queue.log'
    with open(qfile, 'a', encoding='utf-8') as f:
        f.write(item + '\n')


def dequeue(timeout=0):
    if r:
        try:
            res = r.brpop('phos:queue', timeout=timeout)
            if not res:
                return None
            _, raw = res
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')
            return json.loads(raw)
        except Exception as e:
            logger.warning('Redis dequeue failed: %s', e)
    # fallback: read file
    qfile = 'queue.log'
    try:
        with open(qfile, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines:
            return None
        first = lines[0].strip()
        remaining = lines[1:]
        with open(qfile, 'w', encoding='utf-8') as f:
            f.writelines(remaining)
        return json.loads(first)
    except FileNotFoundError:
        return None


def qlen():
    if r:
        try:
            return r.llen('phos:queue')
        except Exception:
            return 0
    try:
        with open('queue.log', 'r', encoding='utf-8') as f:
            return len(f.readlines())
    except Exception:
        return 0


def list_items():
    """
    Return queued items without removing them.
    Uses Redis when available, otherwise falls back to queue.log.
    Items are returned in FIFO order (oldest first).
    """
    items = []
    if r:
        try:
            raw_items = r.lrange('phos:queue', 0, -1)
            for raw in raw_items:
                try:
                    if isinstance(raw, bytes):
                        raw = raw.decode('utf-8')
                    items.append(json.loads(raw))
                except Exception:
                    logger.warning('Skipping malformed Redis queue item')
            items.reverse()
            return items
        except Exception as e:
            logger.debug('Redis list_items failed: %s', e)

    try:
        with open('queue.log', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception:
                    logger.warning('Skipping malformed queue.log line')
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.debug('Failed to read queue.log: %s', e)
    return items


def peek():
    """Return the oldest queued item without removing it."""
    if r:
        try:
            raw = r.lindex('phos:queue', -1)
            if raw is None:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')
            return json.loads(raw)
        except Exception as e:
            logger.debug('Redis peek failed: %s', e)
    try:
        with open('queue.log', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    return json.loads(line)
                except Exception:
                    continue
    except FileNotFoundError:
        return None
    return None


def remove(item_id: str):
    """Remove the first occurrence of an item with given id from queue."""
    if not item_id:
        return False
    if r:
        try:
            # find raw representation by scanning
            raw_items = r.lrange('phos:queue', 0, -1)
            target_raw = None
            for raw in raw_items:
                try:
                    if isinstance(raw, bytes):
                        s = raw.decode('utf-8')
                    else:
                        s = raw
                    obj = json.loads(s)
                    if obj.get('id') == item_id:
                        target_raw = s
                        break
                except Exception:
                    continue
            if target_raw:
                r.lrem('phos:queue', 1, target_raw)
                return True
            return False
        except Exception as e:
            logger.debug('Redis remove failed: %s', e)
    # file fallback
    qfile = 'queue.log'
    try:
        changed = False
        out_lines = []
        with open(qfile, 'r', encoding='utf-8') as f:
            for line in f:
                line_strip = line.strip()
                if not line_strip:
                    continue
                try:
                    obj = json.loads(line_strip)
                    if obj.get('id') == item_id and not changed:
                        changed = True
                        continue
                except Exception:
                    pass
                out_lines.append(line)
        if changed:
            with open(qfile, 'w', encoding='utf-8') as f:
                f.writelines(out_lines)
        return changed
    except FileNotFoundError:
        return False


def requeue(item_id: str):
    """Move the item with item_id to the front so it will be processed next.
    Returns True if requeued.
    """
    if not item_id:
        return False
    # find and remove the item first
    item_obj = None
    if r:
        try:
            raw_items = r.lrange('phos:queue', 0, -1)
            target_raw = None
            for raw in raw_items:
                try:
                    if isinstance(raw, bytes):
                        s = raw.decode('utf-8')
                    else:
                        s = raw
                    obj = json.loads(s)
                    if obj.get('id') == item_id:
                        target_raw = s
                        item_obj = obj
                        break
                except Exception:
                    continue
            if target_raw:
                # remove
                r.lrem('phos:queue', 1, target_raw)
                # push to right so it becomes the next popped (oldest)
                r.rpush('phos:queue', target_raw)
                return True
            return False
        except Exception as e:
            logger.debug('Redis requeue failed: %s', e)
    # file fallback
    qfile = 'queue.log'
    try:
        lines = []
        found = None
        with open(qfile, 'r', encoding='utf-8') as f:
            for line in f:
                line_strip = line.strip()
                if not line_strip:
                    continue
                try:
                    obj = json.loads(line_strip)
                    if obj.get('id') == item_id and found is None:
                        found = line_strip
                        continue
                except Exception:
                    pass
                lines.append(line)
        if found is None:
            return False
        # write: found as first line (oldest), then remaining
        with open(qfile, 'w', encoding='utf-8') as f:
            f.write(found + '\n')
            f.writelines(lines)
        return True
    except FileNotFoundError:
        return False
