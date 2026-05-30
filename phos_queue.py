import os
import json
import logging

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


try:
    import redis
    r = redis.from_url(REDIS_URL)
except Exception:
    r = None


def enqueue(path: str):
    item = json.dumps({'path': path})
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
