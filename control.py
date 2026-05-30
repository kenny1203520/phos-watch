import json
import logging
import os

import phos_queue as pq

logger = logging.getLogger(__name__)
CONTROL_FILE = 'control.json'


def _read_file():
    try:
        with open(CONTROL_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _write_file(data):
    tmp_file = CONTROL_FILE + '.tmp'
    try:
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        os.replace(tmp_file, CONTROL_FILE)
    except Exception:
        logger.exception('Failed writing control file')


def is_paused():
    redis_client = getattr(pq, 'r', None)
    if redis_client:
        try:
            value = redis_client.get('phos:paused')
            if value is None:
                return False
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            return str(value).lower() in ('1', 'true', 'yes', 'on')
        except Exception:
            logger.debug('Redis read for paused failed', exc_info=True)
    return bool(_read_file().get('paused', False))


def set_paused(state: bool):
    redis_client = getattr(pq, 'r', None)
    if redis_client:
        try:
            redis_client.set('phos:paused', '1' if state else '0')
        except Exception:
            logger.exception('Redis set paused failed')
    current = _read_file()
    current['paused'] = bool(state)
    _write_file(current)


def get_state():
    return {'paused': is_paused()}