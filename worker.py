import os
import time
import logging
import subprocess
import shutil
import phos_queue as q
import rules
import yaml

logger = logging.getLogger(__name__)


def load_config(path='config.yaml'):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _find_imagemagick_command():
    from shutil import which
    if which('magick'):
        return 'magick'
    if which('convert'):
        return 'convert'
    return None


def process_item(item, cfg):
    src = item.get('path')
    if not src:
        logger.warning('Empty item received: %s', item)
        return False

    target_format = cfg.get('target_format', 'jpg')
    out = rules.normalize_output_path(src, target_format)
    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    cmd_base = _find_imagemagick_command()
    if not cmd_base:
        logger.error('ImageMagick not found; cannot convert %s', src)
        return False

    cmd = [cmd_base, src, out] if cmd_base == 'convert' else [cmd_base, src, out]

    max_retries = int(cfg.get('max_retries', 2))
    backoff = float(cfg.get('retry_backoff', 1.0))

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
                # Try Pillow fallback if ImageMagick failed or not present
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
    logging.basicConfig(level=logging.INFO)
    while True:
        cfg = load_config()
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
