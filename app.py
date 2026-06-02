import argparse
import logging
import uvicorn

from watcher import start_watcher
from worker import run_worker

def main():
    parser = argparse.ArgumentParser(description='phos-watch entrypoint')
    parser.add_argument('--mode', choices=['watch','worker','web'], default='web')
    parser.add_argument('--config', default='config.yaml')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.mode == 'watch':
        # read watch paths from config
        import yaml
        cfg = {}
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        except Exception:
            pass
        paths = cfg.get('watch_paths', [{'path': './watched', 'recursive': True}])
        if isinstance(paths, list) and paths and isinstance(paths[0], str):
            # Legacy config compatibility: convert to object form with global recursive flag.
            default_recursive = bool(cfg.get('recursive', True))
            paths = [{'path': p, 'recursive': default_recursive} for p in paths]
        start_watcher(paths)
    elif args.mode == 'worker':
        run_worker()
    else:
        # run FastAPI app
        uvicorn.run('web.api:app', host='0.0.0.0', port=8000, reload=False)


if __name__ == '__main__':
    main()
