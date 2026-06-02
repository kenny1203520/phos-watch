from pathlib import Path


def normalize_output_path(input_path: str, target_format: str = 'jpg') -> str:
    p = Path(input_path)
    stem = p.stem
    suffix = str(target_format or 'jpg').strip().lstrip('.') or 'jpg'
    out = p.with_name(f"{stem}.{suffix}")
    return str(out)
