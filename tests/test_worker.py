import os
from pathlib import Path
import worker


def test_pillow_fallback(tmp_path, monkeypatch):
    # create a small PNG file
    src = tmp_path / "in.png"
    from PIL import Image
    Image.new('RGB', (16, 16), (10, 20, 30)).save(src)

    # ensure ImageMagick is not used
    monkeypatch.setattr(worker, '_find_imagemagick_command', lambda: None)

    cfg = {'target_format': 'jpg', 'image_quality': 80}
    item = {'path': str(src)}

    out_path = Path(worker.rules.normalize_output_path(str(src), 'jpg'))
    # ensure output dir
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        success = worker.process_item(item, cfg)
        assert success is True
        assert out_path.exists()
    finally:
        # cleanup
        if out_path.exists():
            out_path.unlink()
