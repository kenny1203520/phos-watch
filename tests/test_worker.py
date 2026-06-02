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


def test_process_item_renames_same_format_only(tmp_path, monkeypatch):
    src = tmp_path / 'photo.JPG'
    src.write_bytes(b'not-an-image-but-rename-only-should-not-open-it')

    called = {'opened': False}

    def fail_open(*args, **kwargs):
        called['opened'] = True
        raise AssertionError('Image backend should not be used for rename-only normalization')

    monkeypatch.setattr(worker, '_find_imagemagick_command', lambda: None)
    monkeypatch.setattr(worker, 'os', worker.os)
    monkeypatch.setattr(worker, 'shutil', worker.shutil)
    monkeypatch.setattr(worker, 'time', worker.time)
    monkeypatch.setattr(worker, 'subprocess', worker.subprocess)
    monkeypatch.setattr(worker, '_should_rename_only', lambda src_path, target_format, cfg: True)

    import PIL.Image
    monkeypatch.setattr(PIL.Image, 'open', fail_open)

    cfg = {'target_format': 'jpg', 'extension_aliases': {'jpg': ['jpg', 'jpeg', 'JPG', 'JPEG']}}
    item = {'path': str(src)}

    success = worker.process_item(item, cfg)

    assert success is True
    assert any(p.name == 'photo.jpg' for p in tmp_path.iterdir())
    assert called['opened'] is False
