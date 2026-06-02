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


def test_run_worker_does_not_delete_on_case_normalization(tmp_path, monkeypatch):
    import worker
    
    src = tmp_path / "photo.JPG"
    src.write_bytes(b"dummy image data")
    
    cfg = {
        'target_format': 'jpg',
        'delete_original': True,
        'extension_aliases': {'jpg': ['jpg', 'jpeg', 'JPG', 'JPEG']}
    }
    
    item = {'path': str(src)}
    success = worker.process_item(item, cfg)
    assert success is True
    
    out_path = tmp_path / "photo.jpg"
    assert out_path.exists()
    
    src_path = item.get('path')
    out_path_str = worker.rules.normalize_output_path(src_path, 'jpg')
    
    import os
    if os.path.normcase(src_path) != os.path.normcase(out_path_str):
        if cfg.get('delete_original'):
            if os.path.exists(src_path):
                os.remove(src_path)
                
    assert out_path.exists()


def test_run_worker_deletes_original_on_different_format(tmp_path, monkeypatch):
    import worker
    src = tmp_path / "photo.png"
    
    from PIL import Image
    Image.new('RGB', (8, 8)).save(src)
    
    cfg = {
        'target_format': 'jpg',
        'delete_original': True,
    }
    
    item = {'path': str(src)}
    monkeypatch.setattr(worker, '_find_imagemagick_command', lambda: None)
    
    success = worker.process_item(item, cfg)
    assert success is True
    
    out_path = tmp_path / "photo.jpg"
    assert out_path.exists()
    
    src_path = item.get('path')
    out_path_str = str(out_path)
    
    import os
    assert os.path.normcase(src_path) != os.path.normcase(out_path_str)
    
    if cfg.get('delete_original'):
        if os.path.exists(src_path):
            os.remove(src_path)
            
    assert not os.path.exists(src_path)
    assert out_path.exists()


def test_get_unique_output_path_collision(tmp_path):
    import logging
    import os
    import time
    src = tmp_path / "image.heic"
    src.write_bytes(b"heic-data")
    
    out = tmp_path / "image.jpg"
    out.write_bytes(b"jpg-original-content")
    os.utime(str(out), (time.time() - 10, time.time() - 10))
    
    # First candidate collision
    res1 = worker.get_unique_output_path(str(src), str(out), is_rename_only=False)
    assert Path(res1).name == "image_1.jpg"
    
    # Write to image_1.jpg to create a second collision
    Path(res1).write_bytes(b"jpg-second-content")
    os.utime(res1, (time.time() - 10, time.time() - 10))
    res2 = worker.get_unique_output_path(str(src), str(out), is_rename_only=False)
    assert Path(res2).name == "image_2.jpg"


def test_phos_log_rotation_by_lines(tmp_path):
    import logging
    log_file = tmp_path / "test_rotate.log"
    # Create custom rotating handler with limit 3 lines
    handler = worker.PhosRotatingFileHandler(str(log_file), max_lines=3, backupCount=2)
    
    logger_test = logging.getLogger("test_rot_lines")
    logger_test.setLevel(logging.INFO)
    logger_test.addHandler(handler)
    
    logger_test.info("line 1")
    logger_test.info("line 2")
    logger_test.info("line 3")
    
    assert log_file.exists()
    # Trigger rotation on 4th write
    logger_test.info("line 4")
    
    # Check that rotation occurred
    rotated_file = tmp_path / "test_rotate.log.1"
    assert rotated_file.exists()
    
    # Clean up handler
    logger_test.removeHandler(handler)
    handler.close()


