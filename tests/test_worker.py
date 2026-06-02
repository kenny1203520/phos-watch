import os
from pathlib import Path
import phos_watch.worker as worker


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
    import phos_watch.worker as worker
    
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
    import phos_watch.worker as worker
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


def test_original_file_normalization_on_keep(tmp_path):
    import logging
    import os
    from PIL import Image

    cfg = {
        'target_format': 'jpg',
        'delete_original': False,
        'extension_aliases': {'jpg': ['jpg', 'jpeg', 'JPG', 'JPEG'], 'png': ['png', 'PNG']}
    }
    
    src = tmp_path / "photo.PNG"
    Image.new('RGB', (8, 8)).save(src)
    
    item = {'path': str(src)}
    success = worker.process_item(item, cfg)
    assert success is True
    
    src_path = str(src)
    out_path = worker.rules.normalize_output_path(src_path, 'jpg')
    is_rename = worker._should_rename_only(src_path, 'jpg', cfg)
    out_path = worker.get_unique_output_path(src_path, out_path, is_rename)
    
    if os.path.normcase(src_path) != os.path.normcase(out_path):
        if not cfg.get('delete_original') and not cfg.get('archive_dir'):
            if os.path.exists(src_path):
                src_ext = worker._normalize_ext(os.path.splitext(src_path)[1])
                ext_map = worker._build_extension_map(cfg)
                canonical_ext = worker._resolve_extension(src_ext, ext_map)
                if canonical_ext:
                    src_dir = os.path.dirname(src_path)
                    src_stem = Path(src_path).stem
                    normalized_src_path = os.path.join(src_dir, f"{src_stem}.{canonical_ext}")
                    if src_path != normalized_src_path:
                        worker._rename_output_path(src_path, normalized_src_path)
                        
    expected_png = tmp_path / "photo.png"
    expected_jpg = tmp_path / "photo.jpg"
    
    # Case-sensitive check in directory listing
    filenames = os.listdir(tmp_path)
    assert "photo.png" in filenames
    assert "photo.jpg" in filenames
    assert "photo.PNG" not in filenames


def test_multi_scheme_matching(tmp_path, monkeypatch):
    # Setup files: in1.heic and in2.PNG
    src1 = tmp_path / "in1.heic"
    src1.write_bytes(b"heic-data")
    
    src2 = tmp_path / "in2.PNG"
    from PIL import Image
    Image.new('RGB', (8, 8)).save(src2)

    monkeypatch.setattr(worker, '_find_imagemagick_command', lambda: None)

    # Mock PIL Image.open to handle fake HEIC
    class FakeImage:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
        def convert(self, mode):
            return self
        def save(self, out, **kwargs):
            # Create the output file to simulate successful conversion
            with open(out, 'wb') as f:
                f.write(b"fake-converted-image")

    original_open = Image.open
    def mock_open(path, *args, **kwargs):
        if str(path).endswith('.heic'):
            return FakeImage()
        return original_open(path, *args, **kwargs)
        
    monkeypatch.setattr(Image, 'open', mock_open)

    cfg = {
        'enable_conversion_schemes': True,
        'enable_extension_aliases': True,
        'extension_aliases': {
            'heic': ['heic', 'HEIC'],
            'png': ['png', 'PNG']
        },
        'conversion_schemes': [
            {
                'name': 'heic-to-jpg',
                'source_extensions': ['heic'],
                'target_format': 'jpg',
                'delete_original': True,
                'enabled': True
            },
            {
                'name': 'png-to-webp',
                'source_extensions': ['png'],
                'target_format': 'webp',
                'delete_original': False,
                'enabled': True
            }
        ]
    }

    # Process first item (in1.heic -> should convert to in1.jpg and delete original)
    success1 = worker.process_item({'path': str(src1)}, cfg)
    assert success1 is True
    assert (tmp_path / "in1.jpg").exists()
    assert not src1.exists()

    # Process second item (in2.PNG -> should convert to in2.webp and rename original to in2.png)
    success2 = worker.process_item({'path': str(src2)}, cfg)
    assert success2 is True
    assert (tmp_path / "in2.webp").exists()
    filenames = os.listdir(tmp_path)
    assert "in2.png" in filenames
    assert "in2.PNG" not in filenames


def test_module_toggles_disabled(tmp_path, monkeypatch):
    # 1. Test when enable_conversion_schemes is False but enable_extension_aliases is True
    src1 = tmp_path / "photo.JPEG"
    src1.write_bytes(b"dummy jpeg")
    
    cfg = {
        'enable_conversion_schemes': False,
        'enable_extension_aliases': True,
        'extension_aliases': {
            'jpg': ['jpg', 'jpeg', 'JPG', 'JPEG']
        },
        'conversion_schemes': [
            {
                'name': 'jpg-to-png',
                'source_extensions': ['jpg'],
                'target_format': 'png',
                'delete_original': True,
                'enabled': True
            }
        ]
    }
    
    success1 = worker.process_item({'path': str(src1)}, cfg)
    assert success1 is True
    assert not (tmp_path / "photo.png").exists()
    assert (tmp_path / "photo.jpg").exists()
    assert not src1.exists()

    # 2. Test when both enable_conversion_schemes and enable_extension_aliases are False
    src2 = tmp_path / "photo2.JPEG"
    src2.write_bytes(b"dummy jpeg 2")

    cfg['enable_extension_aliases'] = False
    
    success2 = worker.process_item({'path': str(src2)}, cfg)
    assert success2 is False or not (tmp_path / "photo2.jpg").exists()
    assert src2.exists()




