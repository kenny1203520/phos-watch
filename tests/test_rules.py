import pytest
from rules import normalize_output_path


def test_normalize_output_basic():
    inp = 'watched/image.JPG'
    out = normalize_output_path(inp, 'jpg')
    assert out.endswith('image.jpg')


def test_normalize_output_diff_ext():
    inp = 'watched/subdir/photo.png'
    out = normalize_output_path(inp, 'webp')
    assert out.endswith('photo.webp')


def test_normalize_output_preserves_case_and_strips_dot():
    inp = 'watched/subdir/photo.jpeg'
    out = normalize_output_path(inp, '.JPG')
    assert out.endswith('photo.JPG')
