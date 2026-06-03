import pytest
from unittest.mock import patch, MagicMock
from phos_watch.updater import parse_version, check_update_sync

def test_parse_version():
    # basic comparisons
    assert parse_version("0.1.0") == (0, 1, 0, 4, 0)
    assert parse_version("v0.1.0") == (0, 1, 0, 4, 0)
    
    # pre-release label comparison
    assert parse_version("0.1.0-alpha.1") == (0, 1, 0, 1, 1)
    assert parse_version("0.1.0-beta.2") == (0, 1, 0, 2, 2)
    assert parse_version("0.1.0-rc.3") == (0, 1, 0, 3, 3)
    
    # sorting / comparisons
    assert parse_version("0.1.0-alpha.1") < parse_version("0.1.0-beta.1")
    assert parse_version("0.1.0-beta.2") > parse_version("0.1.0-beta.1")
    assert parse_version("0.1.0-rc.1") < parse_version("0.1.0")
    assert parse_version("0.1.0") < parse_version("0.1.1-alpha.1")
    
    # complex versions
    assert parse_version("10.2.33-beta.14") == (10, 2, 33, 2, 14)


@patch("phos_watch.updater.__version__", "0.1.0")
@patch("urllib.request.urlopen")
def test_check_update_no_update(mock_urlopen):
    # Mock return data
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"tag_name": "v0.1.0", "body": "Release 0.1.0"}'
    mock_urlopen.return_value.__enter__.return_value = mock_response
    
    res = check_update_sync(include_prerelease=False)
    assert res["status"] == "idle"
    assert res["latest_version"] == "v0.1.0"
    assert res["update_available"] is False


@patch("phos_watch.updater.__version__", "0.1.0")
@patch("urllib.request.urlopen")
def test_check_update_has_update(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"tag_name": "v9.9.9", "body": "Huge Release"}'
    mock_urlopen.return_value.__enter__.return_value = mock_response
    
    res = check_update_sync(include_prerelease=False)
    assert res["status"] == "idle"
    assert res["latest_version"] == "v9.9.9"
    assert res["update_available"] is True


@patch("phos_watch.updater.__version__", "0.1.0")
@patch("urllib.request.urlopen")
def test_check_update_include_prerelease(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = b'[{"tag_name": "v9.9.9-beta.1", "body": "Pre Release"}]'
    mock_urlopen.return_value.__enter__.return_value = mock_response
    
    res = check_update_sync(include_prerelease=True)
    assert res["status"] == "idle"
    assert res["latest_version"] == "v9.9.9-beta.1"
    assert res["update_available"] is True


@patch("phos_watch.updater.__version__", "0.1.0")
@patch("urllib.request.urlopen")
def test_check_update_404_stable(mock_urlopen):
    import urllib.error
    mock_urlopen.side_effect = urllib.error.HTTPError("http://api.github.com/...", 404, "Not Found", {}, None)
    
    res = check_update_sync(include_prerelease=False)
    assert res["status"] == "idle"
    assert res["latest_version"] is None
    assert res["update_available"] is False
    assert res["error_message"] is None


@patch("phos_watch.updater.__version__", "0.1.0")
@patch("urllib.request.urlopen")
def test_check_update_404_prerelease(mock_urlopen):
    import urllib.error
    mock_urlopen.side_effect = urllib.error.HTTPError("http://api.github.com/...", 404, "Not Found", {}, None)
    
    res = check_update_sync(include_prerelease=True)
    assert res["status"] == "error"
    assert "HTTP 404" in res["error_message"]
