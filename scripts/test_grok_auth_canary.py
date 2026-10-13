import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from grok_auth_canary import classify


def test_ok():
    assert classify(200, "42", 300) == "OK"


def test_device_login_text_even_on_200():
    assert classify(200, "Error: device login needed for xai", 300) == "DEVICE_LOGIN"


def test_http_401():
    assert classify(401, "", None) == "DEVICE_LOGIN"


def test_expired_token_no_answer():
    assert classify(None, "TimeoutError", -5) == "DEVICE_LOGIN"


def test_timeout_other():
    assert classify(None, "TimeoutError: timed out", 200) == "FAIL"
