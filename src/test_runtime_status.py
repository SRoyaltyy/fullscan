"""Shape tests for Claw runtime doors (no live OpenClaw)."""
from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    path = Path(__file__).resolve().parent.parent / "scripts" / "runtime_status.py"
    spec = importlib.util.spec_from_file_location("runtime_status", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_door_payload_shape():
    mod = _load()
    d = mod.door("pong", "PONG", "classroom", "FAIL", True, "port down", "heal")
    assert d["id"] == "pong"
    assert d["action"] == "heal"
    assert d["required"] is True


def test_oauth_action_is_reauth():
    mod = _load()
    d = mod.door("oauth", "xAI auth", "auth", "FAIL", True, "expired", "reauth")
    assert d["action"] == "reauth"
