"""Shape + verdict tests for Claw runtime doors (no live OpenClaw)."""
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


def test_64_char_token_is_fail():
    mod = _load()
    st, req, det, act = mod.token_verdict(0, 64)
    assert st == "FAIL" and req and act == "heal"
    assert "64" in det
    st, req, det, act = mod.token_verdict(64, 64)
    assert st == "FAIL"
    st, req, det, act = mod.token_verdict(48, 64)
    assert st == "WARN" and "401" in det
    st, req, det, act = mod.token_verdict(48, 0)
    assert st == "OK"


def test_1800_timeout_is_fail():
    mod = _load()
    st, req, det, act = mod.timeout_verdict({
        "agents.defaults.timeoutSeconds": 1800,
        "subagents.runTimeoutSeconds": 1800,
        "models.providers.xai.timeoutSeconds": 1800,
    })
    assert st == "FAIL" and req and act == "heal"
    assert "1800" in det
    st, req, det, act = mod.timeout_verdict({
        "agents.defaults.timeoutSeconds": 10800,
        "subagents.runTimeoutSeconds": 10800,
        "models.providers.xai.timeoutSeconds": 10800,
    })
    assert st == "OK"


def test_zombie_process_is_fail():
    mod = _load()
    st, req, det, act = mod.process_verdict(
        unit_active=True, pid=12, pid_alive=False, port_up=False, pong_ok=False)
    assert st == "FAIL" and req
    assert "died" in det.lower() or "dead" in det.lower()
    st, req, det, act = mod.process_verdict(
        unit_active=True, pid=12, pid_alive=True, port_up=True, pong_ok=True)
    assert st == "OK"


def test_pong_401_403_timeout():
    mod = _load()
    st, req, det, act = mod.pong_verdict(401, "", "Unauthorized")
    assert st == "FAIL" and "401" in det
    st, req, det, act = mod.pong_verdict(403, "", "Forbidden")
    assert st == "FAIL" and "403" in det
    st, req, det, act = mod.pong_verdict(0, "", "timed out")
    assert st == "FAIL" and "timeout" in det.lower()
    st, req, det, act = mod.pong_verdict(200, "LLM request timed out", "")
    assert st == "FAIL"
    st, req, det, act = mod.pong_verdict(200, "PONG", "")
    assert st == "OK"


def test_run_timed_out_is_fail():
    mod = _load()
    st, req, det, act = mod.run_verdict("timed_out", "success")
    assert st == "FAIL" and req
    st, req, det, act = mod.run_verdict("success", "success")
    assert st == "OK"
    st, req, det, act = mod.run_verdict("failure", "failure")
    assert st == "WARN"
