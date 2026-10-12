from datetime import datetime
from src import predict_grok_heal as h

ET = h.ET


class G:
    def __init__(self, ok=True):
        self.ok, self.n = ok, 0
    def hard_restart(self, r):
        self.n += 1
    def real_completion(self, r, tag):
        return self.ok


def _early():
    return datetime(2026, 10, 13, 6, 30, tzinfo=ET)


def test_ok_no_retry(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    g = G()
    r = h.heal("d", lambda: True, lambda e: 0, guard=g, now=_early)
    assert r["ok"] and g.n == 0


def test_empty_retries_grok_only(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    monkeypatch.setattr(h, "reset_breaker", lambda: None)
    st = {"ok": False}; envs = []
    def run(e):
        envs.append(e); st["ok"] = True
    r = h.heal("d", lambda: st["ok"], run, guard=G(), now=_early)
    assert r["ok"] and r["retries"] == 1
    assert envs[0]["GROK_ONLY"] == "1" and "DEEPSEEK_API_KEY" not in envs[0]


def test_max_two_then_red(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    monkeypatch.setattr(h, "reset_breaker", lambda: None)
    g = G()
    r = h.heal("d", lambda: False, lambda e: 1, guard=g, now=_early)
    assert not r["ok"] and r["retries"] == 2 and g.n == 2


def test_no_retry_after_0900(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    g = G()
    late = datetime(2026, 10, 13, 9, 0, tzinfo=ET)
    r = h.heal("d", lambda: False, lambda e: 0, guard=g, now=late)
    assert not r["ok"] and g.n == 0 and "09:00" in r["reason"]


def test_breaker_open_triggers(monkeypatch):
    st = {"b": True}
    monkeypatch.setattr(h, "breaker_opened", lambda: st["b"])
    monkeypatch.setattr(h, "reset_breaker", lambda: st.update(b=False))
    g = G()
    r = h.heal("d", lambda: True, lambda e: 0, guard=g, now=_early)
    assert r["ok"] and g.n == 1


def test_auth_treated_like_hang(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    monkeypatch.setattr(h, "reset_breaker", lambda: None)
    logs = ["OpenClaw HTTP 401: token expired"]; st = {"ok": True}
    def run(e):
        assert e["GROK_ONLY"] == "1"; logs[0] = ""
    g = G()
    r = h.heal("d", lambda: st["ok"], run, log_text=lambda: logs[0],
               guard=g, now=_early)
    assert r["ok"] and g.n == 1


def test_auth_still_failing_says_device_login(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    monkeypatch.setattr(h, "reset_breaker", lambda: None)
    r = h.heal("d", lambda: False, lambda e: 1,
               log_text=lambda: "refresh_token invalid_grant",
               guard=G(), now=_early)
    assert not r["ok"] and "device login" in r["reason"]


def test_restart_fails_auth_red(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    r = h.heal("d", lambda: False, lambda e: 0, log_text=lambda: "HTTP 403",
               guard=G(ok=False), now=_early)
    assert not r["ok"] and "device login" in r["reason"]


def test_grok_backend_skips_deepseek_preflight(monkeypatch):
    from src import run_preopen_all as rpa
    from src import deepseek_client as dc
    monkeypatch.setenv("LLM_BACKEND", "grok")
    monkeypatch.delenv("GROK_ONLY", raising=False)
    monkeypatch.setattr(dc, "credits_preflight",
                        lambda: {"ok": False, "reason": "402"})
    assert rpa._deepseek_credits_ok() is True
    monkeypatch.setenv("LLM_BACKEND", "auto")
    monkeypatch.setenv("GROK_ONLY", "0")
    assert rpa._deepseek_credits_ok() is False


def test_ecs_preopen_exports_grok_before_run():
    from pathlib import Path
    s = (Path(__file__).resolve().parent.parent / "scripts" / "ecs_preopen.sh").read_text()
    i = s.index('export LLM_BACKEND="${LLM_BACKEND:-grok}"')
    assert 'export GROK_ONLY="${GROK_ONLY:-1}"' in s
    assert i < s.index('-m src.run_preopen_all')
