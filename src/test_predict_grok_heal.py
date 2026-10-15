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


def test_spent_shot_does_not_open_another_search(monkeypatch):
    from src import run_predict
    monkeypatch.setenv("OPENCLAW_RESTART_ON_TIMEOUT", "1")
    monkeypatch.setattr(run_predict.config, "openclaw_enabled", lambda: True)
    monkeypatch.setattr(
        "src.openclaw_fastfail.predict_attempt", lambda restart=None: "skip")
    calls = []
    monkeypatch.setattr(
        run_predict.deepseek_client, "_openclaw_chat",
        lambda *a, **k: calls.append(("openclaw", k.get("tools"))))
    monkeypatch.setattr(
        run_predict.deepseek_client, "chat",
        lambda *a, **k: calls.append(("chat", k.get("tools"),
                                      k.get("force_deepseek"))))
    monkeypatch.setattr(
        run_predict.deepseek_client, "_free_primary_ready", lambda: False)
    assert run_predict._grok_then_free(
        "r", "u", "2026-10-15", "PREDICT 2026-10-15") == ""
    assert calls == []


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


def test_glm_trace_is_not_success(monkeypatch):
    monkeypatch.setattr(h, "breaker_opened", lambda: False)
    monkeypatch.setattr(h, "reset_breaker", lambda: None)
    st = {"predict": True, "trace": False}
    envs = []
    def run(e):
        envs.append(e)
        st["trace"] = True
    r = h.heal("d", lambda: st["predict"], run, guard=G(), now=_early,
               trace_ok=lambda: st["trace"])
    assert r["ok"] and r["retries"] == 1
    assert envs[0]["PREDICT_GROK_REWRITE"] == "1"
    assert envs[0]["OPENCLAW_RESTART_ON_TIMEOUT"] == "1"
    assert "XAI_API_KEY" not in envs[0]


def test_empty_backup_does_not_skip_grok_when_gateway_is_set(monkeypatch):
    """Zhipu/TokenHub 402 must not cancel essays on the ECS gateway."""
    from pathlib import Path
    from src import run_preopen_all as rpa
    monkeypatch.setenv("LLM_BACKEND", "auto")
    monkeypatch.setenv("GROK_ONLY", "0")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    assert rpa._gateway_blocks_essay_skip() is True
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL")
    assert rpa._gateway_blocks_essay_skip() is False
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    monkeypatch.setenv("LLM_BACKEND", "deepseek")
    assert rpa._gateway_blocks_essay_skip() is False
    src = Path(__file__).resolve().parent.joinpath("run_preopen_all.py").read_text()
    assert "Grok essays still run" in src
    gate = src.split("if not skip_writes and not skip_essays:", 1)[1]
    gate = gate.split("if not skip_writes:", 1)[0]
    assert gate.index("_gateway_blocks_essay_skip") < gate.index("_deepseek_credits_ok")


def test_ecs_preopen_exports_grok_before_run():
    from pathlib import Path
    s = (Path(__file__).resolve().parent.parent / "scripts" / "ecs_preopen.sh").read_text()
    i = s.index('export LLM_BACKEND="${LLM_BACKEND:-grok}"')
    assert 'export GROK_ONLY="${GROK_ONLY:-1}"' in s
    assert i < s.index('-m src.run_preopen_all')
