"""GROK_ONLY must still let the FREE GLM provider answer when Grok is empty
(10-14 #171), but never native paid api.deepseek.com."""
import os
from src import config, deepseek_client as dc


def _setup(monkeypatch, base):
    monkeypatch.setenv("GROK_ONLY", "1")
    monkeypatch.setenv("LLM_MODEL_OVERRIDE", "glm-5.3-flash")
    monkeypatch.delenv("LLM_FALLBACK_API_KEY", raising=False)
    monkeypatch.setattr(config, "DEEPSEEK_API_KEY", "k")
    monkeypatch.setattr(config, "DEEPSEEK_BASE_URL", base)
    monkeypatch.setattr(config, "grok_only", lambda: True)
    dc._FALLBACK.clear(); dc._FALLBACK["active"] = False
    calls = []

    def fake(messages, model, *a, **kw):
        calls.append(kw.get("force_deepseek", False))
        return "x" * 300 if kw.get("force_deepseek") else ""
    monkeypatch.setattr(dc, "_chat_impl", fake)
    monkeypatch.setattr(dc, "last_sector_degraded", lambda: False)
    return calls


def test_free_glm_answers_when_grok_empty(monkeypatch):
    calls = _setup(monkeypatch, "https://open.bigmodel.cn/api/paas/v4")
    assert len(dc.chat([{"role": "user", "content": "q"}], "m")) == 300
    assert calls == [False, True]


def test_no_balance_429_is_not_retried(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_PRIMARY_EMPTY_FILE", str(tmp_path / "empty.json"))
    monkeypatch.setattr(config, "DEEPSEEK_API_KEY", "k")
    monkeypatch.setattr(config, "DEEPSEEK_BASE_URL",
                        "https://open.bigmodel.cn/api/paas/v4")
    dc._FALLBACK.clear()
    dc._FALLBACK["active"] = False
    calls = []

    class Resp:
        status_code = 429
        text = ('{"error":{"code":"1113","message":"余额不足或无可用资源包,请充值。"}}')

        def raise_for_status(self):
            raise AssertionError("no-balance must not be treated as success")

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(url)
        return Resp()

    monkeypatch.setattr(dc.requests, "post", fake_post)
    monkeypatch.setattr(dc.time, "sleep", lambda s: None)
    try:
        dc._post_once({"model": "glm-5.3-flash"}, retries=4)
        raised = False
    except RuntimeError as exc:
        raised = "no balance" in str(exc)
    assert raised
    assert len(calls) == 1
    assert dc._primary_is_empty()


def test_known_empty_primary_skips_flash(monkeypatch, tmp_path):
    flag = tmp_path / "empty.json"
    monkeypatch.setenv("LLM_PRIMARY_EMPTY_FILE", str(flag))
    flag.write_text('{"until": 9999999999, "detail": "1113"}', encoding="utf-8")
    calls = _setup(monkeypatch, "https://open.bigmodel.cn/api/paas/v4")
    monkeypatch.setenv("LLM_FALLBACK_API_KEY", "hub-key")
    monkeypatch.setenv("LLM_FALLBACK_BASE_URL", "https://tokenhub.example/v1")
    monkeypatch.setenv("LLM_FALLBACK_MODEL", "glm-5.3")
    dc.chat([{"role": "user", "content": "q"}], "m")
    assert True not in calls
    assert dc.fallback_active()


def test_native_paid_deepseek_stays_blocked(monkeypatch):
    calls = _setup(monkeypatch, "https://api.deepseek.com")
    assert dc.chat([{"role": "user", "content": "q"}], "m") == ""
    assert calls == [False]
