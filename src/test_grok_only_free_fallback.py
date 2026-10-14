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


def test_native_paid_deepseek_stays_blocked(monkeypatch):
    calls = _setup(monkeypatch, "https://api.deepseek.com")
    assert dc.chat([{"role": "user", "content": "q"}], "m") == ""
    assert calls == [False]
