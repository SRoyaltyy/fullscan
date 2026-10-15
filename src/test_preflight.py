"""Tests for scripts/preflight.py and its workflow wiring (2026-10-15)."""
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"
spec = importlib.util.spec_from_file_location("preflight", ROOT / "scripts" / "preflight.py")
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)


def _item(i):
    return next(x for x in json.loads((ROOT / "config/prereq_checklist.json").read_text())["checks"]["preopen_all"] if x["id"] == i)


def test_config_has_every_check_coded():
    cfg = json.loads((ROOT / "config/prereq_checklist.json").read_text())
    for wf, items in cfg["checks"].items():
        for it in items:
            assert it["id"] in pf.CHECKS, (wf, it["id"])
            assert it.get("plain")


def test_runner_ubuntu_fails():
    ok, _ = pf.check_runner_is_ecs({}, {"RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_NAME": "GitHub Actions 1000030400"}, "d")
    assert not ok
    ok, _ = pf.check_runner_is_ecs({}, {"RUNNER_ENVIRONMENT": "self-hosted", "RUNNER_NAME": "ecs-1"}, "d")
    assert ok


def test_backend_must_be_grok():
    it = _item("backend_grok")
    assert pf.check_backend_grok(it, {"LLM_BACKEND": "grok"}, "d")[0]
    assert not pf.check_backend_grok(it, {"LLM_BACKEND": "deepseek"}, "d")[0]
    assert not pf.check_backend_grok(it, {}, "d")[0]


def test_read_cap():
    it = _item("read_cap_fits")
    assert not pf.check_read_cap_fits(it, {"OPENCLAW_PREDICT_READ_CAP_S": "75"}, "d")[0]
    assert pf.check_read_cap_fits(it, {"OPENCLAW_PREDICT_READ_CAP_S": "300"}, "d")[0]
    assert not pf.check_read_cap_fits(it, {}, "d")[0]


def test_paid_fallback_rejected():
    it = _item("no_unapproved_paid_fallback")
    assert not pf.check_no_unapproved_paid_fallback(it, {"LLM_FALLBACK_MODEL": "glm-5.3"}, "d")[0]
    assert pf.check_no_unapproved_paid_fallback(it, {"LLM_FALLBACK_MODEL": ""}, "d")[0]
    assert pf.check_no_unapproved_paid_fallback(it, {"LLM_FALLBACK_MODEL": "glm-5.3-flash"}, "d")[0]


def test_gateway_down_fails_fast():
    ok, detail = pf.check_gateway_up({"url": "http://127.0.0.1:9/health", "timeout_s": 2}, {}, "d")
    assert not ok and "127.0.0.1:9" in detail


def test_run_writes_named_failures(tmp_path, monkeypatch):
    monkeypatch.setitem(pf.CHECKS, "gateway_up", lambda *a: (False, "down"))
    monkeypatch.setitem(pf.CHECKS, "xai_token_outlives_run", lambda *a: (True, "300 min left"))
    monkeypatch.setitem(pf.CHECKS, "no_twin_preopen", lambda *a: (True, "no twin"))
    env = {"RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_NAME": "GitHub Actions 1",
           "LLM_BACKEND": "deepseek", "OPENCLAW_PREDICT_READ_CAP_S": "75", "LLM_FALLBACK_MODEL": "glm-5.3"}
    rc = pf.run("preopen_all", "2026-10-15", env, tmp_path)
    assert rc == 1
    doc = json.loads((tmp_path / "2026-10-15_preopen_all_preflight.json").read_text())
    assert set(doc["failed"]) == {"runner_is_ecs", "backend_grok", "gateway_up",
                                  "no_unapproved_paid_fallback", "read_cap_fits"}
    md = (tmp_path / "2026-10-15_preopen_all_preflight.md").read_text()
    assert "STOPPED" in md and "not on the ECS box" in md


def test_run_pass(tmp_path, monkeypatch):
    for k in ("gateway_up", "xai_token_outlives_run", "no_twin_preopen"):
        monkeypatch.setitem(pf.CHECKS, k, lambda *a: (True, "ok"))
    env = {"RUNNER_ENVIRONMENT": "self-hosted", "RUNNER_NAME": "ecs", "LLM_BACKEND": "grok",
           "OPENCLAW_PREDICT_READ_CAP_S": "300", "LLM_FALLBACK_MODEL": ""}
    assert pf.run("preopen_all", "2026-10-15", env, tmp_path) == 0


def test_predict_missing_named(tmp_path):
    rc = pf.run("fm_preopen", "1999-01-01", {}, tmp_path)
    assert rc == 1
    assert "predict missing" in (tmp_path / "1999-01-01_fm_preopen_preflight.md").read_text()


def test_wiring():
    pre = (WF / "preopen_all.yml").read_text()
    assert "  preflight:\n" in pre
    assert "needs: [gate, scrape, preflight]" in pre
    assert "needs.preflight.result == 'success'" in pre
    assert "timeout-minutes: 2" in pre
    assert "scripts/preflight.py --workflow preopen_all" in pre
    assert "default: \"ecs\"" in pre
    assert "llm_backend || 'grok'" in pre and "llm_backend || 'deepseek'" not in pre
    assert 'LLM_FALLBACK_MODEL: ""' in pre
    assert 'OPENCLAW_READ_CAP_S: "75"' in pre
    assert 'OPENCLAW_PREDICT_READ_CAP_S: "300"' in pre
    assert "essays use the free model" not in pre
    orch = (WF / "daily_orchestrator.yml").read_text()
    assert "inputs[runner]=ecs" in orch and "inputs[runner]=ubuntu\" \\\n              -f \"inputs[llm_backend]=auto" not in orch
    fm = (WF / "fm_preopen.yml").read_text()
    assert "--workflow fm_preopen" in fm and "predict missing" in fm
    ecs = (ROOT / "scripts" / "ecs_preopen.sh").read_text()
    assert 'OPENCLAW_READ_CAP_S="${OPENCLAW_READ_CAP_S:-75}"' in ecs
    assert 'OPENCLAW_PREDICT_READ_CAP_S="${OPENCLAW_PREDICT_READ_CAP_S:-300}"' in ecs


def test_read_cap_is_per_stage(monkeypatch):
    from src import deepseek_client as dc
    monkeypatch.setenv("OPENCLAW_READ_CAP_S", "75")
    monkeypatch.setenv("OPENCLAW_PREDICT_READ_CAP_S", "300")
    assert dc._read_cap(420, "PREDICT 2026-10-16") == 300
    assert dc._read_cap(420, "PREDICT 2026-10-16 nosearch") == 300
    assert dc._read_cap(420, "NEWS_JUDGE 2026-10-16") == 300
    assert dc._read_cap(420, "SECTOR PREDICT XLK") == 75
    assert dc._read_cap(420, "grok_review") == 75
    assert dc._read_cap(420) == 75
    monkeypatch.delenv("OPENCLAW_PREDICT_READ_CAP_S")
    assert dc._read_cap(420, "PREDICT 2026-10-16") == 75


def _runs(monkeypatch, runs):
    monkeypatch.setattr(pf, "_preopen_runs", lambda env: runs)


def test_twin_guard_earlier_active_fails(monkeypatch, tmp_path):
    env = {"GITHUB_RUN_ID": "2", "PREOPEN_LOCK": str(tmp_path / "x" / "l")}
    _runs(monkeypatch, [
        {"id": 1, "status": "in_progress", "conclusion": None, "created_at": "2026-10-15T10:31:00Z"},
        {"id": 2, "status": "in_progress", "conclusion": None, "created_at": "2026-10-15T11:48:32Z"}])
    ok, d = pf.check_no_twin_preopen({}, env, "2026-10-15")
    assert not ok and "1" in d
    # The earlier run is not stopped by a later queued twin.
    env["GITHUB_RUN_ID"] = "1"
    assert pf.check_no_twin_preopen({}, env, "2026-10-15")[0]


def test_orchestrator_refused_after_success(monkeypatch, tmp_path):
    env = {"GITHUB_RUN_ID": "3", "PREOPEN_STARTED_BY": "orchestrator",
           "PREOPEN_LOCK": str(tmp_path / "x" / "l")}
    runs = [{"id": 1, "status": "completed", "conclusion": "success", "created_at": "2026-10-15T10:31:00Z"},
            {"id": 3, "status": "in_progress", "conclusion": None, "created_at": "2026-10-15T12:00:00Z"}]
    _runs(monkeypatch, runs)
    assert not pf.check_no_twin_preopen({}, env, "2026-10-15")[0]
    env["PREOPEN_STARTED_BY"] = "manual"
    assert pf.check_no_twin_preopen({}, env, "2026-10-15")[0]


def test_orchestrator_wiring():
    orch = (WF / "daily_orchestrator.yml").read_text()
    assert "inputs[started_by]=orchestrator" in orch
    assert "already queued/running/succeeded" in orch
    pre = (WF / "preopen_all.yml").read_text()
    assert "started_by:" in pre and "PREOPEN_STARTED_BY" in pre
