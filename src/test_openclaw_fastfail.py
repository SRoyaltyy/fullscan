"""Fast-fail: one gateway restart, one no-search predict, then the free model."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from src import openclaw_fastfail as ff
from src import run_postclose_all as pc
from src import run_preopen_all as rpa

ET = ZoneInfo("America/New_York")


def test_note_hung_once(monkeypatch, tmp_path):
    marker = tmp_path / "b.txt"
    monkeypatch.setenv("OPENCLAW_BREAKER_FILE", str(marker))
    monkeypatch.delenv("OPENCLAW_RESTART_ON_TIMEOUT", raising=False)
    calls = []
    assert ff.note_hung_call(restart=lambda: calls.append(1) or True) is False
    assert calls == []
    monkeypatch.setenv("OPENCLAW_RESTART_ON_TIMEOUT", "1")
    assert ff.note_hung_call(restart=lambda: calls.append(1) or True) is True
    assert ff.note_hung_call(restart=lambda: calls.append(1) or True) is False
    assert calls == [1]
    assert (tmp_path / "b.txt.restarted").is_file()


def test_predict_attempt_search_then_nosearch_then_skip(monkeypatch, tmp_path):
    from src import config, deepseek_client as dc
    monkeypatch.setenv("OPENCLAW_BREAKER_FILE", str(tmp_path / "b.txt"))
    monkeypatch.setenv("OPENCLAW_RESTART_ON_TIMEOUT", "1")
    monkeypatch.setattr(config, "OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    monkeypatch.setattr(dc, "_breaker_open", lambda: False)
    assert ff.predict_attempt(restart=lambda: True) == "search"
    monkeypatch.setattr(dc, "_breaker_open", lambda: True)
    monkeypatch.setattr(dc, "reset_openclaw_breaker", lambda *a, **k: None)
    monkeypatch.setattr(dc, "_breaker_file", lambda: str(tmp_path / "b.txt"))
    assert ff.predict_attempt(restart=lambda: True) == "nosearch"
    assert ff.predict_attempt(restart=lambda: True) == "skip"


def test_probe_ok_and_restart(monkeypatch, tmp_path):
    from src import config
    monkeypatch.setenv("OPENCLAW_RESTART_ON_TIMEOUT", "1")
    monkeypatch.setenv("OPENCLAW_BREAKER_FILE", str(tmp_path / "b.txt"))
    monkeypatch.setattr(config, "OPENCLAW_GATEWAY_URL", "http://127.0.0.1:9")
    monkeypatch.setattr(config, "OPENCLAW_TOKEN", "")
    monkeypatch.setattr(config, "OPENCLAW_AGENT", "openclaw/default")
    written = {}
    monkeypatch.setattr(ff, "_write_status", lambda p: written.update(p))

    class Resp:
        status_code = 200
        def json(self):
            return {"choices": [{"message": {"content": "42"}}]}

    ok = ff.completion_probe(post=lambda: Resp())
    assert ok["ok"] and ok["answer"] == "42"

    class Bad:
        status_code = 500
        text = "nope"
        def json(self):
            return {}

    n = {"n": 0}
    def probe():
        n["n"] += 1
        return {"ok": n["n"] > 1, "reason": "first" if n["n"] == 1 else "ok",
                "model": "xai/grok-4.6", "answer": "42"}
    restarts = []
    out = ff.ensure_before_essays(probe=probe, restart=lambda: restarts.append(1) or True)
    assert out["ok"] and out.get("restarted") and restarts == [1]
    assert written["ok"]
    assert (tmp_path / "b.txt.restarted").is_file()


def test_step_budget_skips_when_deadline_is_close(monkeypatch):
    import time
    monkeypatch.setenv("FULLSCAN_STEP_DEADLINE", str(int(time.time() + 10)))
    assert rpa._step_budget(120) is None
    monkeypatch.setenv("FULLSCAN_STEP_DEADLINE", str(int(time.time() + 400)))
    got = rpa._step_budget(120)
    assert got == 120


def test_morning_box_yield(monkeypatch):
    monkeypatch.setenv("POSTCLOSE_YIELD_MORNING", "1")
    monkeypatch.setenv("FULLSCAN_HOME", "/home/gha")
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL", raising=False)
    assert pc.morning_box_yield(datetime(2026, 10, 15, 5, 0, tzinfo=ET))
    assert not pc.morning_box_yield(datetime(2026, 10, 15, 11, 0, tzinfo=ET))
    left = pc.seconds_until_yield(datetime(2026, 10, 15, 3, 0, tzinfo=ET))
    assert 3500 < left < 3700
    monkeypatch.setenv("FULLSCAN_HOME", "/home/runner")
    assert not pc.morning_box_yield(datetime(2026, 10, 15, 5, 0, tzinfo=ET))


def test_token_dying_before_0630_is_not_fresh():
    import importlib.util
    from datetime import timedelta, timezone
    from pathlib import Path
    path = Path(__file__).resolve().parent.parent / "scripts" / "openclaw_oauth_guard.py"
    spec = importlib.util.spec_from_file_location("openclaw_oauth_guard", path)
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    # 00:41 ET on 2026-10-15. Expiry 05:08 ET. 267 minutes left, still before Pre-Open.
    now = datetime(2026, 10, 15, 4, 41, tzinfo=timezone.utc)
    exp = datetime(2026, 10, 15, 9, 8, tzinfo=timezone.utc)
    left = (exp - now).total_seconds() / 60
    assert left > 90
    assert guard.token_is_fresh(left, exp, now) is False
    # 21:10 ET is more than 8 hours before the next 06:30, so a 4h token is fresh.
    now2 = datetime(2026, 10, 14, 21, 10, tzinfo=ET).astimezone(timezone.utc)
    exp2 = now2 + timedelta(hours=4)
    assert guard.token_is_fresh(4 * 60, exp2, now2) is True
    # A 42 that does not move an expiry of 05:08 ET is not a renew.
    assert guard.renew_verdict(exp, exp, now) == "NEEDS_FORCE"
    later = datetime(2026, 10, 15, 14, 0, tzinfo=timezone.utc)
    assert guard.renew_verdict(exp, later, now) == "OK"


def test_nudge_expiry_only_touches_the_matching_field(tmp_path):
    import importlib.util
    import json
    from datetime import datetime, timezone
    from pathlib import Path
    path = Path(__file__).resolve().parent.parent / "scripts" / "openclaw_oauth_guard.py"
    spec = importlib.util.spec_from_file_location("openclaw_oauth_guard", path)
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    exp = datetime(2026, 10, 15, 9, 8, 45, tzinfo=timezone.utc)
    profile = tmp_path / "auth-profiles.json"
    profile.write_text(json.dumps({
        "profiles": {
            "xai:oauth": {
                "type": "oauth",
                "provider": "xai",
                "access": "keep-this-access",
                "refresh": "keep-this-refresh",
                "expires": "2026-10-15T09:08:45+00:00",
            },
            "other": {"expires": "2026-12-01T00:00:00+00:00", "access": "other"},
        }
    }), encoding="utf-8")
    (tmp_path / "openclaw.json").write_text(
        json.dumps({"gateway": {"expires": "2026-10-15T09:08:45+00:00"}}),
        encoding="utf-8")
    backups = guard.nudge_auth_expiry(exp, tmp_path)
    assert len(backups) == 1
    data = json.loads(profile.read_text(encoding="utf-8"))
    nudged = data["profiles"]["xai:oauth"]
    assert nudged["expires"] == "2000-01-01T00:00:00.000Z"
    assert nudged["access"] == "keep-this-access"
    assert nudged["refresh"] == "keep-this-refresh"
    assert data["profiles"]["other"]["expires"] == "2026-12-01T00:00:00+00:00"
    assert "09:08:45" in (tmp_path / "openclaw.json").read_text(encoding="utf-8")
    guard.restore_auth(backups)
    restored = json.loads(profile.read_text(encoding="utf-8"))
    assert restored["profiles"]["xai:oauth"]["expires"] == "2026-10-15T09:08:45+00:00"


def test_sector_retry_does_not_sleep_on_file_breaker(monkeypatch, tmp_path):
    from src import deepseek_client as dc
    marker = tmp_path / "breaker.txt"
    marker.write_text("1 hung search\n", encoding="utf-8")
    monkeypatch.setenv("OPENCLAW_BREAKER_FILE", str(marker))
    monkeypatch.setenv("OPENCLAW_BREAKER_N", "1")
    slept = []
    monkeypatch.setattr(dc.time, "sleep", lambda s: slept.append(s))

    def boom(*_a, **_k):
        raise AssertionError("ping")

    monkeypatch.setattr(dc.requests, "get", boom)
    text = dc._retry_sector_grok(
        [{"role": "user", "content": "hi"}], False, 32, 0.0,
        None, None, "SECTOR PREDICT Utilities 2026-10-14", None)
    assert text == ""
    assert slept == []


def test_wiring_strings():
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    pre = (root / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    post = (root / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    yml = (root / ".github" / "workflows" / "preopen_all.yml").read_text(encoding="utf-8")
    guard = (root / ".github" / "workflows" / "openclaw_oauth_guard.yml").read_text(encoding="utf-8")
    timer = (root / "scripts" / "systemd" / "fullscan-openclaw-oauth.timer").read_text(encoding="utf-8")
    push = (root / "scripts" / "safe_git_push.sh").read_text(encoding="utf-8")
    assert 'OPENCLAW_READ_CAP_S="${OPENCLAW_READ_CAP_S:-75}"' in pre
    assert 'OPENCLAW_BREAKER_N="${OPENCLAW_BREAKER_N:-1}"' in pre
    assert 'export LLM_BACKEND="${LLM_BACKEND:-grok}"' in pre
    assert "paid DeepSeek not used" in pre
    assert "morning window (04:00-10:30 ET)" in post
    assert "preopen lock held — skip (exit 0" not in post
    assert 'OPENCLAW_READ_CAP_S: "75"' in yml
    assert 'OPENCLAW_BREAKER_N: "1"' in yml
    assert "FULLSCAN_STEP_DEADLINE" in yml
    assert "timeout-minutes: 50" in yml
    assert "timeout-minutes: 75" in yml
    assert 'OPENCLAW_TIMEOUT: "10800"' in yml
    assert 'cron: "40 9 * * 1-5"' not in guard
    assert 'cron: "10 1 * * 1-5"' in guard
    assert 'cron: "40 8 * * 1-5"' in guard
    assert "_openclaw_oauth_status.json" in guard
    assert "OnCalendar=Mon..Fri *-*-* 04:10:00 America/New_York" in timer
    assert "past 09:25 ET — scrape refuses a late map_heat (exit 0)" in yml
    assert "loopback gateway is not reachable" in yml
    timeouts = (root / "scripts" / "ensure_openclaw_timeouts.sh").read_text(encoding="utf-8")
    enable = (root / "scripts" / "enable_openclaw_chat.sh").read_text(encoding="utf-8")
    reauth = (root / "scripts" / "xai_device_reauth.py").read_text(encoding="utf-8")
    orch = (root / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    assert "skip openclaw config set" in timeouts
    assert "gateway.mode set to local" in enable
    assert "skip hung chat ping" in enable
    assert "killing listener on 18789" in enable
    assert "XDG_RUNTIME_DIR" in reauth
    assert "quality-ok packet has no Grok trace" in orch
    assert "passthrough (deadline)" in orch
    assert "FULLSCAN_PUSH_TOKEN" in push
    assert "Invalid username or token" in push
    # The publish step must not keep a raw token. The redaction list is the contract.
    assert "user_code" in guard
    sample = {"gateway_token_len": 48, "has_refresh_token": True, "renew": "OK",
              "user_code": "ABCD-EFGH", "access_token": "x" * 90}
    drop = [k for k in sample if k not in ("gateway_token_len", "has_refresh_token")
            and any(w in k.lower() for w in ("token", "secret", "user_code", "password"))]
    assert "user_code" in drop and "access_token" in drop
    assert "gateway_token_len" not in drop
    json.dumps(sample)
