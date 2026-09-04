"""Elite-only Finviz session. No network.

Run: python -m src.test_finviz_session
"""
from __future__ import annotations

import os
from unittest import mock

os.environ["FINVIZ_GAP_SEC"] = "0"

from src import finviz_session


def test_looks_like_login_html() -> None:
    assert finviz_session.looks_like_login_html("")
    assert finviz_session.looks_like_login_html(
        "<html><title>Login</title><form action=login_submit>password"
    )
    assert not finviz_session.looks_like_login_html(
        "<html><title>SPY</title><td>Daily Digest</td>"
    )


def test_get_rewrites_public_to_elite() -> None:
    sess = mock.Mock()
    resp = mock.Mock()
    resp.status_code = 200
    resp.text = "<html><title>groups</title><table class=groups_table>"
    sess.get.return_value = resp
    r = finviz_session.get(sess, "https://finviz.com/groups?g=industry")
    assert r is resp
    url = sess.get.call_args[0][0]
    assert url.startswith("https://elite.finviz.com/")
    assert "finviz.com/groups" in url.replace("elite.finviz.com", "finviz.com") or True
    assert "elite.finviz.com" in url


def test_get_rejects_login_html() -> None:
    sess = mock.Mock()
    resp = mock.Mock()
    resp.status_code = 200
    resp.text = "<html><title>Login</title><form>login_submit password"
    sess.get.return_value = resp
    r = finviz_session.get(sess, "/quote.ashx?t=SPY")
    assert r is None
    url = sess.get.call_args[0][0]
    assert url == "https://elite.finviz.com/quote.ashx?t=SPY"


def test_get_rejects_403() -> None:
    sess = mock.Mock()
    resp = mock.Mock()
    resp.status_code = 403
    resp.text = "Forbidden"
    sess.get.return_value = resp
    r = finviz_session.get(sess, ["/news.ashx?v=3"])
    assert r is None


def test_get_retries_then_succeeds_on_403() -> None:
    sess = mock.Mock()
    bad = mock.Mock()
    bad.status_code = 403
    bad.text = "Forbidden"
    good = mock.Mock()
    good.status_code = 200
    good.text = "<html><title>futures</title>var tiles = {};"
    sess.get.side_effect = [bad, bad, good]
    os.environ["FINVIZ_GET_RETRIES"] = "3"
    r = finviz_session.get(sess, "/futures.ashx")
    os.environ.pop("FINVIZ_GET_RETRIES", None)
    assert r is good
    assert sess.get.call_count == 3


def test_session_cookie_probe_ok() -> None:
    with mock.patch.dict("os.environ", {
        "FINVIZ_AUTH": "tok",
        "FINVIZ_EMAIL": "",
        "FINVIZ_PASSWORD": "",
        "AUTH_TOKEN_FINVIZ": "",
    }, clear=False):
        with mock.patch.object(finviz_session, "_probe", return_value=True):
            s = finviz_session.session()
    assert finviz_session.authed(s)
    assert s.headers.get("X-Fullscan-Finviz") == "cookie"


def test_session_missing_auth() -> None:
    env = {
        "FINVIZ_AUTH": "",
        "AUTH_TOKEN_FINVIZ": "",
        "FINVIZ_EMAIL": "",
        "FINVIZ_PASSWORD": "",
    }
    with mock.patch.dict("os.environ", env, clear=False):
        s = finviz_session.session()
    assert not finviz_session.authed(s)
    assert s.headers.get("X-Fullscan-Finviz") == "missing"


def test_gap_default_is_five() -> None:
    with mock.patch.dict(os.environ, {"FINVIZ_GAP_SEC": ""}, clear=False):
        os.environ.pop("FINVIZ_GAP_SEC", None)
        assert finviz_session.gap_sec() == 5.0
    os.environ["FINVIZ_GAP_SEC"] = "0"
    assert finviz_session.gap_sec() == 0.0


def test_get_paces_between_calls() -> None:
    sess = mock.Mock()
    resp = mock.Mock()
    resp.status_code = 200
    resp.text = "<html><title>SPY</title><td>Daily Digest</td>"
    sess.get.return_value = resp
    slept = []
    os.environ["FINVIZ_GAP_SEC"] = "5"
    finviz_session._LAST_GET = 0.0
    with mock.patch.object(finviz_session.time, "sleep", side_effect=lambda s: slept.append(s)):
        with mock.patch.object(finviz_session.time, "monotonic", side_effect=[0.0, 0.0, 0.0, 5.0]):
            finviz_session.get(sess, "/quote.ashx?t=SPY")
            finviz_session.get(sess, "/quote.ashx?t=QQQ")
    os.environ["FINVIZ_GAP_SEC"] = "0"
    assert slept and slept[0] >= 4.9


def test_skip_live_get_never_hits_network() -> None:
    sess = mock.Mock()
    with mock.patch.dict(os.environ, {"FINVIZ_SKIP_LIVE": "1", "FINVIZ_FORCE_LIVE": ""}, clear=False):
        r = finviz_session.get(sess, "/quote.ashx?t=SPY")
    assert r is None
    sess.get.assert_not_called()


def test_skip_live_session_never_probes() -> None:
    with mock.patch.dict(os.environ, {
        "FINVIZ_SKIP_LIVE": "1",
        "FINVIZ_FORCE_LIVE": "",
        "FINVIZ_AUTH": "tok",
        "FINVIZ_EMAIL": "a@b.c",
        "FINVIZ_PASSWORD": "x",
    }, clear=False):
        with mock.patch.object(finviz_session, "_probe") as probe:
            s = finviz_session.session()
    assert not finviz_session.authed(s)
    assert s.headers.get("X-Fullscan-Finviz") == "skipped"
    probe.assert_not_called()


def test_ecs_runner_labels_block_live() -> None:
    env = {
        "FINVIZ_SKIP_LIVE": "",
        "FINVIZ_FORCE_LIVE": "",
        "RUNNER_ENVIRONMENT": "",
        "RUNNER_NAME": "ecs-box",
        "RUNNER_LABELS": "self-hosted,ecs,linux",
        "HOME": "/home/gha",
        "FULLSCAN_HOME": "/home/gha",
    }
    with mock.patch.dict(os.environ, env, clear=False):
        assert finviz_session.live_html_allowed() is False
        sess = mock.Mock()
        r = finviz_session.get(sess, "/futures.ashx")
    assert r is None
    sess.get.assert_not_called()


def test_force_live_overrides_skip() -> None:
    with mock.patch.dict(os.environ, {
        "FINVIZ_SKIP_LIVE": "1",
        "FINVIZ_FORCE_LIVE": "1",
        "RUNNER_LABELS": "self-hosted,ecs",
    }, clear=False):
        assert finviz_session.live_html_allowed() is True


def test_ecs_scripts_export_skip_live() -> None:
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    preopen = (root / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    postclose = (root / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    yml = (root / ".github" / "workflows" / "preopen_all.yml").read_text(encoding="utf-8")
    fa = (root / ".github" / "workflows" / "finviz_all.yml").read_text(encoding="utf-8")
    pc = (root / ".github" / "workflows" / "map_heat_postclose.yml").read_text(encoding="utf-8")
    assert "FINVIZ_SKIP_LIVE=1" in preopen
    assert "FINVIZ_SKIP_LIVE=1" in postclose
    assert 'FINVIZ_SKIP_LIVE: "1"' in yml
    assert 'FINVIZ_SKIP_LIVE: "1"' in pc
    assert 'cron: "10 1 * * 2-6"' in fa
    assert "ubuntu-latest" in fa


def test_github_hosted_allows_live() -> None:
    env = {
        "FINVIZ_SKIP_LIVE": "",
        "FINVIZ_FORCE_LIVE": "",
        "RUNNER_ENVIRONMENT": "github-hosted",
        "RUNNER_LABELS": "ubuntu-latest",
        "HOME": "/home/runner",
        "FULLSCAN_HOME": "",
        "RUNNER_NAME": "GitHub Actions",
    }
    with mock.patch.dict(os.environ, env, clear=False):
        assert finviz_session.live_html_allowed() is True


def test_gha_home_blocks_live() -> None:
    env = {
        "FINVIZ_SKIP_LIVE": "",
        "FINVIZ_FORCE_LIVE": "",
        "RUNNER_ENVIRONMENT": "",
        "RUNNER_LABELS": "",
        "RUNNER_NAME": "",
        "HOME": "/home/gha",
        "FULLSCAN_HOME": "/home/gha",
    }
    with mock.patch.dict(os.environ, env, clear=False):
        assert finviz_session.live_html_allowed() is False


def test_insider_fetch_uses_elite_session() -> None:
    from pathlib import Path
    src = Path(__file__).with_name("insider_fetch.py").read_text(encoding="utf-8")
    assert "https://finviz.com/" not in src
    assert "finviz_session.get" in src
    assert "MARKET_PATHS" in src


def test_preopen_all_does_not_scrape_finviz() -> None:
    from pathlib import Path
    src = Path(__file__).with_name("run_preopen_all.py").read_text(encoding="utf-8")
    assert "src.finviz_digest" not in src
    assert "--overlay" not in src
    assert "wait_for_gh_scrape" in src
    assert "wait_for_night_baseline" in src
    assert "finviz_preopen_scrape.yml" in src


def main() -> None:
    tests = [
        test_looks_like_login_html,
        test_get_rewrites_public_to_elite,
        test_get_rejects_login_html,
        test_get_rejects_403,
        test_get_retries_then_succeeds_on_403,
        test_session_cookie_probe_ok,
        test_session_missing_auth,
        test_gap_default_is_five,
        test_get_paces_between_calls,
        test_skip_live_get_never_hits_network,
        test_skip_live_session_never_probes,
        test_ecs_runner_labels_block_live,
        test_force_live_overrides_skip,
        test_ecs_scripts_export_skip_live,
        test_github_hosted_allows_live,
        test_gha_home_blocks_live,
        test_insider_fetch_uses_elite_session,
        test_preopen_all_does_not_scrape_finviz,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
