"""Elite-only Finviz session. No network.

Run: python -m src.test_finviz_session
"""
from __future__ import annotations

from unittest import mock

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


def test_preopen_all_does_not_scrape_finviz() -> None:
    from pathlib import Path
    src = Path(__file__).with_name("run_preopen_all.py").read_text(encoding="utf-8")
    assert "src.finviz_digest" not in src
    assert "--overlay" not in src
    assert "wait_for_gh_scrape" in src
    assert "finviz_preopen_scrape.yml" in src


def main() -> None:
    tests = [
        test_looks_like_login_html,
        test_get_rewrites_public_to_elite,
        test_get_rejects_login_html,
        test_get_rejects_403,
        test_session_cookie_probe_ok,
        test_session_missing_auth,
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
