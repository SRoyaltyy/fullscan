"""Shared Finviz Elite HTTP session.

Live HTML (groups, futures, calendars, news, quotes) must go to
elite.finviz.com with a real login. Public finviz.com 403s cloud IPs
(Aliyun ECS and Azure GitHub-hosted alike).

Auth, first match wins:
  1. FINVIZ_AUTH / AUTH_TOKEN_FINVIZ cookie
  2. FINVIZ_EMAIL + FINVIZ_PASSWORD (same login the CSV exporter uses)

Never log secrets. If neither works, get() returns None so callers skip
instead of hammering public Finviz into 403s.
"""
from __future__ import annotations

import os
from typing import Iterable

import requests

ELITE = "https://elite.finviz.com"
PUBLIC = "https://finviz.com"
LOGIN_URLS = (
    f"{PUBLIC}/login_submit.ashx",
    f"{ELITE}/login_submit.ashx",
)
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def looks_like_login_html(text: str) -> bool:
    if not text:
        return True
    low = text[:8000].lower()
    if "login_submit" in low and "password" in low:
        return True
    if "<title>login" in low:
        return True
    return False


def quote_urls(ticker: str) -> list[str]:
    t = (ticker or "").strip().upper()
    return [f"{ELITE}/quote.ashx?t={t}"]


def elite_url(path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return ELITE + path


def _set_auth_cookie(sess: requests.Session, token: str) -> None:
    for domain in (".finviz.com", "finviz.com", "elite.finviz.com"):
        sess.cookies.set("auth", token, domain=domain)


def _probe(sess: requests.Session) -> bool:
    try:
        r = sess.get(f"{ELITE}/quote.ashx?t=SPY", timeout=25)
    except requests.RequestException:
        return False
    if r.status_code != 200 or looks_like_login_html(r.text):
        return False
    return True


def session() -> requests.Session:
    """Authenticated Elite session. Always returns a Session; check authed()."""
    s = requests.Session()
    s.headers.update(UA)
    token = (
        os.environ.get("FINVIZ_AUTH")
        or os.environ.get("AUTH_TOKEN_FINVIZ")
        or ""
    ).strip()
    if token:
        _set_auth_cookie(s, token)
        if _probe(s):
            print("[finviz] session: Elite cookie OK")
            s.headers["X-Fullscan-Finviz"] = "cookie"
            return s
        print("[finviz] cookie present but Elite probe failed — trying password")

    email = (os.environ.get("FINVIZ_EMAIL") or "").strip()
    password = (os.environ.get("FINVIZ_PASSWORD") or "").strip()
    if email and password:
        for url in LOGIN_URLS:
            try:
                r = s.post(
                    url,
                    data={"email": email, "password": password},
                    allow_redirects=True,
                    timeout=30,
                )
                print(f"[finviz] login POST {url.split('/')[-1]} → {r.status_code}")
            except requests.RequestException as e:
                print(f"[finviz] login POST failed: {e}")
                continue
            if _probe(s):
                print("[finviz] session: Elite email/password OK")
                s.headers["X-Fullscan-Finviz"] = "password"
                return s
        print("[finviz] ELITE AUTH FAILED — live HTML scrape will skip")
        s.headers["X-Fullscan-Finviz"] = "failed"
        return s

    print("[finviz] ELITE AUTH MISSING — set FINVIZ_AUTH or FINVIZ_EMAIL/PASSWORD")
    s.headers["X-Fullscan-Finviz"] = "missing"
    return s


def authed(sess: requests.Session) -> bool:
    flag = (sess.headers.get("X-Fullscan-Finviz") or "").lower()
    return flag in ("cookie", "password")


def get(
    sess: requests.Session,
    paths: str | Iterable[str],
    timeout: int = 30,
) -> requests.Response | None:
    """GET Elite first. Do not fall through to public finviz.com from the cloud."""
    if isinstance(paths, str):
        paths = [paths]
    last_err = None
    for path in paths:
        url = path if str(path).startswith("http") else elite_url(str(path))
        if url.startswith(PUBLIC + "/") and "login_submit" not in url:
            # Public pages 403 from ECS/Azure. Skip unless it is Elite.
            url = url.replace(PUBLIC, ELITE, 1)
        try:
            r = sess.get(url, timeout=timeout)
        except requests.RequestException as e:
            last_err = f"{e}"
            print(f"[finviz] {url}: {e}")
            continue
        if r.status_code == 200 and not looks_like_login_html(r.text):
            return r
        last_err = f"{r.status_code}"
        print(f"[finviz] {r.status_code} {url}")
    if last_err:
        print(f"[finviz] all Elite URLs failed ({last_err})")
    return None
