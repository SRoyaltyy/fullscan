"""Finviz index quote 403 → yfinance fallback. No network.

Run: python -m src.test_finviz_digest
"""
from __future__ import annotations

from unittest import mock

import requests

from src.finviz_digest import _scrape_index_digest, _yf_index_digest


class _FakeFast:
    def __init__(self, last, prev):
        self.last_price = last
        self.previous_close = prev

    def get(self, key, default=None):
        return getattr(self, key, default)


def test_yf_index_digest() -> None:
    fake = mock.Mock()
    fake.Ticker.return_value.fast_info = _FakeFast(642.10, 644.80)
    with mock.patch.dict("sys.modules", {"yfinance": fake}):
        row = _yf_index_digest("SPY")
    assert row is not None
    assert row["ticker"] == "SPY"
    assert row["source"] == "yfinance"
    assert "642.10" in row["digest"]
    assert row["error"] is None


def test_scrape_falls_to_yfinance_on_403() -> None:
    sess = mock.Mock()
    resp = mock.Mock()
    resp.raise_for_status.side_effect = requests.HTTPError("403 Client Error")
    sess.get.return_value = resp

    fake = mock.Mock()
    fake.Ticker.return_value.fast_info = _FakeFast(500.0, 499.0)
    with mock.patch.dict("sys.modules", {"yfinance": fake}):
        row = _scrape_index_digest("QQQ", sess)
    assert row is not None
    assert row["source"] == "yfinance"
    assert row["ticker"] == "QQQ"
    assert sess.get.call_count == 2  # elite + free


def test_scrape_keeps_finviz_when_page_works() -> None:
    html = """
    <table><tr>
      <td class="snapshot-td2">Daily Digest</td>
      <td>Jackson Hole: markets wait on Powell.</td>
    </tr></table>
    """
    sess = mock.Mock()
    resp = mock.Mock()
    resp.raise_for_status.return_value = None
    resp.text = html
    sess.get.return_value = resp
    row = _scrape_index_digest("DIA", sess)
    assert row["source"] == "finviz_quote"
    assert "Jackson Hole" in row["digest"]
    assert sess.get.call_count == 1


def main() -> None:
    tests = [
        test_yf_index_digest,
        test_scrape_falls_to_yfinance_on_403,
        test_scrape_keeps_finviz_when_page_works,
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
