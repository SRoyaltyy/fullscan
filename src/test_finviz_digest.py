"""Finviz index quote Elite miss → yfinance fallback. No network.

Run: python -m src.test_finviz_digest
"""
from __future__ import annotations

from unittest import mock

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
    fake = mock.Mock()
    fake.Ticker.return_value.fast_info = _FakeFast(500.0, 499.0)
    with mock.patch("src.finviz_digest.finviz_session.get", return_value=None):
        with mock.patch.dict("sys.modules", {"yfinance": fake}):
            row = _scrape_index_digest("QQQ", sess)
    assert row is not None
    assert row["source"] == "yfinance"
    assert row["ticker"] == "QQQ"


def test_scrape_keeps_finviz_when_page_works() -> None:
    html = """
    <table><tr>
      <td class="snapshot-td2">Daily Digest</td>
      <td>Jackson Hole: markets wait on Powell.</td>
    </tr></table>
    """
    sess = mock.Mock()
    resp = mock.Mock()
    resp.text = html
    with mock.patch("src.finviz_digest.finviz_session.get", return_value=resp):
        row = _scrape_index_digest("DIA", sess)
    assert row["source"] == "finviz_quote"
    assert "Jackson Hole" in row["digest"]


def test_session_is_elite_helper() -> None:
    from src import finviz_digest
    with mock.patch("src.finviz_digest.finviz_session.session") as sess:
        sess.return_value = mock.Mock()
        finviz_digest._session()
        sess.assert_called_once()


def main() -> None:
    tests = [
        test_yf_index_digest,
        test_scrape_falls_to_yfinance_on_403,
        test_scrape_keeps_finviz_when_page_works,
        test_session_is_elite_helper,
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
