"""Morning tape falls back to the exact Theme Radar prior raw file. No network.

Run: python3 -m src.test_finviz_morning_tape
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from src.finviz_morning_tape import ensure_morning_export
from src.theme_radar_raw import raw_url

MORNING = "2026-11-05"
PRIOR = "2026-11-04"
OLDER = "2026-11-03"
HEADER = (
    "Ticker,Change,Volume,Average Volume,Market Cap,Industry,"
    "Earnings Date,News Title\n"
)
GOOD_STAMP = "2026-11-04T21:00:00+00:00"
OPEN_STAMP = "2026-11-05T14:30:00+00:00"


def _body(marker: str) -> bytes:
    line = (
        f"AAA,1.2%,1000,500,2000,Software,11/5/2026 8:00:00 AM,{marker}\n"
    )
    return (HEADER + line * 8).encode("utf-8")


def _manifest(stamp: str, day: str = PRIOR) -> dict:
    return {"runs": [{"date": day, "scrape_ts_utc": stamp}]}


def _plant(root: Path, day: str, body: bytes) -> None:
    path = root / "data" / "exports" / f"finviz_{day}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def test_fresh_own_fetch_does_not_read_theme_radar(tmp_path: Path) -> None:
    previous = _body("PREV")
    own = _body("OWN")
    _plant(tmp_path, PRIOR, previous)
    seen: list[str] = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        raise AssertionError(url)

    saved = ensure_morning_export(
        MORNING, own_bytes=own, fetch=fetch, root=tmp_path,
    )
    assert saved["fallback"] is False
    assert seen == []
    assert (tmp_path / "data" / "exports" / f"finviz_{MORNING}.csv").read_bytes() == own


def test_copy_and_failed_fetch_use_exact_prior_raw(tmp_path: Path) -> None:
    previous = _body("STUCK")
    _plant(tmp_path, PRIOR, previous)
    radar = _body("RADAR")
    seen: list[str] = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        assert url == raw_url(PRIOR), url
        assert url != raw_url(MORNING)
        assert url != raw_url(OLDER)
        return radar

    for own in (previous, None):
        seen.clear()
        dest = tmp_path / "data" / "exports" / f"finviz_{MORNING}.csv"
        if dest.exists():
            dest.unlink()
        saved = ensure_morning_export(
            MORNING, own_bytes=own, fetch=fetch,
            manifest=_manifest(GOOD_STAMP), root=tmp_path,
        )
        assert saved["fallback"] is True
        assert saved["source"] == "theme_radar_snapshot"
        assert saved["source_file"] == f"data/snapshots/{PRIOR}.raw.csv"
        assert saved["source_sha256"] == hashlib.sha256(radar).hexdigest()
        assert seen == [raw_url(PRIOR)]
        text = dest.read_text(encoding="utf-8")
        assert text.count("theme_radar_snapshot") == 8
        assert text.count(saved["source_sha256"]) == 8
        assert f"data/snapshots/{PRIOR}.raw.csv" in text
        side = dest.with_name(dest.stem + ".source.json").read_text(encoding="utf-8")
        assert saved["source_sha256"] in side
        assert GOOD_STAMP in side


def test_missing_exact_file_stops_and_does_not_ask_for_an_older_day(tmp_path: Path) -> None:
    seen: list[str] = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        if url == raw_url(OLDER):
            return _body("OLDER")
        raise FileNotFoundError(url)

    try:
        ensure_morning_export(
            MORNING, own_bytes=None, fetch=fetch,
            manifest=_manifest(GOOD_STAMP), root=tmp_path,
        )
    except SystemExit as exc:
        text = str(exc)
        assert "FAIL" in text
        assert f"data/snapshots/{PRIOR}.raw.csv" in text
        assert "older snapshot" in text
    else:
        raise AssertionError("a missing D-1 file was allowed to run")
    assert seen == [raw_url(PRIOR)]
    assert not (tmp_path / "data" / "exports" / f"finviz_{MORNING}.csv").exists()


def test_scrape_at_or_after_the_open_is_refused(tmp_path: Path) -> None:
    seen: list[str] = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        return _body("LATE")

    try:
        ensure_morning_export(
            MORNING, own_bytes=None, fetch=fetch,
            manifest=_manifest(OPEN_STAMP), root=tmp_path,
        )
    except SystemExit as exc:
        text = str(exc)
        assert "09:30" in text
        assert f"data/snapshots/{PRIOR}.raw.csv" in text
        assert "older snapshot" in text
    else:
        raise AssertionError("a snapshot at 09:30 ET was accepted")
    assert seen == []


def main() -> None:
    import tempfile
    tests = (
        test_fresh_own_fetch_does_not_read_theme_radar,
        test_copy_and_failed_fetch_use_exact_prior_raw,
        test_missing_exact_file_stops_and_does_not_ask_for_an_older_day,
        test_scrape_at_or_after_the_open_is_refused,
    )
    for test in tests:
        with tempfile.TemporaryDirectory() as tmp:
            test(Path(tmp))
    print("finviz morning tape ok")


if __name__ == "__main__":
    main()
