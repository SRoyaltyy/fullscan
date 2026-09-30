"""Day-parse trainer: chips read repo news; jobs stay code-first."""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from .jev_day import (
    DAY_CAP,
    list_parse_days,
    load_day_rows,
    run_day_draw,
    write_days_index,
)
from .jev_gate import ROOT

EARNINGS = "Acme beats earnings estimates as revenue jumps"
TAPE = "Gold falls as traders price another Fed hike"


def _write(path: Path, blob) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(blob), encoding="utf-8")


def test_list_parse_days_flags_parsed_digest_judge() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        news = Path(tmp) / "news"
        _write(news / "2026-09-29_parsed.json", {"all_items": []})
        _write(news / "2026-09-28_finviz_digest.json", {})
        _write(news / "2026-09-27_judge.json", {})
        by_day = {row["date"]: row for row in list_parse_days(news)}
        assert by_day["2026-09-29"]["parsed"] is True
        assert by_day["2026-09-28"]["digest"] is True
        assert by_day["2026-09-27"]["judge"] is True


def test_load_day_rows_reads_parsed_then_digest() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        news = Path(tmp) / "news"
        _write(news / "2026-09-29_parsed.json", {
            "all_items": [
                {
                    "title": EARNINGS,
                    "source": "reuters",
                    "url": "https://example.com/a",
                    "published_at": "2026-09-29T12:00:00Z",
                },
                {"title": TAPE, "source": "bloomberg"},
                {"title": EARNINGS, "source": "dup"},
            ]
        })
        rows, kind = load_day_rows("2026-09-29", news_dir=news)
        assert kind == "parsed"
        assert [row["title"] for row in rows] == [EARNINGS, TAPE]
        assert rows[0]["date"] == "2026-09-29"
        assert rows[0]["pool"] == "parsed"
        assert rows[0]["source"] == "reuters"


def test_load_day_rows_falls_back_to_digest() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        news = Path(tmp) / "news"
        _write(news / "2026-09-28_finviz_digest.json", {
            "top_signal": [{"news_title": EARNINGS, "source": "finviz"}],
            "index_digests": [],
        })
        rows, kind = load_day_rows("2026-09-28", news_dir=news)
        assert kind == "digest"
        assert rows[0]["title"] == EARNINGS


def test_run_day_draw_is_code_first_and_needs_no_key() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        _write(news / "2026-09-29_parsed.json", {
            "all_items": [
                {"title": EARNINGS, "source": "reuters"},
                {"title": TAPE, "source": "bloomberg"},
            ]
        })
        closed = ROOT / "00_grounding" / "jev_closed_lists.json"
        if closed.exists():
            dest = root / "00_grounding"
            dest.mkdir(parents=True, exist_ok=True)
            dest.joinpath("jev_closed_lists.json").write_bytes(closed.read_bytes())
        report = run_day_draw(
            day="2026-09-29", stamp="20260929_0900", write=False, root=root,
        )
        assert report["day"] == "2026-09-29"
        assert report["day_kind"] == "parsed"
        assert report["gate"] == "hop0-code-bits"
        assert len(report["items"]) == 2
        for item in report["items"]:
            assert item["jev"] in {"KEEP", "DROP"}
            assert isinstance(item["bits"], list)
        assert not (root / "keep.json").exists()
        assert not (root / "dashboard" / "jev-train" / "draw.json").exists()


def test_write_days_index_and_cap() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        items = [{"title": f"Headline {i} about a market move"} for i in range(DAY_CAP + 20)]
        _write(news / "2026-09-29_parsed.json", {"all_items": items})
        path = write_days_index(root=root, news_dir=news)
        blob = json.loads(path.read_text(encoding="utf-8"))
        assert blob["schema"] == "jev-train-days-1"
        assert blob["days"][0]["date"] == "2026-09-29"
        rows, _kind = load_day_rows("2026-09-29", news_dir=news)
        assert len(rows) == DAY_CAP


def test_page_day_script_parses_repo_news() -> None:
    node = shutil.which("node")
    if not node:
        return
    script = r"""
const api = require("./dashboard/jev-train/day.js");
if (api.DAY_CAP < 400) throw new Error("day cap too small for a parse day");
const urls = api.newsUrls("2026-09-29", {parsed: true, digest: false, judge: false});
if (!urls.some((u) => u.indexOf("01_daily/news/2026-09-29_parsed.json") !== -1)) {
  throw new Error("missing parsed url");
}
if (!urls.some((u) => u.indexOf("../../01_daily/news/2026-09-29_parsed.json") !== -1)) {
  throw new Error("missing local parsed url");
}
const rows = api.rowsFromNews({
  all_items: [
    {title: "Acme beats earnings estimates as revenue jumps", source: "reuters"},
    {title: "Acme beats earnings estimates as revenue jumps", source: "dup"},
    {title: "Gold falls as traders price another Fed hike", source: "bloomberg"}
  ]
}, "2026-09-29", "parsed", 10);
if (rows.length !== 2) throw new Error("dedupe");
if (rows[0].date !== "2026-09-29") throw new Error("date");
if (rows[0].jev) throw new Error("preview must not invent Jev");
if (rows[0].reason) throw new Error("preview must not invent reason");
const draw = api.previewDraw("2026-09-29", rows, "parsed", 2);
if (!draw.preview) throw new Error("preview flag");
if (JSON.stringify(api).indexOf("JEV_API_KEY") !== -1) throw new Error("key");
"""
    subprocess.check_call([node, "-e", script], cwd=str(ROOT))


def main() -> None:
    tests = [
        test_list_parse_days_flags_parsed_digest_judge,
        test_load_day_rows_reads_parsed_then_digest,
        test_load_day_rows_falls_back_to_digest,
        test_run_day_draw_is_code_first_and_needs_no_key,
        test_write_days_index_and_cap,
        test_page_day_script_parses_repo_news,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
