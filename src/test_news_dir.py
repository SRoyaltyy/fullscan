"""News directory: term index dates and JEV verdict walk."""
from __future__ import annotations

import json
from pathlib import Path

from .news_dir import build, published_day, write
from .news_dir_jev import load_titles, publish_keep_file, score_rows, walk_verdicts, write_scores


def test_published_day_reads_rfc_and_iso() -> None:
    assert published_day("Wed, 30 Sep 2026 19:29:00 GMT") == "2026-09-30"
    assert published_day("2026-10-01T12:00:00Z") == "2026-10-01"
    assert published_day("Sun, 9 Aug 2026") == "2026-08-09"
    assert published_day("") == ""


def test_build_writes_iso_published_and_days(tmp_path: Path) -> None:
    news = tmp_path / "01_daily" / "news"
    news.mkdir(parents=True)
    (news / "2026-10-01_parsed.json").write_text(json.dumps({
        "all_items": [
            {
                "title": "Micron reports record earnings",
                "source": "reuters",
                "url": "https://example.com/mu",
                "published_at": "Wed, 01 Oct 2026 12:00:00 GMT",
                "ticker": "MU",
            },
            {
                "title": "Gold falls as traders price another Fed hike",
                "source": "bloomberg",
                "published_at": "2026-09-30T08:00:00Z",
            },
        ]
    }), encoding="utf-8")
    doc = build(tmp_path)
    assert doc["n"] == 2
    assert {row["published"] for row in doc["rows"]} == {"2026-10-01", "2026-09-30"}
    assert any("MU" in row["tick"] for row in doc["rows"])
    days = write(tmp_path)
    assert days["n"] == 2
    assert days["days"] == [{"date": "2026-10-01", "n": 2}]
    idx = json.loads((tmp_path / "dashboard" / "news-dir" / "index.json").read_text())
    assert idx["n"] == 2


def test_walk_verdicts_reads_drops_and_keeps() -> None:
    found = walk_verdicts({
        "keeps": [{"title": "Apple ordered to pay", "decision": "keep", "reason": "done"}],
        "drops": [{"title": "Should you buy Sandisk", "decision": "drop", "reason": "tip"}],
        "dropped": [{"title": "Odds of a hike", "jev": "DROP", "why": "preview"}],
    })
    titles = {it["title"]: it["decision"] for it in found}
    assert titles["Apple ordered to pay"] == "KEEP"
    assert titles["Should you buy Sandisk"] == "DROP"
    assert titles["Odds of a hike"] == "DROP"


def test_publish_keep_file_and_score_stamp(tmp_path: Path) -> None:
    src = tmp_path / "keep.json"
    src.write_text(json.dumps({
        "keeps": [{"title": "Fed holds rates", "decision": "keep", "reason": "print"}],
        "drops": [{"title": "Stock market today", "decision": "drop", "reason": "tape"}],
    }), encoding="utf-8")
    dest = tmp_path / "scores" / "2026-09-28.json"
    doc = publish_keep_file(src, dest, "2026-09-28")
    assert doc["n"] == 2
    assert {it["decision"] for it in doc["items"]} == {"KEEP", "DROP"}
    written = write_scores(
        {"stamp": "2026-10-01-eval", "items": doc["items"], "n": 2},
        "2026-10-01-eval",
        dest_dir=tmp_path / "scores",
    )
    assert written.name == "2026-10-01-eval.json"
    assert (tmp_path / "scores" / "latest.json").is_file()


def test_write_scores_updates_manifest(tmp_path: Path) -> None:
    dest = tmp_path / "scores"
    write_scores({"stamp": "2026-09-28", "items": [], "n": 0}, "2026-09-28", dest_dir=dest)
    write_scores({"stamp": "2026-10-01-eval", "items": [], "n": 0}, "2026-10-01-eval", dest_dir=dest)
    manifest = json.loads((dest / "index.json").read_text(encoding="utf-8"))
    assert manifest["stamps"] == ["2026-09-28", "2026-10-01-eval"]
    assert (dest / "latest.json").is_file()


def test_score_rows_requires_key() -> None:
    import os
    old = {key: os.environ.pop(key, None) for key in ("JEV_API_KEY", "TYPESAFE_API_KEY")}
    try:
        try:
            score_rows([{"title": "Micron reports record earnings"}], stamp="2026-10-01-eval", live=True)
        except RuntimeError as exc:
            assert "API_KEY" in str(exc)
        else:
            raise AssertionError("score_rows should refuse a live call without a key")
    finally:
        for key, value in old.items():
            if value is not None:
                os.environ[key] = value


def test_term_and_published_scores_split() -> None:
    from .news_dir import collect_rows

    root = Path(__file__).resolve().parents[1]
    rows = [row for row in collect_rows(root) if row.get("parse") == "2026-09-28"]
    scores = json.loads((root / "dashboard" / "news-dir" / "scores" / "2026-09-28.json").read_text(encoding="utf-8"))
    def hay(row: dict) -> str:
        return (row["title"] + " " + row.get("source", "") + " " + row.get("tick", "")).lower()

    apple = [row for row in rows if "apple" in hay(row)]
    gold = [row for row in rows if "gold" in hay(row)]
    assert apple, "term search should find Apple headlines"
    assert gold, "term search should find Gold headlines"
    assert any("Taptic Engine" in row["title"] for row in apple)

    def key(title: str) -> str:
        return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in title).split())

    by_title = {key(it["title"]): it["decision"] for it in scores["items"]}
    keeps = [row for row in rows if by_title.get(key(row["title"])) == "KEEP"]
    drops = [row for row in rows if by_title.get(key(row["title"])) == "DROP"]
    assert any("Taptic Engine" in row["title"] for row in keeps)
    assert any("Gold falls more than 2%" in row["title"] for row in keeps)
    assert drops
    assert not any(row in keeps for row in drops)


def test_load_titles_caps_and_dedupes() -> None:
    rows = load_titles({
        "rows": [
            {"title": "Micron reports record earnings", "source": "r"},
            {"title": "Micron reports record earnings", "source": "dup"},
            "Gold falls as traders price another Fed hike",
        ]
    })
    assert [r["title"] for r in rows] == [
        "Micron reports record earnings",
        "Gold falls as traders price another Fed hike",
    ]


def main() -> None:
    import tempfile
    tests = [
        test_published_day_reads_rfc_and_iso,
        test_walk_verdicts_reads_drops_and_keeps,
        test_score_rows_requires_key,
        test_term_and_published_scores_split,
        test_load_titles_caps_and_dedupes,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    with tempfile.TemporaryDirectory() as tmp:
        try:
            test_build_writes_iso_published_and_days(Path(tmp))
            print("ok test_build_writes_iso_published_and_days")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL test_build", type(exc).__name__, exc)
        try:
            pub = Path(tmp) / "pub"
            pub.mkdir()
            test_publish_keep_file_and_score_stamp(pub)
            print("ok test_publish_keep_file_and_score_stamp")
            test_write_scores_updates_manifest(pub)
            print("ok test_write_scores_updates_manifest")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL test_publish", type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
