"""Jev trainer: grade math and draw exclusion. No live key.

stdlib only — workflow_selfcheck has no pip.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import random
import tempfile
from pathlib import Path
from unittest import mock

from src.jev_eval import gold_norms, jaccard, load_archive, title_id, tokens
from src.jev_gate import gate, load_gold, normalize_title
from src.jev_rubric import (
    propose_one_change,
    rubric_path,
    upsert_rubric,
)
from src.jev_train import (
    BIT_ORDER,
    COMMIT_ALLOW,
    MIN_MARKS,
    append_hard_misses,
    commit_allowed,
    count_marks,
    ensure_pool_files,
    hard_miss_path,
    holdout_path,
    issue_body,
    blank_disagreements,
    hydrate_grades,
    parse_grades_payload,
    propose_hard_miss_bank,
    publish_issue,
    read_holdout_items,
    run_draw,
    score_grades,
    select_draw,
    take_rotating,
    trained_ids,
    why_bits,
    write_grade,
)

ROOT = Path(__file__).resolve().parent.parent
NOW = dt.datetime(2026, 9, 29, 1, 30, tzinfo=dt.timezone.utc)


def _row(title: str, day: str, source: str = "reuters") -> dict:
    return {
        "id": title_id(title),
        "title": title,
        "source": source,
        "published_at": f"{day}T12:00:00Z",
        "url": "",
        "date": day,
        "archive": "parsed",
    }


def _distinct_archive() -> list[dict]:
    pairs = [
        ("2026-09-01", "Copper mine in Chile raises output after a new shaft opens"),
        ("2026-09-02", "Japan cabinet approves a supplemental budget for chip tools"),
        ("2026-09-03", "Rotterdam port labor talks stay on the calendar"),
        ("2026-09-04", "Brazil soy exports clear the Santos loading queue"),
        ("2026-09-05", "Indian refiners book extra October crude cargoes"),
        ("2026-09-06", "Korean battery plant adds a second line in Georgia"),
        ("2026-09-01", "Widget opinion column says nothing happened today"),
        ("2026-09-07", "Norway salmon farms report a quiet week on volumes"),
    ]
    return [_row(title, day) for day, title in pairs]


def _rss(*titles: str) -> list[dict]:
    return [_row(title, "2026-09-29", source="rss_google_markets") | {"archive": "rss", "query": "q"}
            for title in titles]


def test_why_bits_follow_the_current_gate():
    rows = [
        _row("Acme beats earnings estimates as revenue jumps", "2026-09-28"),
        _row("Gold falls as traders price another Fed hike", "2026-09-28"),
        _row("August Core PCE print lands ahead of the Fed decision", "2026-09-28"),
        _row("Trump bans oil exports under an emergency order", "2026-09-28"),
        _row("Federal Register final rule sets a new CAFE standard", "2026-09-28"),
        _row("Refinery explosion shuts the Houston plant overnight", "2026-09-28"),
    ]
    decided = gate(rows, code_only=True, live=False, asof=dt.date(2026, 9, 28))
    by_title = {item["title"]: item for item in decided}
    for row in rows:
        assert by_title[row["title"]]["decision"] in {"keep", "drop"}
    earnings = why_bits(rows[0]["title"], by_title[rows[0]["title"]])
    tape = why_bits(rows[1]["title"], by_title[rows[1]["title"]])
    printed = why_bits(rows[2]["title"], by_title[rows[2]["title"]])
    lever = why_bits(rows[3]["title"], by_title[rows[3]["title"]])
    signed = why_bits(rows[4]["title"], by_title[rows[4]["title"]])
    blast = why_bits(rows[5]["title"], by_title[rows[5]["title"]])
    assert "earnings" in earnings
    assert "tape" in tape
    assert "print" in printed
    assert "lever" in lever and "actor" in lever
    assert "signed" in signed
    assert "blast" in blast
    assert why_bits("A column", {"reason": "opinion", "geo": "other"})[0] == "opinion"
    for bits in (earnings, tape, printed, lever, signed, blast):
        assert list(bits) == [name for name in BIT_ORDER if name in bits]


def test_false_keep_and_false_drop_record_bits():
    tape_title = "Gold falls as traders price another Fed hike"
    print_title = "August Core PCE print lands ahead of the Fed decision"
    rows = [
        {
            "id": title_id(print_title),
            "title": print_title,
            "source": "reuters",
            "pool": "parsed",
            "jev": "KEEP",
            "reason": "code_print",
            "geo": "",
            "actor_power": "",
            "new_instrument": 0,
            "grade": "D",
            "note": "too thin",
        },
        {
            "id": title_id(tape_title),
            "title": tape_title,
            "source": "reuters",
            "pool": "rss",
            "jev": "DROP",
            "reason": "tape",
            "geo": "",
            "actor_power": "",
            "new_instrument": 0,
            "grade": "K",
            "note": "real move",
        },
        {
            "id": title_id("Norway salmon farms report a quiet week on volumes"),
            "title": "Norway salmon farms report a quiet week on volumes",
            "source": "reuters",
            "pool": "holdout",
            "jev": "DROP",
            "reason": "geo_other",
            "geo": "other",
            "actor_power": "other_person",
            "new_instrument": 0,
            "grade": "?",
            "note": "",
        },
        {
            "id": title_id("Brazil soy exports clear the Santos loading queue"),
            "title": "Brazil soy exports clear the Santos loading queue",
            "source": "reuters",
            "pool": "parsed",
            "jev": "DROP",
            "reason": "low_material",
            "geo": "other",
            "actor_power": "",
            "new_instrument": 0,
            "grade": "D",
            "note": "",
        },
    ]
    scored = score_grades(rows)
    assert scored["counts"]["false_keep"] == 1
    assert scored["counts"]["false_drop"] == 1
    assert scored["counts"]["unsure"] == 1
    assert scored["false_keep"][0]["title"] == print_title
    assert "print" in scored["false_keep"][0]["bits"]
    assert scored["bits_on_misses"]["false_keep"]["print"] == 1
    assert scored["false_drop"][0]["title"] == tape_title
    assert "tape" in scored["false_drop"][0]["bits"]
    assert scored["bits_on_misses"]["false_drop"]["tape"] >= 1
    assert all(row["title"] != rows[2]["title"] for row in scored["false_keep"] + scored["false_drop"])


def test_grade_files_need_thirty_marks_and_do_not_touch_closed_lists():
    rows = []
    for i in range(29):
        title = f"Quiet desk note {i} about municipal parking in city {i}"
        rows.append({
            "title": title,
            "source": "reuters",
            "pool": "parsed",
            "jev": "DROP",
            "reason": "low_material",
            "geo": "other",
            "grade": "D",
            "note": "",
        })
    payload = {"draw_stamp": "20260929_0100", "nonce": "n-29", "rows": rows}
    try:
        write_grade(parse_grades_payload(json.dumps({
            "schema": "jev-train-grades-1",
            **payload,
        })), stamp="20260929_0130", now=NOW, write=False, minimum=MIN_MARKS)
    except RuntimeError as exc:
        assert "30" in str(exc)
    else:
        raise AssertionError("expected a minimum-mark error")

    title = "August Core PCE print lands ahead of the Fed decision"
    rows.append({
        "title": title,
        "source": "reuters",
        "pool": "holdout",
        "jev": "KEEP",
        "reason": "code_print",
        "geo": "core",
        "grade": "D",
        "note": "miss",
    })
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0100",
        "nonce": "abc-nonce",
        "rows": rows,
    }))
    assert count_marks(parsed["rows"]) == 30
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ground = root / "00_grounding"
        ground.mkdir()
        closed = ground / "jev_closed_lists.json"
        closed.write_text('{"shapes":["do-not-touch"]}\n', encoding="utf-8")
        before = closed.read_bytes()
        hold = holdout_path(ground)
        hold.write_text('{"ids":["frozen"],"items":[{"id":"frozen","title":"Frozen headline stays"}]}\n',
                        encoding="utf-8")
        hold_before = hold.read_bytes()
        result = write_grade(
            parsed, stamp="20260929_0130", now=NOW, root=root, ground=ground, write=True,
        )
        assert result["stamp"] == "20260929_0130"
        session_path = ground / "jev_train" / "20260929_0130.json"
        grade_path = ground / "jev_train" / "20260929_0130_grade.json"
        md_path = ground / "jev_train" / "20260929_0130.md"
        assert session_path.is_file() and grade_path.is_file() and md_path.is_file()
        session = json.loads(session_path.read_text(encoding="utf-8"))
        grade = json.loads(grade_path.read_text(encoding="utf-8"))
        assert session["counts"]["false_keep"] == 1
        assert grade["false_keep"][0]["id"] == title_id(title)
        assert "print" in grade["false_keep"][0]["bits"]
        assert "<table" not in md_path.read_text(encoding="utf-8")
        md_text = md_path.read_text(encoding="utf-8")
        assert "| # | jev | you | bits | source | title | note |" in md_text
        assert "miss" in md_text
        assert closed.read_bytes() == before
        assert hold.read_bytes() == hold_before
        hard = json.loads(hard_miss_path(ground).read_text(encoding="utf-8"))
        assert title_id(title) in {item["id"] for item in hard["items"]}
        body = result["issue_body"]
        assert "Paste this to Grok to discuss." in body
        assert "20260929_0130.json" in body
        assert "false keep" in body.lower() or "False keep" in body
        assert "nonce: abc-nonce" in body
        assert grade["counts"]["human_drop"] == 30
        assert grade["counts"]["human_keep"] == 0


def test_trained_hash_and_gold_are_excluded_from_every_pool():
    archive = _distinct_archive()
    gold_title = "Widget opinion column says nothing happened today"
    trained_title = "Korean battery plant adds a second line in Georgia"
    gold = {normalize_title(gold_title)}
    banned = {title_id(trained_title)}
    # The hash can live alone in a markdown sheet, without the title text.
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        (directory / "20260928_0900.md").write_text(
            f"already graded {title_id(trained_title)}\n", encoding="utf-8",
        )
        (directory / "note.json").write_text(
            json.dumps({"items": [{"title": gold_title}]}), encoding="utf-8",
        )
        found = trained_ids(directory)
    assert title_id(trained_title) in found
    assert title_id(gold_title) in found

    holdout = [
        _row("August Core PCE print lands ahead of the Fed decision", "2026-08-01"),
        _row("Acme beats earnings estimates as revenue jumps", "2026-08-02"),
    ]
    hard = [
        _row("Copper mine in Chile raises output after a new shaft opens", "2026-09-01"),
        _row("A fresh hard miss about shipping insurance rates", "2026-09-20", source="hard"),
        _row("Another hard miss about grid interconnect queues", "2026-09-21", source="hard"),
    ]
    rss = _rss(
        "Airline pilots vote on a new contract in Dallas",
        "Airline pilots vote on a new contract in Dallas today",
        "Lumber futures settle quietly in Chicago",
        "A city council renames a park in Tucson",
        "Solar inverter lead times stretch in Arizona",
    )
    assert jaccard(tokens(rss[0]["title"]), tokens(rss[1]["title"])) >= 0.72
    sample = select_draw(
        archive=archive,
        rss_rows=rss,
        holdout_items=holdout,
        hard_items=hard,
        hard_cursor=0,
        gold=gold,
        banned_ids=banned,
        rng=random.Random(1),
        parsed_n=4,
        rss_n=3,
        exam_n=2,
    )
    picked = sample["parsed"] + sample["rss"] + sample["exam"]
    titles = [row["title"] for row in picked]
    assert gold_title not in titles
    assert trained_title not in titles
    assert sample["exam_source"] == "holdout"
    assert sample["hard_cursor"] == 0
    assert [row["title"] for row in sample["exam"]] == [row["title"] for row in holdout]
    assert "Copper mine in Chile raises output after a new shaft opens" not in [
        row["title"] for row in sample["parsed"]
    ]
    rss_titles = [row["title"] for row in sample["rss"]]
    assert not (
        "Airline pilots vote on a new contract in Dallas" in rss_titles
        and "Airline pilots vote on a new contract in Dallas today" in rss_titles
    )
    assert sample["gold_excluded"] >= 1
    assert sample["trained_excluded"] >= 1
    ids = [row["id"] for row in picked]
    assert len(ids) == len(set(ids)) == 9


def test_hard_misses_rotate_when_holdout_is_used_up():
    gold: set[str] = set()
    banned = {title_id("August Core PCE print lands ahead of the Fed decision")}
    holdout = [
        _row("August Core PCE print lands ahead of the Fed decision", "2026-08-01"),
    ]
    hard_titles = [
        "Shipping insurance rates jump after a hull loss",
        "Grid interconnect queues lengthen in west Texas",
        "Cocoa grindings disappoint in Europe this quarter",
        "Railroad dwell times ease at the Chicago yards",
        "Lithium brine output slows on the Atacama flats",
    ]
    hard = [_row(title, f"2026-09-{10+i:02d}") for i, title in enumerate(hard_titles)]
    archive_titles = [
        ("2026-09-01", "Copper mine in Chile raises output after a new shaft opens"),
        ("2026-09-02", "Japan cabinet approves a supplemental budget for chip tools"),
        ("2026-09-03", "Rotterdam port labor talks stay on the calendar"),
        ("2026-09-04", "Brazil soy exports clear the Santos loading queue"),
        ("2026-09-05", "Indian refiners book extra October crude cargoes"),
        ("2026-09-06", "Norway salmon farms report a quiet week on volumes"),
        ("2026-09-07", "Korean battery plant adds a second line in Georgia"),
        ("2026-09-08", "Mexico auto plants schedule a maintenance Sunday"),
    ]
    archive = [_row(title, day) for day, title in archive_titles]
    rss = _rss(
        "First wire about cobalt shipments from the Congo",
        "Second wire about canola crush margins",
        "Third wire about container dwell in Savannah",
    )
    first = select_draw(
        archive=archive, rss_rows=rss, holdout_items=holdout, hard_items=hard,
        hard_cursor=0, gold=gold, banned_ids=banned, rng=random.Random(2),
        parsed_n=3, rss_n=2, exam_n=2,
    )
    assert first["exam_source"] == "hard_miss"
    assert [row["title"] for row in first["exam"]] == [hard[0]["title"], hard[1]["title"]]
    assert first["hard_cursor"] == 2
    second = select_draw(
        archive=archive, rss_rows=rss, holdout_items=holdout, hard_items=hard,
        hard_cursor=first["hard_cursor"], gold=gold, banned_ids=banned,
        rng=random.Random(2), parsed_n=3, rss_n=2, exam_n=2,
    )
    assert [row["title"] for row in second["exam"]] == [hard[2]["title"], hard[3]["title"]]
    assert second["hard_cursor"] == 4
    # A trained hash inside the bank is skipped, and the cursor walks past it.
    banned2 = {hard[0]["id"]}
    skipped, cursor = take_rotating(hard, 2, 0, gold, banned2)
    assert hard[0]["id"] not in {row["id"] for row in skipped}
    assert [row["id"] for row in skipped] == [hard[1]["id"], hard[2]["id"]]
    assert cursor == 3


def test_draw_writes_page_json_and_leaves_holdout_bytes_alone():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        news.mkdir(parents=True)
        by_day: dict[str, list] = {}
        for row in _distinct_archive():
            if "Widget opinion" in row["title"] or "Korean battery" in row["title"]:
                continue
            by_day.setdefault(row["date"], []).append({
                "title": row["title"], "source": row["source"],
                "published_at": row["published_at"], "url": "",
            })
        for day, items in by_day.items():
            (news / f"{day}_parsed.json").write_text(
                json.dumps({"all_items": items}), encoding="utf-8",
            )
        ground = root / "00_grounding"
        ground.mkdir()
        hold_items = [
            _row("August Core PCE print lands ahead of the Fed decision", "2026-08-01"),
            _row("Refinery explosion shuts the Houston plant overnight", "2026-08-02"),
        ]
        hold = {
            "created_at": "2026-09-28T00:00:00+00:00",
            "algo": "sha256-norm-16",
            "note": "frozen",
            "ids": [row["id"] for row in hold_items],
            "items": hold_items,
        }
        hold_path = holdout_path(ground)
        hold_path.write_text(json.dumps(hold), encoding="utf-8")
        hold_bytes = hold_path.read_bytes()
        trained = ground / "jev_train"
        trained.mkdir()
        (trained / "old.json").write_text(
            json.dumps({"items": [{"id": title_id("Brazil soy exports clear the Santos loading queue"),
                                   "title": "Brazil soy exports clear the Santos loading queue"}]}),
            encoding="utf-8",
        )
        closed = ground / "jev_closed_lists.json"
        closed.write_text("{}\n", encoding="utf-8")
        closed_bytes = closed.read_bytes()
        rss = _rss(
            "Airline pilots vote on a new contract in Dallas",
            "Lumber futures settle quietly in Chicago",
            "Solar inverter lead times stretch in Arizona",
            "A city council renames a park in Tucson",
        )
        report = run_draw(
            live=False, seed=7, stamp="20260929_0130", rss_rows=rss, now=NOW,
            root=root, ground=ground, news_dir=news, write=True,
            gold=set(), parsed_n=3, rss_n=3, exam_n=2,
        )
        assert report["exam_source"] == "holdout"
        assert hold_path.read_bytes() == hold_bytes
        assert closed.read_bytes() == closed_bytes
        page = json.loads((root / "dashboard" / "jev-train" / "draw.json").read_text(encoding="utf-8"))
        archived = json.loads((trained / "20260929_0130_draw.json").read_text(encoding="utf-8"))
        assert page["stamp"] == archived["stamp"] == "20260929_0130"
        assert len(page["items"]) == 8
        titles = [item["title"] for item in page["items"]]
        assert "Brazil soy exports clear the Santos loading queue" not in titles
        assert "August Core PCE print lands ahead of the Fed decision" in titles
        pce = next(item for item in page["items"] if "PCE" in item["title"])
        assert pce["jev"] == "KEEP"
        assert "print" in pce["bits"]
        blast = next(item for item in page["items"] if "explosion" in item["title"])
        assert "blast" in blast["bits"]
        assert not (root / "keep.json").exists()
        assert not list(ground.glob("keep.json"))


def test_ensure_pool_files_does_not_rewrite_existing_holdout():
    with tempfile.TemporaryDirectory() as tmp:
        ground = Path(tmp)
        hold = holdout_path(ground)
        hold.parent.mkdir(parents=True, exist_ok=True)
        payload = b'{"ids":["abc"],"items":[{"title":"Stay"}]}\n'
        hold.write_bytes(payload)
        ensure_pool_files(ground)
        assert hold.read_bytes() == payload
        assert hard_miss_path(ground).is_file()
        ensure_pool_files(ground)
        assert hold.read_bytes() == payload


def test_grades_payload_accepts_base64_and_rejects_secrets():
    title = "Norway salmon farms report a quiet week on volumes"
    blob = {
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0130",
        "nonce": "n",
        "rows": [{
            "title": title,
            "jev": "drop",
            "grade": "d",
            "note": "ok",
            "source": "reuters",
        }],
    }
    raw = base64.b64encode(json.dumps(blob).encode("utf-8")).decode("ascii")
    parsed = parse_grades_payload(raw)
    assert parsed["rows"][0]["id"] == title_id(title)
    assert parsed["rows"][0]["grade"] == "D"
    assert parsed["rows"][0]["jev"] == "DROP"
    blob["rows"][0]["note"] = "ghp_abcdefghijklmnop"
    try:
        parse_grades_payload(json.dumps(blob))
    except RuntimeError as exc:
        assert "secret" in str(exc).lower()
    else:
        raise AssertionError("expected secret rejection")


def test_publish_issue_opens_or_updates_the_titled_issue():
    session = {
        "stamp": "20260929_0130",
        "draw_stamp": "20260929_0100",
        "nonce": "nonce-1",
        "json_url": "https://github.com/SRoyaltyy/fullscan/blob/main/00_grounding/jev_train/20260929_0130.json",
        "counts": {"human_keep": 10, "human_drop": 20, "jev_keep": 4, "jev_drop": 26,
                   "false_keep": 1, "false_drop": 2, "unsure": 0},
    }
    grade = {
        "counts": session["counts"],
        "false_keep": [],
        "false_drop": [],
        "bits_on_misses": {"false_keep": {}, "false_drop": {}},
        "grade_url": "https://github.com/SRoyaltyy/fullscan/blob/main/00_grounding/jev_train/20260929_0130_grade.json",
    }
    calls = []

    def fake(method, url, token, body=None):
        calls.append((method, url, body))
        if method == "GET":
            return 200, []
        if url.endswith("/issues"):
            return 201, {"html_url": "https://github.com/SRoyaltyy/fullscan/issues/5150", "number": 5150}
        return 201, {}

    with mock.patch("src.jev_train._github_json", side_effect=fake):
        url = publish_issue(session, grade, token="actions-token", repo="SRoyaltyy/fullscan")
    assert url == "https://github.com/SRoyaltyy/fullscan/issues/5150"
    created = [c for c in calls if c[0] == "POST" and c[1].endswith("/issues")]
    assert created[0][2]["title"] == "jev-train 20260929_0130"
    assert created[0][2]["labels"] == ["jev-train"]
    assert "Paste this to Grok to discuss." in created[0][2]["body"]
    assert "nonce: nonce-1" in created[0][2]["body"]
    labels = [c for c in calls if c[1].endswith("/labels")]
    assert labels[0][2]["name"] == "jev-train"

    def fake_update(method, url, token, body=None):
        calls.append((method, url, body))
        if method == "GET":
            return 200, [{"number": 9, "title": "jev-train 20260929_0130",
                          "html_url": "https://github.com/SRoyaltyy/fullscan/issues/9"}]
        if method == "PATCH":
            return 200, {"html_url": "https://github.com/SRoyaltyy/fullscan/issues/9"}
        return 201, {}

    calls.clear()
    with mock.patch("src.jev_train._github_json", side_effect=fake_update):
        url = publish_issue(session, grade, token="actions-token", repo="SRoyaltyy/fullscan")
    assert url.endswith("/issues/9")
    assert any(c[0] == "PATCH" and c[1].endswith("/issues/9") for c in calls)


def test_issue_body_has_counts_and_json_link():
    session = {"stamp": "20260929_0130", "draw_stamp": "20260929_0100", "nonce": "z",
               "counts": {"human_keep": 3, "human_drop": 27, "jev_keep": 5, "jev_drop": 25,
                          "false_keep": 1, "false_drop": 0, "unsure": 1}}
    grade = {
        "counts": session["counts"],
        "false_keep": [{"bits": ["print"], "reason": "code_print", "title": "August Core PCE"}],
        "false_drop": [],
        "bits_on_misses": {"false_keep": {"print": 1}, "false_drop": {}},
    }
    body = issue_body(
        session, grade,
        json_url="https://github.com/SRoyaltyy/fullscan/blob/main/00_grounding/jev_train/20260929_0130.json",
        grade_url="https://github.com/SRoyaltyy/fullscan/blob/main/00_grounding/jev_train/20260929_0130_grade.json",
    )
    assert "| human | 3 | 27 |" in body
    assert "20260929_0130.json" in body
    assert "print" in body
    assert "## Rubric (one Jev-analysis change)" in body
    assert "closed lists" in body.lower() or "Closed lists" in body


def test_session_409_proposes_action_material():
    """Largest Jev miss bucket on #409 is low_material → action_material."""
    grade = {
        "stamp": "20260929_1223",
        "draw_stamp": "20260929_1120",
        "counts": {
            "human_keep": 25, "human_drop": 75, "jev_keep": 3, "jev_drop": 97,
            "false_keep": 0, "false_drop": 22,
        },
        "false_keep": [],
        "false_drop": [
            {"title": "Cyclospora fears", "human": "K", "reason": "opinion",
             "human_reason": "Health scare--could trigger recalls, FDA, policies etc"},
            {"title": "Anthropic IPO", "human": "K", "reason": "opinion",
             "human_reason": "Major upcoming IPO"},
            {"title": "OpenAI teens", "human": "K", "reason": "low_material",
             "human_reason": "Product launch from major AI company"},
            {"title": "CFTC rules", "human": "K", "reason": "low_material",
             "human_reason": "CFTC part worth looking into"},
            {"title": "Vanguard 4.6B", "human": "K", "reason": "low_material",
             "human_reason": "potential merger/acquisition"},
            {"title": "Basin rigs", "human": "K", "reason": "low_material",
             "human_reason": "useful context for oil prices"},
            {"title": "Fed holds rates", "human": "K", "reason": "low_material",
             "human_reason": "US Fed action"},
            {"title": "House CR", "human": "K", "reason": "low_material",
             "human_reason": "House of representatives action"},
            {"title": "UP-NS merger", "human": "K", "reason": "low_material",
             "human_reason": "Merger involving US company"},
            {"title": "EIA storage", "human": "K", "reason": "low_material",
             "human_reason": "Oil and petrol information"},
            {"title": "Wells Fargo", "human": "K", "reason": "low_material",
             "human_reason": "Major company charges dismissed"},
            {"title": "Chinese EVs", "human": "K", "reason": "low_material",
             "human_reason": "Potential EV policy from Trump"},
            {"title": "Azeri Light", "human": "K", "reason": "geo_other",
             "human_reason": "Useful oil price context"},
            {"title": "Nordic PE", "human": "K", "reason": "geo_other",
             "human_reason": "Company acquisition/merger"},
            {"title": "Hormuz reject", "human": "K", "reason": "reprint_weather",
             "human_reason": "explicit acceptance/rejection of peace deals"},
            {"title": "Tokyo yen", "human": "K", "reason": "crowd",
             "human_reason": "Japanese Yen is a factor in US markets"},
            {"title": "Social Security?", "human": "K", "reason": "punct",
             "human_reason": "Trump-linked government policy"},
            {"title": "Explainer CDS?", "human": "K", "reason": "punct",
             "human_reason": "Credit default swaps"},
            {"title": "simplywall tariffs", "human": "K", "reason": "source",
             "human_reason": "US China Tariffs"},
        ],
    }
    proposal = propose_one_change(grade)
    assert proposal["question"] == "action_material"
    assert proposal["kind"] == "criteria"
    assert proposal["n"] == 10
    assert "US Fed action" in proposal["text"]
    assert "new_instrument" in proposal["text"]
    assert any(row["reason"] == "punct" for row in proposal["code_notes"])
    living = upsert_rubric(
        Path("/tmp/does-not-write-rubric.md"), grade, proposal, write=False,
    )
    assert "## Session `20260929_1223`" in living
    assert "action_material" in living
    assert "jev_closed_lists.json" in living


def test_write_grade_updates_living_rubric():
    title = "August Core PCE print lands ahead of the Fed decision"
    tape = "Gold falls as traders price another Fed hike"
    rows = _quiet_rows(28)
    rows.extend([
        {
            "title": title,
            "source": "reuters",
            "jev": "DROP",
            "reason": "low_material",
            "grade": "K",
            "human_reason": "US Fed print — keep for Lane",
        },
        {
            "title": tape,
            "source": "reuters",
            "jev": "KEEP",
            "reason": "core_material",
            "grade": "D",
            "human_reason": "gold tape is trash",
        },
    ])
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0100",
        "nonce": "rubric-nonce",
        "rows": rows,
    }))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ground = root / "00_grounding"
        ground.mkdir()
        closed = ground / "jev_closed_lists.json"
        closed.write_text('{"shapes":["do-not-touch"]}\n', encoding="utf-8")
        before = closed.read_bytes()
        result = write_grade(
            parsed, stamp="20260929_0160", now=NOW, root=root, ground=ground, write=True,
        )
        living = Path(result["paths"]["rubric"])
        assert living == rubric_path(ground)
        text = living.read_text(encoding="utf-8")
        assert "## Session `20260929_0160`" in text
        assert "US Fed print — keep for Lane" in text
        assert result["rubric"]["question"] in {"action_material", "is_opinion", "geo"}
        assert "## Rubric (one Jev-analysis change)" in result["issue_body"]
        assert result["rubric"]["question"] in result["issue_body"]
        assert closed.read_bytes() == before
        again = write_grade(
            parsed, stamp="20260929_0161", now=NOW, root=root, ground=ground, write=True,
        )
        twice = Path(again["paths"]["rubric"]).read_text(encoding="utf-8")
        assert twice.count("## Session `20260929_0160`") == 1
        assert twice.count("## Session `20260929_0161`") == 1
        assert twice.count("## Pending one change") == 1


def test_commit_allowlist_and_workflow_and_page():
    assert commit_allowed("00_grounding/jev_train/20260929_0130.json")
    assert commit_allowed("00_grounding/jev_train/20260929_0130_grade.json")
    assert commit_allowed("00_grounding/jev_hard_misses.json")
    assert commit_allowed("dashboard/jev-train/draw.json")
    assert not commit_allowed("00_grounding/jev_closed_lists.json")
    assert not commit_allowed("00_grounding/jev_gold.json")
    assert not commit_allowed("00_grounding/jev_holdout.json")
    assert not commit_allowed("keep.json")
    assert not commit_allowed("src/factor_mine.py")
    assert not commit_allowed("src/jev_gate.py")
    yml = (ROOT / ".github" / "workflows" / "jev_train.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in yml
    assert "schedule:" not in yml
    assert "cron:" not in yml
    assert "pull_request:" not in yml
    assert "secrets.JEV_API_KEY" in yml
    assert "keep.json" not in yml
    for line in yml.splitlines():
        if "ghp_" in line or "github_pat_" in line:
            assert "grep" in line
    assert "jev_closed_lists" not in yml
    assert "src.factor_mine" not in yml
    for prefix in COMMIT_ALLOW:
        assert prefix in yml
    page = (ROOT / "dashboard" / "jev-train" / "index.html").read_text(encoding="utf-8")
    script = (ROOT / "dashboard" / "jev-train" / "app.js").read_text(encoding="utf-8")
    assert "<table" in page
    assert ">title<" in page or ">Title<" in page or "title</th>" in page
    assert "Jev" in page
    for column in ("title", "source", "bits", "reason"):
        assert column in page.lower()
    assert "MIN_MARKS = 30" in script
    assert "JEV_API_KEY" not in script
    assert "ghp_" not in script
    assert "github_pat_" not in script
    assert "secrets." not in script
    assert "localStorage" not in script
    assert "sessionStorage" not in script
    assert "workflow_dispatch" in script or "dispatches" in script
    assert "Paste this to Grok to discuss." in page
    deploy = (ROOT / ".github" / "workflows" / "deploy-dashboard.yml").read_text(encoding="utf-8")
    assert "dashboard/jev-train" in deploy
    hop0 = (ROOT / ".github" / "workflows" / "jev_hop0.yml").read_text(encoding="utf-8")
    assert "schedule:" in hop0 or "cron:" in hop0
    # This change must not have enabled the harvest by deleting its cron,
    # and the trainer workflow must not point at it.
    assert "jev_hop0" not in yml
    # 16:19 draw wrote the sheet then lost the push race with auto-lands.
    assert "git fetch origin main" in yml
    assert "commit-tree" in yml
    assert "replay trainer blobs onto origin/main" in yml
    assert "decision_ready" not in yml


def test_repo_holdout_and_hard_miss_bank_are_disjoint_from_gold():
    hold = read_holdout_items(ROOT / "00_grounding" / "jev_holdout.json")
    assert len(hold) == 20
    hard = json.loads((ROOT / "00_grounding" / "jev_hard_misses.json").read_text(encoding="utf-8"))
    assert hard["schema"] == "jev-hard-misses-1"
    # A hard-miss draw advances cursor. Requiring 0 blocked Submit after 1327.
    assert int(hard["cursor"]) >= 0
    assert len(hard["items"]) >= 20
    gold = gold_norms()
    hold_ids = {item["id"] for item in hold}
    for item in hard["items"]:
        assert item["id"] == title_id(item["title"])
        assert normalize_title(item["title"]) not in gold
        assert item["id"] not in hold_ids
    # Real gold titles are excluded by the same helper the draw uses.
    gold_title = load_gold()["items"][0]["title"]
    assert normalize_title(gold_title) in gold


def test_hard_miss_bank_builder_skips_gold_and_holdout():
    archive = load_archive(ROOT / "01_daily" / "news")
    hold = read_holdout_items(ROOT / "00_grounding" / "jev_holdout.json")
    gold = gold_norms()
    bank = propose_hard_miss_bank(archive, hold, gold, 20, random.Random(20260929))
    assert len(bank) == 20
    hold_ids = {item["id"] for item in hold}
    for item in bank:
        assert item["id"] not in hold_ids
        assert normalize_title(item["title"]) not in gold


def test_append_hard_misses_does_not_duplicate_or_drop():
    blob = {"schema": "jev-hard-misses-1", "cursor": 4, "items": [
        {"id": "aaaaaaaaaaaaaaaa", "title": "Keep me"},
    ]}
    title = "Gold falls as traders price another Fed hike"
    out = append_hard_misses(blob, [
        {"id": title_id(title), "title": title, "source": "reuters"},
        {"id": title_id(title), "title": title, "source": "reuters"},
    ])
    assert out["cursor"] == 4
    assert len(out["items"]) == 2
    assert out["items"][0]["title"] == "Keep me"
    holdout_title = "Holdout merger involving a US railroad"
    skipped = append_hard_misses(out, [
        {"id": title_id(holdout_title), "title": holdout_title, "source": "reuters"},
    ], skip_ids={title_id(holdout_title)})
    assert len(skipped["items"]) == 2


def test_write_grade_does_not_copy_holdout_misses_into_hard_bank():
    holdout_title = "Holdout merger involving a US railroad"
    leftover = "CFTC explores crypto rules after a named dollar deal"
    rows = _quiet_rows(28)
    rows.extend([
        {
            "title": holdout_title,
            "source": "reuters",
            "pool": "holdout",
            "jev": "DROP",
            "reason": "low_material",
            "grade": "K",
            "human_reason": "named M&A — keep",
        },
        {
            "title": leftover,
            "source": "reuters",
            "pool": "parsed",
            "jev": "DROP",
            "reason": "low_material",
            "grade": "K",
            "human_reason": "regulator exploring rules",
        },
    ])
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0100",
        "nonce": "holdout-skip-nonce",
        "rows": rows,
    }))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ground = root / "00_grounding"
        ground.mkdir()
        hid = title_id(holdout_title)
        (ground / "jev_holdout.json").write_text(json.dumps({
            "ids": [hid],
            "items": [{"id": hid, "title": holdout_title}],
        }), encoding="utf-8")
        write_grade(
            parsed, stamp="20260929_0170", now=NOW, root=root, ground=ground, write=True,
        )
        hard = json.loads((ground / "jev_hard_misses.json").read_text(encoding="utf-8"))
        ids = {item["id"] for item in hard["items"]}
        assert hid not in ids
        assert title_id(leftover) in ids


def _quiet_rows(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append({
            "title": f"Quiet desk note {i} about municipal parking in city {i}",
            "source": "reuters",
            "pool": "parsed",
            "jev": "DROP",
            "reason": "low_material",
            "geo": "other",
            "grade": "D",
            "human_reason": "",
        })
    return rows


def test_human_reason_persists_in_json_md_and_issue():
    title = "August Core PCE print lands ahead of the Fed decision"
    rows = _quiet_rows(29)
    rows.append({
        "title": title,
        "source": "reuters",
        "pool": "holdout",
        "jev": "KEEP",
        "reason": "code_print",
        "geo": "core",
        "grade": "D",
        "human_reason": "print is a false keep\nsecond line" + ("x" * 600),
    })
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0100",
        "nonce": "reason-nonce",
        "rows": rows,
    }))
    hit = next(row for row in parsed["rows"] if row["title"] == title)
    assert hit["human_reason"] == ("print is a false keep second line" + ("x" * 600))[:500]
    assert "\n" not in hit["human_reason"]
    legacy = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "rows": [{
            "title": "Brazil soy exports clear the Santos loading queue",
            "jev": "DROP",
            "grade": "D",
            "note": "from note",
        }],
    }))
    assert legacy["rows"][0]["human_reason"] == "from note"
    explicit_blank = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "rows": [{
            "title": "Norway salmon farms report a quiet week on volumes",
            "jev": "DROP",
            "grade": "D",
            "human_reason": "",
            "note": "ignored when human_reason is set",
        }],
    }))
    assert explicit_blank["rows"][0]["human_reason"] == ""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ground = root / "00_grounding"
        ground.mkdir()
        result = write_grade(
            parsed, stamp="20260929_0140", now=NOW, root=root, ground=ground, write=True,
        )
        session = json.loads((ground / "jev_train" / "20260929_0140.json").read_text(encoding="utf-8"))
        item = next(row for row in session["items"] if row["title"] == title)
        assert item["human_reason"] == hit["human_reason"]
        assert "note" not in item
        md = (ground / "jev_train" / "20260929_0140.md").read_text(encoding="utf-8")
        assert "| # | jev | you | bits | source | title | note |" in md
        assert hit["human_reason"] in md
        body = result["issue_body"]
        rows_section = body.split("## Rows", 1)[1].split("## FLAG blank reason", 1)[0]
        assert "| title | Jev | You | human_reason |" in rows_section
        assert f"| {title} | KEEP | D | {hit['human_reason']} |" in rows_section
        assert "Rows where You != Jev and human_reason is blank: 0." in body


def test_blank_disagreement_is_flagged():
    soy = "Brazil soy exports clear the Santos loading queue"
    pce = "August Core PCE print lands ahead of the Fed decision"
    tape = "Gold falls as traders price another Fed hike"
    salmon = "Norway salmon farms report a quiet week on volumes"
    battery = "Korean battery plant adds a second line in Georgia"
    rows = _quiet_rows(27)
    rows.extend([
        {
            "title": soy,
            "source": "reuters",
            "jev": "DROP",
            "grade": "D",
            "human_reason": "   ",
        },
        {
            "title": pce,
            "source": "reuters",
            "jev": "KEEP",
            "grade": "D",
            "human_reason": "",
        },
        {
            "title": tape,
            "source": "reuters",
            "jev": "DROP",
            "grade": "K",
            "human_reason": "real tape",
        },
        {
            "title": salmon,
            "source": "reuters",
            "jev": "DROP",
            "grade": "?",
            "human_reason": "",
        },
        {
            "title": battery,
            "source": "reuters",
            "jev": "KEEP",
            "grade": "?",
            "human_reason": "not sure",
        },
    ])
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_0100",
        "nonce": "flag-nonce",
        "rows": rows,
    }))
    assert count_marks(parsed["rows"]) == 30
    flagged = blank_disagreements(parsed["rows"])
    flagged_titles = [row["title"] for row in flagged]
    assert pce in flagged_titles
    assert salmon in flagged_titles
    assert soy not in flagged_titles
    assert tape not in flagged_titles
    assert battery not in flagged_titles
    result = write_grade(parsed, stamp="20260929_0150", now=NOW, write=False)
    body = result["issue_body"]
    flag = body.split("## FLAG blank reason", 1)[1].split("## False keeps", 1)[0]
    assert "Rows where You != Jev and human_reason is blank: 2." in flag
    assert f"| {pce} | KEEP | D | |" in flag
    assert f"| {salmon} | DROP | ? | |" in flag
    assert soy not in flag
    assert tape not in flag
    assert battery not in flag
    rows_section = body.split("## Rows", 1)[1].split("## FLAG blank reason", 1)[0]
    assert f"| {soy} | DROP | D |  |" in rows_section
    assert f"| {tape} | DROP | K | real tape |" in rows_section
    assert f"| {battery} | KEEP | ? | not sure |" in rows_section
    assert "nonce: flag-nonce" in body


def test_slim_grades_hydrate_from_draw():
    title = "August Core PCE print lands ahead of the Fed decision"
    tid = title_id(title)
    rows = _quiet_rows(29)
    rows.append({
        "title": title,
        "source": "reuters",
        "pool": "holdout",
        "jev": "KEEP",
        "reason": "code_print",
        "geo": "core",
        "grade": "D",
        "human_reason": "print miss",
    })
    draw_items = []
    slim_rows = []
    for row in rows:
        item_id = title_id(row["title"])
        draw_items.append({
            "id": item_id,
            "title": row["title"],
            "source": row.get("source") or "",
            "pool": row.get("pool") or "",
            "jev": row["jev"],
            "reason": row.get("reason") or "",
            "geo": row.get("geo") or "",
            "actor_power": "",
            "new_instrument": 0,
        })
        slim_rows.append({
            "id": item_id,
            "grade": row["grade"],
            "human_reason": row.get("human_reason") or "",
        })
    parsed = parse_grades_payload(json.dumps({
        "schema": "jev-train-grades-1",
        "draw_stamp": "20260929_1120",
        "nonce": "slim-nonce",
        "rows": slim_rows,
    }))
    assert parsed["rows"][0]["title"] == ""
    assert parsed["rows"][0]["jev"] == ""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ground = root / "00_grounding"
        train = ground / "jev_train"
        train.mkdir(parents=True)
        (train / "20260929_1120_draw.json").write_text(json.dumps({
            "schema": "jev-train-draw-1",
            "stamp": "20260929_1120",
            "items": draw_items,
        }), encoding="utf-8")
        result = write_grade(
            parsed, stamp="20260929_1121", now=NOW, root=root, ground=ground, write=True,
        )
        session = json.loads((train / "20260929_1121.json").read_text(encoding="utf-8"))
        hit = next(row for row in session["items"] if row["id"] == tid)
        assert hit["title"] == title
        assert hit["jev"] == "KEEP"
        assert hit["grade"] == "D"
        assert hit["human_reason"] == "print miss"
        assert "print miss" in result["issue_body"]
    hydrated = hydrate_grades(parsed, {"stamp": "20260929_1120", "items": draw_items})
    assert hydrated["rows"][-1]["title"] == title
    assert hydrated["rows"][-1]["jev"] == "KEEP"


def test_page_script_submit_threshold():
    import shutil
    import subprocess
    node = shutil.which("node")
    if not node:
        return
    script = r"""
const api = require("./dashboard/jev-train/app.js");
const grades = Array.from({length: 29}, () => "D").concat(["?"]);
if (api.marksReady(grades)) throw new Error("29 D plus ? must stay disabled");
grades.push("K");
if (!api.marksReady(grades)) throw new Error("30 K/D must enable");
const draw = {stamp: "20260929_0130", items: [{
  id: "x", title: "August Core PCE", source: "reuters", pool: "parsed",
  jev: "KEEP", reason: "code_print", bits: ["print"], geo: "", actor_power: "",
  new_instrument: 0
}]};
const marks = {x: {grade: "D", human_reason: "print miss"}};
const payload = api.buildGrades(draw, marks, "nonce-9");
const text = JSON.stringify(payload);
if (payload.rows[0].grade !== "D") throw new Error("grade");
if (payload.rows[0].human_reason !== "print miss") throw new Error("human_reason");
if (Object.prototype.hasOwnProperty.call(payload.rows[0], "note")) throw new Error("note field");
const legacy = api.buildGrades(draw, {x: {grade: "D", note: "from note"}}, "nonce-9");
if (legacy.rows[0].human_reason !== "from note") throw new Error("note fallback");
const blank = api.buildGrades(draw, {x: {grade: "D", human_reason: ""}}, "nonce-9");
if (blank.rows[0].human_reason !== "") throw new Error("empty reason");
if (!api.marksReady(Array.from({length: 30}, () => "D"))) throw new Error("empty reasons still count");
if (!text.includes("nonce-9")) throw new Error("nonce");
if (text.includes("ghp_") || text.includes("JEV_API_KEY") || text.includes("token")) {
  throw new Error("payload leaked a token field");
}
if (typeof payload.token !== "undefined") throw new Error("token key");
const slim = api.buildDispatchGrades(draw, marks, "nonce-9");
if (slim.rows[0].title) throw new Error("dispatch sent title");
if (slim.rows[0].jev) throw new Error("dispatch sent jev");
if (slim.rows[0].human_reason !== "print miss") throw new Error("dispatch reason");
const hundred = {stamp: "20260929_1120", items: Array.from({length: 100}, (_, i) => ({
  id: ("0".repeat(15) + i.toString(16)).slice(-16),
  title: "Fixture headline " + (i + 1) + " about a market move with extra words",
  source: "fixture", pool: "parsed", jev: "KEEP", reason: "code_print",
  bits: ["print"], geo: "", actor_power: "", new_instrument: 0,
  url: "https://example.com/article/" + i, published_at: "2026-09-29T11:20:00Z"
}))};
const marks100 = {};
hundred.items.forEach(function (row, i) {
  marks100[row.id] = {grade: i % 2 ? "D" : "K", human_reason: i === 0 ? "health scare" : ""};
});
const fat = JSON.stringify(api.buildGrades(hundred, marks100, "nonce-100"));
const packedFat = Buffer.from(fat).toString("base64");
const packedSlim = Buffer.from(JSON.stringify(api.buildDispatchGrades(hundred, marks100, "nonce-100"))).toString("base64");
if (packedFat.length <= packedSlim.length) throw new Error("full sheet should be larger than dispatch sheet");
if (packedSlim.length > api.DISPATCH_MAX) throw new Error("slim 100-row payload still too large: " + packedSlim.length);
"""
    subprocess.check_call([node, "-e", script], cwd=str(ROOT))


def main() -> None:
    tests = [
        test_why_bits_follow_the_current_gate,
        test_false_keep_and_false_drop_record_bits,
        test_grade_files_need_thirty_marks_and_do_not_touch_closed_lists,
        test_human_reason_persists_in_json_md_and_issue,
        test_blank_disagreement_is_flagged,
        test_slim_grades_hydrate_from_draw,
        test_trained_hash_and_gold_are_excluded_from_every_pool,
        test_hard_misses_rotate_when_holdout_is_used_up,
        test_draw_writes_page_json_and_leaves_holdout_bytes_alone,
        test_ensure_pool_files_does_not_rewrite_existing_holdout,
        test_grades_payload_accepts_base64_and_rejects_secrets,
        test_publish_issue_opens_or_updates_the_titled_issue,
        test_issue_body_has_counts_and_json_link,
        test_session_409_proposes_action_material,
        test_write_grade_updates_living_rubric,
        test_commit_allowlist_and_workflow_and_page,
        test_repo_holdout_and_hard_miss_bank_are_disjoint_from_gold,
        test_hard_miss_bank_builder_skips_gold_and_holdout,
        test_append_hard_misses_does_not_duplicate_or_drop,
        test_write_grade_does_not_copy_holdout_misses_into_hard_bank,
        test_page_script_submit_threshold,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
