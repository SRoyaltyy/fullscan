"""Hop-0 eval draw. No live key. Stdlib only."""
from __future__ import annotations

import datetime as dt
import json
import random
import tempfile
from pathlib import Path
from unittest import mock

from src.jev_eval import (
    HOLDOUT_PATH,
    draw_sample,
    load_archive,
    parse_rss_xml,
    render_sheet,
    resolve_knobs,
    run_eval,
    score_rules,
    stratified_draw,
    title_id,
)
from src.jev_gate import (
    GROUND,
    MATERIAL_KEEP,
    NEWS_DIR,
    ROOT,
    GateKnobs,
    decide,
    gate,
    load_chokepoint_state,
    load_gold,
    main as gate_main,
    normalize_title,
    tokens,
)

ASOF = dt.datetime(2026, 9, 28, 15, 4, tzinfo=dt.timezone.utc)


def _row(title: str, day: str, source: str = "reuters") -> dict:
    return {
        "id": title_id(title),
        "title": title,
        "source": source,
        "published_at": f"{day}T12:00:00Z",
        "url": "",
        "date": day,
        "archive": "parsed",
        "query": "",
    }


def _answers(material: float, instrument: float, geo: str, actor: str,
             opinion: float = 0.02) -> dict:
    return {
        "is_opinion": opinion,
        "is_tabloid": 0.02,
        "is_reaction": 0.02,
        "geo": geo,
        "actor_power": actor,
        "action_material": material,
        "new_instrument": instrument,
        "reprint_weather": 0.05,
    }


def test_title_id_is_stable_and_ignores_source_suffix():
    a = title_id("Fed holds rates at 4.25 percent - CNBC")
    b = title_id("Fed holds rates at 4.25 percent")
    assert a == b
    assert a != title_id("Boeing identifies a 737 MAX software glitch")
    assert len(a) == 16


def test_gold_title_is_discarded_and_redrawn():
    gold_title = "Trump finalizes CAFE rollback"
    assert normalize_title(gold_title) in {
        normalize_title(it["title"]) for it in load_gold()["items"]
    }
    rows = [
        _row(gold_title, "2026-09-01"),
        _row("FDA grants a dated approval for a cardiac device", "2026-09-01"),
    ]
    picked, gold_n = stratified_draw(
        rows, 2, random.Random(1),
        gold={normalize_title(gold_title)},
    )
    assert gold_n >= 1
    assert len(picked) == 1
    assert gold_title not in [r["title"] for r in picked]


def test_stratified_draw_covers_each_date():
    rows = []
    for day in ("2026-08-01", "2026-08-02", "2026-08-03"):
        for i in range(8):
            # Distinct stems so cross-date Jaccard does not collapse the strata.
            rows.append(_row(
                f"day{day[-2:]} agency{i} prints statute {day[-2:]}{i} for docket {1000 + i}",
                day,
            ))
    picked, gold_n = stratified_draw(rows, 9, random.Random(3), gold=set())
    assert gold_n == 0
    assert len(picked) == 9
    counts = {}
    for row in picked:
        counts[row["date"]] = counts.get(row["date"], 0) + 1
    assert counts == {"2026-08-01": 3, "2026-08-02": 3, "2026-08-03": 3}


def test_rss_jaccard_skips_the_same_wire():
    from src.jev_eval import pool_draw
    base = "Fed holds rates at 4.25 percent after the FOMC meeting"
    rows = [
        _row("Fed holds rates at 4.25 percent after the FOMC meeting - CNBC", "2026-09-28"),
        _row("Boeing identifies a software glitch on the 737 MAX landing computer", "2026-09-28"),
        _row("Apple loses a patent verdict over the haptic engine in federal court", "2026-09-28"),
    ]
    picked, gold_n = pool_draw(
        rows, 2, random.Random(1), gold=set(), blocked=[tokens(base)],
    )
    assert gold_n == 0
    titles = " ".join(r["title"] for r in picked)
    assert "FOMC" not in titles
    assert len(picked) == 2


def test_parse_rss_xml_reads_items():
    xml = b"""<?xml version="1.0"?>
    <rss><channel>
      <item>
        <title>SpaceX files a first earnings print - Reuters</title>
        <link>https://example.test/a</link>
        <pubDate>Mon, 28 Sep 2026 12:00:00 GMT</pubDate>
        <source>Reuters</source>
      </item>
    </channel></rss>"""
    rows = parse_rss_xml(xml, "rss_google_markets")
    assert len(rows) == 1
    assert rows[0]["source"] == "Reuters"
    assert rows[0]["query"] == "rss_google_markets"
    assert rows[0]["date"] == "2026-09-28"
    assert rows[0]["id"] == title_id(rows[0]["title"])


def test_digest_fallback_when_parsed_day_is_empty():
    with tempfile.TemporaryDirectory() as tmp:
        news = Path(tmp)
        (news / "2026-09-01_parsed.json").write_text(
            json.dumps({"all_items": []}), encoding="utf-8")
        (news / "2026-09-01_finviz_digest.json").write_text(json.dumps({
            "top_signal": [{"news_title": "NHTSA opens a probe into a brake defect", "source": "finviz"}],
        }), encoding="utf-8")
        (news / "2026-09-02_parsed.json").write_text(json.dumps({
            "all_items": [{"title": "SEC charges a listed broker over a filing", "source": "sec",
                           "published_at": "2026-09-02"}],
        }), encoding="utf-8")
        rows = load_archive(news)
        titles = {r["title"] for r in rows}
        assert "NHTSA opens a probe into a brake defect" in titles
        assert "SEC charges a listed broker over a filing" in titles
        digest = next(r for r in rows if r["title"].startswith("NHTSA"))
        assert digest["archive"] == "digest"


def test_one_knob_only_and_closed_list_is_not_a_knob():
    knobs, name, _raw = resolve_knobs(None)
    assert name is None
    assert knobs.action_material == MATERIAL_KEEP
    knobs, name, _raw = resolve_knobs(["action_material=0.55"])
    assert name == "action_material"
    assert knobs.action_material == 0.55
    try:
        resolve_knobs(["action_material=0.55", "new_instrument=0.50"])
        raise AssertionError("two knobs should fail")
    except ValueError as exc:
        assert "ONE" in str(exc)
    try:
        resolve_knobs(["headline=Boeing identifies 737 MAX software glitch"])
        raise AssertionError("closed-list headline should fail")
    except ValueError as exc:
        assert "not allowed" in str(exc)


def test_default_knobs_match_current_gate():
    samples = [
        ("Trump finalizes CAFE rollback", _answers(0.88, 0.91, "core", "state_head")),
        ("What it means for markets if the Fed holds", _answers(0.2, 0.1, "core", "other_person", 0.9)),
        ("Yemen bombed by terrorist group", _answers(0.12, 0.05, "other", "other_person")),
    ]
    for title, answers in samples:
        plain = decide({"title": title}, answers)
        knobbed = decide({"title": title}, answers, GateKnobs())
        assert plain["decision"] == knobbed["decision"]
        assert plain["reason"] == knobbed["reason"]


def test_allowed_knobs_move_one_decision_each():
    cafe = _answers(0.60, 0.10, "core", "other_person")
    low = decide({"title": "A listed mill changes a supplier contract"}, cafe)
    assert low["decision"] == "drop"
    high = decide(
        {"title": "A listed mill changes a supplier contract"},
        cafe,
        GateKnobs(action_material=0.55),
    )
    assert high["decision"] == "keep" and high["reason"] == "core_material"

    inst = _answers(0.10, 0.55, "core", "regulator")
    assert decide({"title": "FDA posts a routine bulletin"}, inst)["decision"] == "drop"
    kept = decide(
        {"title": "FDA posts a routine bulletin"},
        inst,
        GateKnobs(new_instrument=0.50),
    )
    assert kept["decision"] == "keep"

    reaction = decide(
        {"title": "Gold falls as traders price another Fed hike"},
        _answers(0.80, 0.20, "core", "listed_firm"),
        GateKnobs(reaction_regex=r"gold falls|oil price today|brent rises|stocks jump as"),
    )
    assert reaction["decision"] == "drop" and reaction["reason"] == "reaction_regex"
    untouched = decide(
        {"title": "Gold falls as traders price another Fed hike"},
        _answers(0.80, 0.20, "core", "listed_firm"),
    )
    assert untouched["reason"] != "reaction_regex"

    listed = decide(
        {"title": "Boeing BA identifies a software glitch"},
        _answers(0.10, 0.05, "other", "other_person"),
        GateKnobs(listed_token_in_title=True, listed_tokens=frozenset({"ba"})),
    )
    assert listed["decision"] == "keep" and listed["reason"] == "listed_token"

    state = load_chokepoint_state()
    title = "Tensions persist as tankers transit the Strait of Hormuz"
    row = {"title": title, "source": "reuters", "published_at": "2026-09-27T00:00:00Z"}
    stale = gate([dict(row)], code_only=True, asof=dt.date(2026, 9, 27), state=state)
    assert stale[0]["reason"] == "reprint_weather"
    fresh_days = gate(
        [dict(row)], code_only=True, asof=dt.date(2026, 9, 27), state=state,
        knobs=GateKnobs(reprint_weather_days=10000),
    )
    assert fresh_days[0]["decision"] == "keep"
    fresh_verb = gate(
        [dict(row)], code_only=True, asof=dt.date(2026, 9, 27), state=state,
        knobs=GateKnobs(new_verbs=("persist",)),
    )
    assert fresh_verb[0]["decision"] == "keep"


def test_score_rules_four_bars():
    def item(pred, label):
        return {"predicted": pred, "label": label}

    unlabeled = score_rules([item("drop", "")], drop_rate=0.8)
    assert unlabeled["pass"] is None and unlabeled["unlabeled"] == 1

    ok = [
        item("keep", "must_keep"),
        item("drop", "should_keep"),
        item("drop", "should_keep"),
        item("drop", "must_drop"),
        item("keep", "should_drop"),
        item("keep", "should_drop"),
        item("keep", "should_drop"),
        item("drop", "should_drop"),
    ]
    # 5 drops / 8 = 0.625, under 70%
    assert score_rules(ok, drop_rate=0.625)["pass"] is False
    assert score_rules(ok, drop_rate=0.70)["pass"] is True
    assert score_rules(ok + [item("drop", "must_keep")], drop_rate=0.9)["pass"] is False
    assert score_rules(ok + [item("keep", "must_drop")], drop_rate=0.9)["pass"] is False
    assert score_rules(ok + [item("drop", "should_keep")], drop_rate=0.9)["pass"] is False
    assert score_rules(ok + [item("keep", "should_drop")], drop_rate=0.9)["pass"] is False


def _poster(state, questions, key):
    title = state.split("TITLE:", 1)[-1]
    keep = "FDA" in title or "NHTSA" in title or "SEC " in title
    def noul(v):
        return {"type": "noul", "noul": v}
    return {
        "model": "jev-test",
        "answers": {
            "is_opinion": noul(0.05 if keep else 0.92),
            "is_tabloid": noul(0.01),
            "is_reaction": noul(0.01),
            "geo": {"type": "choice", "choice": "core" if keep else "other"},
            "actor_power": {"type": "choice", "choice": "regulator" if keep else "other_person"},
            "action_material": noul(0.9 if keep else 0.1),
            "new_instrument": noul(0.2),
            "reprint_weather": noul(0.05),
        },
    }


def _fixture_news(path: Path) -> None:
    catalog = [
        ("2026-09-01", [
            "FDA grants a dated approval for a cardiac valve",
            "Trump finalizes CAFE rollback",
            "Opinion column wonders what the tape means today",
            "A port worker describes the morning fog",
            "NHTSA opens a formal defect probe",
            "Tourists crowd a city square after a parade",
        ]),
        ("2026-09-02", [
            "SEC charges a listed broker over late filings",
            "Gold falls as desk chatter recaps the session",
            "A bakery raises the price of bread downtown",
            "Protesters gather outside a town hall",
            "An airline pauses one regional route",
            "A museum opens a new wing on Friday",
        ]),
        ("2026-09-03", [
            "A chip foundry delays a factory tour",
            "A retailer guides inventory lower for spring",
            "A storm closes a coastal road",
            "A bank sets aside more for card losses",
            "A miner updates its reserve statement",
            "A studio moves a film release to winter",
        ]),
    ]
    for day, titles in catalog:
        items = [{"title": t, "source": "reuters", "published_at": f"{day}T12:00:00Z"} for t in titles]
        (path / f"{day}_parsed.json").write_text(
            json.dumps({"all_items": items}), encoding="utf-8")


def _fixture_rss() -> list[dict]:
    titles = [
        "A cargo insurer revises a hull clause",
        "A grid operator schedules a substation outage",
        "A railroad posts a maintenance window",
        "A refinery reports a compressor trip",
        "A pharmacy chain updates a shortage list",
        "A shipyard books a drydock slot",
        "Fed holds rates at 4.25 percent after the FOMC meeting - CNBC",
    ]
    return [_row(t, "2026-09-28", "rss") | {"archive": "rss", "query": "rss_google_macro"} for t in titles]


def test_run_eval_writes_unlabeled_round_and_freezes_holdout():
    watched = [
        ROOT / "00_grounding" / "jev_gold.json",
        ROOT / "00_grounding" / "jev_closed_lists.json",
        ROOT / "00_grounding" / "jev_junk_shapes.json",
        ROOT / "00_grounding" / "jev_chokepoint_state.json",
        ROOT / "03_scoreboard" / "JEV_GATE.md",
        ROOT / "01_daily" / "news" / "2026-09-28_jev_keep.json",
        ROOT / ".github" / "workflows" / "jev_hop0.yml",
    ]
    before = {p: p.read_bytes() for p in watched}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "news"
        ground = root / "ground"
        news.mkdir()
        ground.mkdir()
        _fixture_news(news)
        report = run_eval(
            live=True, workers=1, seed=7, news_dir=news, ground_dir=ground,
            now=ASOF, rss_rows=_fixture_rss(), poster=_poster, key="test",
            parsed_n=4, rss_n=4, holdout_n=3,
        )
        assert report["labeled"] is False
        assert report["rules"] is None
        assert report["knob_changed"] is None
        assert report["round"] == 1
        assert all(it["label"] == "" for it in report["items"])
        assert report["sample"]["gold_discarded_redrawn"] >= 0
        assert report["sample"]["parsed"] == 4
        assert report["sample"]["rss"] == 4
        assert report["sample"]["holdout"] == 3
        assert report["sample"]["tuning_overlap_holdout"] == []
        assert report["predicted"]["overall"]["n"] == 11
        assert report["predicted"]["overall"]["keep"] + report["predicted"]["overall"]["drop"] == 11
        assert report["predicted"]["overall"]["drop"] >= 4
        sheet = Path(report["_sheet_path"]).read_text(encoding="utf-8")
        assert "UNLABELED" in sheet
        assert "| must_keep |" not in sheet
        assert "| should_drop |" not in sheet
        hold_bytes = (ground / "jev_holdout.json").read_bytes()
        blob = json.loads(hold_bytes)
        assert blob["algo"] == "sha256-norm-16"
        assert len(blob["ids"]) == 3
        second = run_eval(
            live=True, workers=1, seed=99, news_dir=news, ground_dir=ground,
            now=ASOF.replace(minute=5), rss_rows=_fixture_rss(), poster=_poster,
            key="test", parsed_n=4, rss_n=4, holdout_n=3,
        )
        assert (ground / "jev_holdout.json").read_bytes() == hold_bytes
        assert second["sample"]["holdout_created"] is False
        assert second["sample"]["holdout_ids"] == blob["ids"]
        assert second["round"] == 2
        tuning = {it["id"] for it in second["items"] if it["pool"] != "holdout"}
        assert tuning.isdisjoint(set(blob["ids"]))
        # FOMC wire was blocked by Jaccard against nothing in this fixture's
        # archive, so it may be drawn. Gold title must not be.
        assert all("CAFE rollback" not in it["title"] for it in second["items"])
    after = {p: p.read_bytes() for p in watched}
    assert before == after
    assert not HOLDOUT_PATH.exists()


def test_real_archive_draw_excludes_gold_and_reuses_holdout(tmp_ok=True):
    rng = random.Random(11)
    rss = []
    for n in range(80):
        rss.append(_row(
            f"zzhold{n} qqq{n} uniqueeval wire {n} xyz{n} plantgate",
            "2026-09-28",
            "rss",
        ))
    with tempfile.TemporaryDirectory() as tmp:
        hold = Path(tmp) / "jev_holdout.json"
        now = ASOF
        sample = draw_sample(
            news_dir=NEWS_DIR, holdout_path=hold, rng=rng, now=now,
            rss_rows=rss, parsed_n=50, rss_n=50, holdout_n=20,
        )
        assert sample["parsed"] and len(sample["parsed"]) == 50
        assert len(sample["rss"]) == 50
        assert len(sample["holdout"]) == 20
        assert len(set(r["date"] for r in sample["parsed"])) >= 5
        gold = {normalize_title(it["title"]) for it in load_gold()["items"]}
        for row in sample["parsed"] + sample["rss"] + sample["holdout"]:
            assert normalize_title(row["title"]) not in gold
        ids = {r["id"] for r in sample["holdout"]}
        assert ids.isdisjoint({r["id"] for r in sample["parsed"]})
        assert ids.isdisjoint({r["id"] for r in sample["rss"]})
        from src.jev_gate import _write_json
        _write_json(hold, sample["holdout_blob"])
        frozen = hold.read_bytes()
        again = draw_sample(
            news_dir=NEWS_DIR, holdout_path=hold, rng=random.Random(12),
            now=now, rss_rows=rss, parsed_n=50, rss_n=50, holdout_n=20,
        )
        assert again["holdout_created"] is False
        assert hold.read_bytes() == frozen
        assert {r["id"] for r in again["holdout"]} == ids


def test_cli_refuses_missing_key_and_two_knobs():
    with mock.patch.dict("os.environ", {"JEV_API_KEY": "", "TYPESAFE_API_KEY": ""}, clear=False):
        assert gate_main(["--eval"]) == 1
        assert gate_main(["--eval", "--code-only"]) == 2
        assert gate_main(["--eval", "--knob", "action_material=0.5", "--knob", "new_instrument=0.5"]) == 2


def test_workflow_is_dispatch_and_pr_only():
    yml = (ROOT / ".github" / "workflows" / "jev_eval.yml").read_text(encoding="utf-8")
    assert "schedule:" not in yml
    assert "cron:" not in yml
    assert "workflow_dispatch:" in yml
    assert "pull_request:" in yml
    assert "secrets.JEV_API_KEY" in yml
    assert "src/jev_eval.py" in yml
    assert "src/jev_gate.py" in yml
    assert "src/test_jev_eval.py" in yml
    assert "safe_git_push" not in yml
    paths = yml.split("paths:", 1)[1].split("permissions:", 1)[0]
    assert "jev_rounds" not in paths
    assert "jev_holdout" not in paths
    hop0 = (ROOT / ".github" / "workflows" / "jev_hop0.yml").read_text(encoding="utf-8")
    assert 'cron: "25 11 * * 1-6"' in hop0
    text = (ROOT / "src" / "jev_eval.py").read_text(encoding="utf-8")
    assert "write_report" not in text
    assert "sk-" not in text
    for line in yml.splitlines():
        if "sk-" in line and "grep" not in line:
            raise AssertionError(line)
    sheet = render_sheet({
        "stamp": "20260928_1504",
        "round": 1,
        "run_id": "local",
        "knob_changed": None,
        "items": [{
            "pool": "parsed", "predicted": "drop", "reason": "opinion",
            "title": "A column", "label": "",
        }],
    })
    assert "|  |" in sheet or "|  | A column |" in sheet


def main() -> None:
    tests = [
        test_title_id_is_stable_and_ignores_source_suffix,
        test_gold_title_is_discarded_and_redrawn,
        test_stratified_draw_covers_each_date,
        test_rss_jaccard_skips_the_same_wire,
        test_parse_rss_xml_reads_items,
        test_digest_fallback_when_parsed_day_is_empty,
        test_one_knob_only_and_closed_list_is_not_a_knob,
        test_default_knobs_match_current_gate,
        test_allowed_knobs_move_one_decision_each,
        test_score_rules_four_bars,
        test_run_eval_writes_unlabeled_round_and_freezes_holdout,
        test_real_archive_draw_excludes_gold_and_reuses_holdout,
        test_cli_refuses_missing_key_and_two_knobs,
        test_workflow_is_dispatch_and_pr_only,
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
