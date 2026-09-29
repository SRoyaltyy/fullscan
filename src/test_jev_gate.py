"""Hop-0 Jev gate: code math + mocked decide. No live key.

stdlib only — workflow_selfcheck has no pip.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from unittest import mock

from src.jev_gate import (
    FORBIDDEN_QUESTION_BITS,
    JACCARD_DROP,
    QUESTIONS,
    answers_from_gold,
    api_key,
    calendar_day,
    classifiable_reason,
    code_drop_reason,
    code_hints,
    decide,
    dedup_rows,
    gate,
    gold_rows,
    jaccard,
    jev_post,
    junk_shape_hit,
    load_chokepoint_state,
    load_gold,
    load_titles,
    make_state,
    mine_junk_shapes,
    normalize_title,
    parse_answers,
    punct_trash,
    questions_are_hop0,
    reprint_weather_code,
    run_gold,
    source_denied,
    tokens,
)
import datetime as dt

ROOT = Path(__file__).resolve().parent.parent


def test_normalize_strips_source_suffix_and_punct():
    assert normalize_title("Fed holds rates at 4.25% - CNBC") == "fed holds rates at 4 25"
    assert "cnbc" not in normalize_title("Fed holds rates - CNBC")
    assert normalize_title("AAPL!!!  Extra   spaces") == "aapl extra spaces"


def test_tokens_and_jaccard_dup():
    a = tokens("Fed holds rates at 4.25 percent after FOMC meeting")
    b = tokens("Fed holds rates at 4.25 percent after FOMC meeting - CNBC")
    assert jaccard(a, b) >= JACCARD_DROP
    far = tokens("Yemen bombed by terrorist group")
    assert jaccard(a, far) < 0.4


def test_dedup_keeps_earliest_wire():
    rows = [
        {
            "title": "Fed holds rates at 4.25 percent after FOMC meeting - CNBC",
            "published_at": "2026-09-27T07:40:00Z",
            "source": "cnbc",
        },
        {
            "title": "Fed holds rates at 4.25 percent after FOMC meeting",
            "published_at": "2026-09-27T07:00:00Z",
            "source": "reuters",
        },
    ]
    out = dedup_rows(rows, "2026-09-27")
    later = [r for r in out if "CNBC" in r["title"]][0]
    early = [r for r in out if r["source"] == "reuters"][0]
    assert later.get("code_reason") == "dup"
    assert not early.get("code_reason")
    assert later.get("dup_of", "").startswith("Fed holds")


def test_regex_trash_and_source_deny():
    assert source_denied("seekingalpha")
    assert source_denied("", "https://www.fool.com/story")
    assert not source_denied("reuters")
    assert punct_trash("Is the market about to crash?!")
    assert not punct_trash("FDA grants accelerated approval")
    assert junk_shape_hit("What it means for markets if the Fed holds")
    assert junk_shape_hit("Why this matters: stocks rally after payrolls")
    assert junk_shape_hit("AAPL stock underperforms competitors this week")
    assert not junk_shape_hit("Trump finalizes CAFE rollback")


def test_calendar_day_rfc2822():
    assert calendar_day("Sat, 26 Sep 2026 13:30:06 GMT") == "2026-09-26"
    assert calendar_day("2026-09-27T08:00:00Z") == "2026-09-27"


def test_reprint_clock_stale_vs_new_verb():
    state = load_chokepoint_state()
    asof = dt.date(2026, 9, 27)
    stale = reprint_weather_code(
        "Tensions persist as tankers transit the Strait of Hormuz", asof, state
    )
    assert stale["hit"] and stale["stale"] and not stale["has_new_verb"]
    fresh = reprint_weather_code(
        "Iran seizes tanker in Strait of Hormuz after first strike on shipping",
        asof, state,
    )
    assert fresh["hit"] and fresh["has_new_verb"] and not fresh["stale"]
    red = reprint_weather_code("Houthis hit a tanker in the Red Sea", asof, state)
    assert red["hit"] and red["has_new_verb"] and not red["stale"]
    yemen = reprint_weather_code("Yemen bombed by terrorist group", asof, state)
    assert not yemen["hit"]


def test_code_does_not_geo_drop_yemen_or_palestine():
    """Geo is Jev's job. Code must not country-allowlist."""
    state = load_chokepoint_state()
    asof = dt.date(2026, 9, 27)
    for title in (
        "Yemen bombed by terrorist group",
        "Palestine Action protesters arrested outside UK Labour Party conference",
        "Trump finalizes CAFE rollback",
        "Bitget lists tokenized US stocks for non-US users",
    ):
        reason = code_drop_reason(
            {"title": title, "source": "reuters"}, asof=asof, state=state,
        )
        assert reason == "", (title, reason)


def test_decide_palestine_vs_trump():
    crowd = decide(
        {"title": "Palestine Action protesters arrested outside UK Labour Party conference"},
        {
            "is_opinion": 0.04, "is_tabloid": 0.08, "is_reaction": 0.05,
            "geo": "other", "actor_power": "crowd",
            "action_material": 0.08, "new_instrument": 0.04,
            "reprint_weather": 0.10,
        },
    )
    assert crowd["decision"] == "drop" and crowd["reason"] == "crowd"
    head = decide(
        {"title": "Donald Trump arrested by UK Police"},
        {
            "is_opinion": 0.02, "is_tabloid": 0.12, "is_reaction": 0.03,
            "geo": "other", "actor_power": "state_head",
            "action_material": 0.93, "new_instrument": 0.55,
            "reprint_weather": 0.05,
        },
    )
    assert head["decision"] == "keep"
    assert head["reason"] in {"other_powerful", "state_head_action"}


def test_decide_core_and_chokepoint():
    cafe = decide(
        {"title": "Trump finalizes CAFE rollback"},
        {
            "is_opinion": 0.03, "is_tabloid": 0.02, "is_reaction": 0.04,
            "geo": "core", "actor_power": "state_head",
            "action_material": 0.88, "new_instrument": 0.91,
            "reprint_weather": 0.04,
        },
    )
    assert cafe["decision"] == "keep"
    noreaster = decide(
        {"title": "Nor'easter lashes East Coast boardwalk"},
        {
            "is_opinion": 0.04, "is_tabloid": 0.10, "is_reaction": 0.06,
            "geo": "core", "actor_power": "other_person",
            "action_material": 0.18, "new_instrument": 0.04,
            "reprint_weather": 0.12,
        },
    )
    assert noreaster["decision"] == "drop" and noreaster["reason"] == "low_material"
    yemen = decide(
        {"title": "Yemen bombed by terrorist group"},
        {
            "is_opinion": 0.04, "is_tabloid": 0.15, "is_reaction": 0.05,
            "geo": "other", "actor_power": "other_person",
            "action_material": 0.12, "new_instrument": 0.05,
            "reprint_weather": 0.20,
        },
    )
    assert yemen["decision"] == "drop" and yemen["reason"] == "geo_other"
    reprint = decide(
        {
            "title": "Tensions persist as tankers transit the Strait of Hormuz",
            "_clock": {"has_new_verb": False, "hit": True, "place": "hormuz"},
        },
        {
            "is_opinion": 0.05, "is_tabloid": 0.03, "is_reaction": 0.06,
            "geo": "chokepoint", "actor_power": "infrastructure",
            "action_material": 0.40, "new_instrument": 0.10,
            "reprint_weather": 0.86,
        },
    )
    assert reprint["decision"] == "drop" and reprint["reason"] == "reprint_weather"


def test_decide_fact_vetoes_opinion():
    """Session 409: a column frame wrapping a first-class fact is keep."""
    cook = decide(
        {"title": "Fed's Cook Warns AI Demand and Oil Prices to Keep Inflation Elevated"},
        {
            "is_opinion": 0.91, "is_tabloid": 0.04, "is_reaction": 0.05,
            "geo": "core", "actor_power": "regulator",
            "action_material": 0.72, "new_instrument": 0.20,
            "reprint_weather": 0.10,
        },
    )
    assert cook["decision"] == "keep", cook
    assert cook["reason"] != "opinion"
    recap = decide(
        {"title": "If a Stock Market Crash Is Coming, History Says This Is the Best Move"},
        {
            "is_opinion": 0.92, "is_tabloid": 0.08, "is_reaction": 0.06,
            "geo": "core", "actor_power": "other_person",
            "action_material": 0.12, "new_instrument": 0.04,
            "reprint_weather": 0.08,
        },
    )
    assert recap["decision"] == "drop" and recap["reason"] == "opinion"
    bitcoin = decide(
        {"title": (
            "10-year Treasury yield may hit 6%, but Bitcoin could still "
            "gain if rise is due to fiscal fears, not Fed hikes. - pluang.com"
        )},
        {
            "is_opinion": 0.91, "is_tabloid": 0.06, "is_reaction": 0.05,
            "geo": "other", "actor_power": "other_person",
            "action_material": 0.12, "new_instrument": 0.08,
            "reprint_weather": 0.10,
        },
    )
    assert bitcoin["decision"] == "drop" and bitcoin["reason"] == "opinion", bitcoin


def test_live_shaped_gold_misses():
    """Recorded 2026-09-28 live Jev answers. Closed lists correct the four misses."""
    trump = decide(
        {"title": "Donald Trump arrested by UK Police"},
        {
            "is_opinion": 0.05, "is_tabloid": 0.82, "is_reaction": 0.04,
            "geo": "other", "actor_power": "other_person",
            "action_material": 0.37, "new_instrument": 0.16,
            "reprint_weather": 0.15,
        },
    )
    assert trump["decision"] == "keep", trump
    assert trump["actor_power"] == "state_head"
    bitget = decide(
        {"title": "Bitget lists tokenized US stocks for non-US users"},
        {
            "is_opinion": 0.04, "is_tabloid": 0.05, "is_reaction": 0.04,
            "geo": "other", "actor_power": "infrastructure",
            "action_material": 0.21, "new_instrument": 0.17,
            "reprint_weather": 0.17,
        },
    )
    assert bitget["decision"] == "keep", bitget
    yemen = decide(
        {"title": "Yemen bombed by terrorist group"},
        {
            "is_opinion": 0.04, "is_tabloid": 0.15, "is_reaction": 0.05,
            "geo": "chokepoint", "actor_power": "other_person",
            "action_material": 0.41, "new_instrument": 0.03,
            "reprint_weather": 0.35,
        },
    )
    assert yemen["decision"] == "drop" and yemen["reason"] == "geo_other", yemen
    nbs = decide(
        {"title": "China NBS reports industrial profits fall 1.8% in August"},
        {
            "is_opinion": 0.03, "is_tabloid": 0.02, "is_reaction": 0.05,
            "geo": "core", "actor_power": "regulator",
            "action_material": 0.30, "new_instrument": 0.15,
            "reprint_weather": 0.31,
        },
    )
    assert nbs["decision"] == "keep", nbs


def test_code_hints_closed_lists():
    assert code_hints("Donald Trump arrested by UK Police")["state_head"]
    assert code_hints("Donald Trump arrested by UK Police")["head_action"]
    assert code_hints("Bitget lists tokenized US stocks")["venue"]
    assert code_hints("China NBS reports industrial profits")["agency"]
    assert code_hints("China NBS reports industrial profits")["newness"]
    assert not code_hints("Yemen bombed by terrorist group")["venue"]


def test_code_reason_short_circuits_jev():
    row = decide(
        {"title": "x", "code_reason": "junk_shape"},
        {"action_material": 0.99, "geo": "core", "actor_power": "regulator",
         "new_instrument": 0.99, "is_opinion": 0.0, "is_tabloid": 0.0,
         "is_reaction": 0.0, "reprint_weather": 0.0},
    )
    assert row["decision"] == "drop" and row["reason"] == "junk_shape"


def test_questions_are_hop0_only():
    questions_are_hop0()
    blob = json.dumps(QUESTIONS).lower()
    for bit in FORBIDDEN_QUESTION_BITS:
        assert bit not in blob
    assert "event_class" not in QUESTIONS
    assert "polarity" not in QUESTIONS
    assert "q5" not in QUESTIONS
    assert set(QUESTIONS) >= {
        "is_opinion", "is_tabloid", "is_reaction", "geo",
        "actor_power", "action_material", "new_instrument", "reprint_weather",
    }
    assert "first-class fact" in QUESTIONS["is_opinion"]["instructions"]
    assert "prices, policy" in QUESTIONS["action_material"]["instructions"]
    assert "weekly official figure" in QUESTIONS["new_instrument"]["instructions"]
    assert "accept/reject/seize" in QUESTIONS["reprint_weather"]["instructions"]
    assert "yen + dollar + Fed" in QUESTIONS["geo"]["criteria"]["core"]
    assert "CFTC" in QUESTIONS["actor_power"]["criteria"]["regulator"]


def test_parse_answers_and_state():
    payload = {
        "model": "jev-1.13.0",
        "answers": {
            "is_opinion": {"type": "noul", "noul": 0.2},
            "geo": {
                "type": "choice",
                "choice": "core",
                "probabilities": {"core": 0.8, "other": 0.2, "chokepoint": 0.0},
            },
        },
    }
    parsed = parse_answers(payload)
    assert parsed["is_opinion"] == 0.2
    assert parsed["geo"] == "core"
    state = make_state({
        "title": "Trump finalizes CAFE rollback",
        "source": "reuters",
        "published_at": "2026-09-27",
    })
    assert state.startswith("TITLE: Trump")
    assert "reuters" in state
    assert "CAFE" in state


def test_jev_post_body_and_429_retry():
    calls = []

    class FakeResp:
        def __init__(self, payload):
            self._payload = json.dumps(payload).encode()

        def read(self):
            return self._payload

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=30):
        calls.append(req)
        if len(calls) == 1:
            err = type("HTTPError", (urllib_http_error(),), {})(
                req.full_url, 429, "rate", {"Retry-After": "0"}, io.BytesIO()
            )
            raise err
        return FakeResp({
            "model": "jev-1.13.0",
            "answers": {"is_opinion": {"type": "noul", "noul": 0.1}},
            "usage": {"input_tokens": 10, "output_tokens": 2},
        })

    with mock.patch("src.jev_gate.urllib.request.urlopen", fake_urlopen):
        with mock.patch("src.jev_gate.time.sleep", lambda *_: None):
            out = jev_post("TITLE: x", QUESTIONS, "dummy-key")
    assert out["answers"]["is_opinion"]["noul"] == 0.1
    assert calls[0].get_header("Authorization") == "Bearer dummy-key"
    body = json.loads(calls[-1].data.decode())
    assert body["model"] == "jev-latest"
    assert "event_class" not in body["questions"]
    assert body["state"] == "TITLE: x"


def urllib_http_error():
    import urllib.error
    return urllib.error.HTTPError


def test_mocked_gold_full_pipeline():
    report = run_gold(live=False, code_only=False, workers=1)
    assert report["gold"]["ok"], report["gold"]
    by_id = {r["id"]: r for r in report["gold"]["rows"]}
    assert by_id["palestine"]["decision"] == "drop"
    assert by_id["cafe"]["decision"] == "keep"
    assert by_id["bitget"]["decision"] == "keep"
    assert by_id["fda"]["decision"] == "keep"
    assert by_id["nbs"]["decision"] == "keep"
    assert by_id["trump_arrest"]["decision"] == "keep"
    assert by_id["hormuz_reprint"]["reason"] == "reprint_weather"
    assert by_id["dup_b"]["reason"] == "dup"
    assert by_id["opinion"]["decision"] == "drop"


def test_code_only_gold_leaves_jev_rows():
    report = run_gold(live=False, code_only=True, workers=1)
    assert report["gold"]["ok"], report["gold"]
    by_id = {r["id"]: r for r in report["gold"]["rows"]}
    assert by_id["palestine"]["decision"] == "keep"
    assert by_id["palestine"]["reason"] == "code_leftover"
    assert by_id["cafe"]["reason"] == "code_leftover"
    assert by_id["opinion"]["decision"] == "drop"
    assert by_id["hormuz_reprint"]["decision"] == "drop"


def test_gate_does_not_call_jev_on_code_drops():
    poster = mock.Mock(side_effect=AssertionError("Jev should not run"))
    rows = [{
        "id": "opinion",
        "title": "What it means for markets if the Fed holds",
        "source": "seekingalpha",
        "published_at": "2026-09-27T18:00:00Z",
    }]
    out = gate(rows, code_only=False, live=True, key="x", poster=poster,
               asof=dt.date(2026, 9, 27))
    assert poster.call_count == 0
    assert out[0]["reason"] in {"source", "junk_shape"}


def test_gold_fixture_has_must_keep_set():
    gold = load_gold()
    ids = {it["id"] for it in gold["items"]}
    assert {"cafe", "bitget", "fda", "nbs", "palestine", "hormuz_reprint"} <= ids
    assert "cafe" in gold["must_keep"]
    rows = gold_rows()
    cafe = next(r for r in rows if r["id"] == "cafe")
    assert "action_material" in answers_from_gold(
        next(i for i in gold["items"] if i["id"] == "cafe")
    )
    assert cafe["title"]


def test_load_titles_reads_parsed_all_items():
    dates = sorted(p.name[:10] for p in (ROOT / "01_daily" / "news").glob("*_parsed.json"))
    assert dates
    rows = load_titles(dates[-1])
    assert rows
    assert all(r.get("title") for r in rows)


def test_mine_junk_shapes_runs():
    report = mine_junk_shapes(top=10, min_junk=20)
    assert report["junk_titles"] > 100
    assert report["top"]
    assert (ROOT / "01_daily" / "news" / "jev_junk_shapes_mined.json").is_file()


def test_api_key_not_in_repo_and_env():
    with mock.patch.dict("os.environ", {"JEV_API_KEY": "", "TYPESAFE_API_KEY": ""}, clear=False):
        assert api_key() == ""
    with mock.patch.dict("os.environ", {"JEV_API_KEY": "abc"}, clear=False):
        assert api_key() == "abc"
    text = (ROOT / "src" / "jev_gate.py").read_text(encoding="utf-8")
    assert "sk-" not in text
    assert "JEV_API_KEY" in text


def test_session_409_false_drops_are_keeps():
    """Round 1 homework: hop-0 keeps classifiable titles, still drops trash."""
    asof = dt.date(2026, 9, 29)
    state = load_chokepoint_state()
    keeps = [
        "Cyclospora fears lead consumers to lose their appetite for salads - CNBC",
        "Anthropic Targets $2 Trillion Record IPO: 8 Key Items Shaping the Stock Market Thursday - TheStreet Pro",
        "OpenAI Introduces ‘ChatGPT for Teens’ as Safety Concerns Grow - The New York Times",
        "Bitcoin Rally Tops $79K. Crypto Shorts, ETF Flows Soar. CFTC Explores Crypto Rules. - Investor's Business Daily",
        "Vanguard pays $4.6B for RIA software startup Altruist - Axios",
        "Basin rig count steady as prices drop",
        "US Federal Reserve holds rates steady as inflation hawks call for hike - CNA",
        "House clears FY2027 CR through Dec. 11, shutdown risk off",
        "Canadian National Railway outlines conditions to U.S. regulators for proposed Union Pacific–Norfolk Southern merger",
        "EIA weekly petroleum and natural gas storage — crude -0.4mb, gas +40 Bcf to 3,254 Bcf",
        "Azeri Light oil price decreases by 1.96% on world market - Report.az",
        "Fed’s Lisa Cook Warns AI Won’t Save The Economy From Near-Term Inflation - TradingView",
        "Oil Surges Over 3% as Trump Rejects Iran Peace Proposal and Hormuz Risk Returns - EnergyNow.com",
        "Fed's Cook Warns AI Demand and Oil Prices to Keep Inflation Elevated - IndexBox",
        "Tokyo yen trades in lower 157 range against dollar as U.S. rate hike bets fuel yen selling - finance.biggo.com",
        "3 Export Stocks Linked To Lower US China Tariffs - simplywall.st",
        "No tax on Social Security? The facts about Trump’s plan are here — and they could hurt US retirees the most",
        "Seafood groups Nordian Group, Norvelita sold to PE firm",
        "The Bond Market Sell-Off Is Freezing American Homebuilding",
        "4th Circuit dismisses some charges against Wells Fargo after jury’s $22.1M fee",
        "Explainer-What are credit default swaps and why are they spooking AI investors? - Yahoo Finance",
        "As Trump mulls building Chinese EVs in U.S., automakers point to Germany as a cautionary tale - NBC News",
    ]
    for title in keeps:
        assert classifiable_reason(title), title
        reason = code_drop_reason(
            {"title": title, "source": "simplywall.st"}, asof=asof, state=state,
        )
        assert reason == "", (title, reason)
        row = {"title": title}
        decided = decide(row, {
            "is_opinion": 0.9, "is_tabloid": 0.1, "is_reaction": 0.1,
            "geo": "other", "actor_power": "other_person",
            "action_material": 0.1, "new_instrument": 0.1,
            "reprint_weather": 0.9,
        })
        assert decided["decision"] == "keep", (title, decided)
    trash = [
        "Argentina star Messi not certain to play ‘much longer’ after father’s death",
        "If a Stock Market Crash Is Coming, History Says This Is the Best Move Investors Can Make - Yahoo Finance",
        "Gold tumbles below $4.150 as US bond yields, oil prices rise - tmgm.com",
    ]
    for title in trash:
        assert not classifiable_reason(title), title
        decided = decide(
            {"title": title},
            {
                "is_opinion": 0.8, "is_tabloid": 0.1, "is_reaction": 0.1,
                "geo": "other", "actor_power": "other_person",
                "action_material": 0.1, "new_instrument": 0.1,
                "reprint_weather": 0.1,
            },
        )
        assert decided["decision"] == "drop", (title, decided)


def test_session_409_forecast_tape_is_not_classifiable():
    """Code-keep must not swallow forecast/odds/tape that name-drop a print."""
    trash = [
        "Fed’s Warsh Rebuked by Investors Craving a Real Inflation Fight - Bloomberg",
        "Markets Price Roughly 69% Chance of October Fed Rate Hike - tokenpost.com",
        "Gold Price Forecast — XAU/USD ($4,146) Plunges 3.3% as 5.22% Yields — $4,000 Test or $4,300 Rebound After PCE - TradingNEWS",
        "Platinum Price Forecast: Fed Hike Bets Pressure Prices Below $1,700. - FXEmpire",
        "Dollar Holds Steady in NY Trading as US-Iran Conflict Lifts Oil Prices and Rates; PCE and Jobs Report Due This Week - finance.biggo.com",
        "Hong Kong’s IPO revival faces test as 3 new stocks stumble on debut - South China Morning Post",
        "Stock Market Today: Dow Steady After Surprise Retail Sales; Applied Materials Dives On Earnings (Live Coverage)",
    ]
    for title in trash:
        assert not classifiable_reason(title), (title, classifiable_reason(title))
        decided = decide(
            {"title": title, "source": "reuters"},
            {
                "is_opinion": 0.9, "is_tabloid": 0.1, "is_reaction": 0.1,
                "geo": "other", "actor_power": "other_person",
                "action_material": 0.1, "new_instrument": 0.1,
                "reprint_weather": 0.1,
            },
        )
        assert decided["decision"] == "drop", (title, decided)


def test_workflow_wires_secret_and_stays_stdlib():
    yml = (ROOT / ".github" / "workflows" / "jev_hop0.yml").read_text(encoding="utf-8")
    assert "secrets.JEV_API_KEY" in yml
    assert "src.jev_gate" in yml
    assert "src.test_jev_gate" in yml
    assert "src.factor_mine" not in yml
    assert "src.flatten" not in yml
    # no unindented print-in-pipe footgun
    for line in yml.splitlines():
        if line.startswith("print("):
            raise AssertionError("unindented print in workflow")


def main() -> None:
    tests = [
        test_normalize_strips_source_suffix_and_punct,
        test_tokens_and_jaccard_dup,
        test_dedup_keeps_earliest_wire,
        test_regex_trash_and_source_deny,
        test_calendar_day_rfc2822,
        test_reprint_clock_stale_vs_new_verb,
        test_code_does_not_geo_drop_yemen_or_palestine,
        test_decide_palestine_vs_trump,
        test_decide_core_and_chokepoint,
        test_decide_fact_vetoes_opinion,
        test_live_shaped_gold_misses,
        test_code_hints_closed_lists,
        test_code_reason_short_circuits_jev,
        test_questions_are_hop0_only,
        test_parse_answers_and_state,
        test_jev_post_body_and_429_retry,
        test_mocked_gold_full_pipeline,
        test_code_only_gold_leaves_jev_rows,
        test_gate_does_not_call_jev_on_code_drops,
        test_gold_fixture_has_must_keep_set,
        test_load_titles_reads_parsed_all_items,
        test_mine_junk_shapes_runs,
        test_api_key_not_in_repo_and_env,
        test_session_409_false_drops_are_keeps,
        test_session_409_forecast_tape_is_not_classifiable,
        test_workflow_wires_secret_and_stays_stdlib,
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
