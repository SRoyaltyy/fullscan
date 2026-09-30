"""0808 sheet comments plus the lock so later patches cannot unfix 0720.

Live path tests mock the six frozen bits (no HTTP). Offline lock uses regex.
"""
from __future__ import annotations

import json
from pathlib import Path

from .jev_bits import BIT_QUESTIONS, SIX_BITS, decide, formula_reason
from .jev_gate import gate

LOCK_PATH = Path(__file__).with_name("jev_bits_lock.json")


def _d(title: str, source: str = "") -> dict:
    return decide({"title": title, "source": source}, None)


def _ans(**fired: float) -> dict:
    out = {key: 0.05 for key in BIT_QUESTIONS}
    out.update(fired)
    return out


def _dj(title: str, answers: dict, source: str = "") -> dict:
    return decide({"title": title, "source": source}, answers)


def test_six_frozen_questions() -> None:
    assert tuple(BIT_QUESTIONS) == SIX_BITS
    assert len(BIT_QUESTIONS) == 6
    for key, spec in BIT_QUESTIONS.items():
        assert spec["type"] == "noul"
        crit = spec["criteria"]
        assert crit["true"] and crit["false"]


def test_formula_keep_is_done_or_print_without_vetoes() -> None:
    assert formula_reason(_ans(done=0.9)) == ("keep", "done")
    assert formula_reason(_ans(print=0.9)) == ("keep", "print")
    assert formula_reason(_ans(done=0.9, print=0.9)) == ("keep", "print")
    assert formula_reason(_ans(listed=0.95)) == ("drop", "no_keep_bit")
    assert formula_reason(_ans(done=0.9, tape=0.9)) == ("drop", "tape")
    assert formula_reason(_ans(print=0.9, soft=0.9)) == ("drop", "soft")
    assert formula_reason(_ans(print=0.9, tip=0.9)) == ("drop", "tip")


def test_listed_alone_does_not_keep() -> None:
    got = _dj(
        "Bakery buys comedian's former sites after closure",
        _ans(listed=0.92),
    )
    assert got["decision"] == "drop"
    assert got["reason"] == "no_keep_bit"


def test_locked_titles_do_not_flip() -> None:
    blob = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    assert blob.get("schema") == "jev-bits-lock-1"
    failed = []
    for item in blob.get("items") or []:
        title = item["title"]
        expect = item["expect"]
        got = _d(title)["decision"]
        if got != expect:
            failed.append(f"{item.get('lock')} want={expect} got={got} | {title[:90]}")
    assert not failed, "bits regression:\n" + "\n".join(failed)


def test_sheet_0808_commented_keeps() -> None:
    assert _d("Bank of America launches $250 billion infrastructure finance initiative")["decision"] == "keep"
    assert _d("Tech stocks today: Anthropic investors aiming for $2 trillion IPO")["reason"] == "k_done"
    assert _d(
        "Stock Yards Bancorp Reports Record Second Quarter Earnings of "
        "$40.1 Million or $1.31 Per Diluted Share - Yahoo Finance Australia"
    )["reason"] == "k_earn"
    crwd = _d("CrowdStrike strong earnings spark cybersecurity rally lifting Fortinet 5%")
    assert crwd["decision"] == "keep" and crwd["reason"] == "k_earn"
    pipe = _d("Saudi East-West (Petroline) pipeline still shut after drone strikes")
    assert pipe["decision"] == "keep" and pipe["reason"] == "k_choke"
    steel = _d(
        "Geopolitical tensions increase the cost of transportation "
        "and change the flow of steel products"
    )
    assert steel["decision"] == "keep" and steel["reason"] == "k_choke"
    assert _d(
        "The U.S. Federal Reserve raised its benchmark interest rate "
        "for the first time in three years and tw.."
    )["reason"] == "k_print"
    yld = _d(
        "30-year Treasury yield hits highest level since 2004 "
        "as bond market rout continues - CNBC"
    )
    assert yld["decision"] == "keep" and yld["reason"] == "k_print"


def test_sheet_0808_stay_drop() -> None:
    assert _d("Once Upon A Farm Q2 Earnings Call Highlights")["reason"] == "v_week"
    assert _d("$10,000 invested at SpaceX stock IPO is now worth - Finbold")["decision"] == "drop"
    assert _d(
        "Micron Stock: Earnings May Show Why $100 Billion Won’t Save The Rally - Forbes"
    )["decision"] == "drop"
    assert _d(
        "Nifty 50 down 13.5% in 2026, set for worst year in 15 years: "
        "Key factors weighing on market sentiment - Livemint"
    )["reason"] == "v_tape"
    assert _d(
        "IP Group (LON:IPO) Stock Price Crosses Above 200-Day Moving Average "
        "- Time to Sell? - MarketBeat",
        "MarketBeat",
    )["decision"] == "drop"


def test_formula_keeps_barr_iran_and_drops_transcript() -> None:
    barr = _dj(
        "Barr Says Further Fed Rate Hikes Likely Needed As Inflation Remains Too High",
        _ans(print=0.91),
    )
    assert barr["decision"] == "keep" and barr["reason"] == "print"
    iran = _dj(
        "Oil climbs as Trump denies Iran sanctions easing reports - Invezz",
        _ans(done=0.88),
    )
    assert iran["decision"] == "keep" and iran["reason"] == "done"
    transcript = _dj("Chiron Q2 Earnings Call Transcript", _ans(soft=0.93, done=0.2))
    assert transcript["decision"] == "drop"
    assert transcript["reason"] in {"v_week", "soft"}


def test_done_beats_ignored_legacy_keys() -> None:
    title = "Nasdaq to Buy Dark Pool Stock Venue LeveL for Equity Trading"
    got = _dj(title, _ans(done=0.91, v_fluff=0.88))
    assert got["decision"] == "keep"
    assert got["reason"] == "done"


def test_answers_do_not_fall_back_to_code_print() -> None:
    title = "Barr Says Further Fed Rate Hikes Likely Needed As Inflation Remains Too High"
    got = _dj(title, _ans())
    assert got["decision"] == "drop"
    assert got["reason"] == "no_keep_bit"
    assert got.get("noul")


def test_jev_answers_keep_the_sheet_misses() -> None:
    """Frontier-shaped six-bit answers keep the last-sheet false drops."""
    cases = [
        ("Nasdaq to Buy Dark Pool Stock Venue LeveL for Equity Trading", {"done": 0.92}),
        ("US July PPI Below Expectations as Producer Inflation Cools Significantly", {"print": 0.91}),
        ("Nigeria's Dangote Refinery secures $1 billion underwriting ahead of IPO", {"done": 0.90}),
        ("Guardant ordered to pay $245m in DNA sequencing patent dispute", {"done": 0.88}),
        ("Bitdeer shares edge higher despite Q2 earnings and revenue miss", {"done": 0.86}),
        ("Nvidia’s $10.2 Billion Quarterly Profit Increase Topped Its Entire 2022 Operating Profit", {"done": 0.87}),
        ("Fed's Williams: No Rush on Rate Hikes, But One More Increase Likely This Year", {"print": 0.90}),
        ("Beth Hammack urges Fed rate hike to fight above-3% inflation", {"print": 0.85}),
        ("As Jackson Hole conference kicks off, three Fed officials issue inflation warnings", {"print": 0.88}),
        ("Consumer confidence sags to 12-year low, eroded by inflation, job anxiety", {"print": 0.84}),
    ]
    for title, fired in cases:
        got = _dj(title, _ans(**fired))
        assert got["decision"] == "keep", (title, got)


def test_jev_answers_drop_the_sheet_false_keeps() -> None:
    cases = [
        ("Capita Flags CSPS Costs but Touts Contract Wins, Savings and AI Growth", {}),
        ("‘Impact on bilateral relations’: India flags concerns over 100% US tariffs", {}),
        ("Asia stocks gain ahead of U.S. PCE inflation; regional data in focus", {"soft": 0.88}),
        ("US core PCE inflation expected to increase, challenging the Fed", {"soft": 0.86}),
        ("China Won't Move Chip Stocks Anymore (NASDAQ:SMH) - Seeking Alpha", {}, "Seeking Alpha"),
        ("3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes", {"tip": 0.93, "print": 0.80}),
        ("Kadant Q2 Earnings Call Highlights", {"soft": 0.94}),
        ("Archer Aviation vs. AST SpaceMobile: Which Industrials Stock Is a Better Buy in 2026?", {"tip": 0.91}),
    ]
    for row in cases:
        title, fired = row[0], row[1]
        source = row[2] if len(row) > 2 else ""
        got = _dj(title, _ans(**fired), source)
        assert got["decision"] == "drop", (title, got)


def test_jev_answers_override_regex_false_keep() -> None:
    title = "Capita Flags CSPS Costs but Touts Contract Wins, Savings and AI Growth"
    assert _d(title)["decision"] == "keep"
    assert _dj(title, _ans())["decision"] == "drop"


def test_gate_uses_posted_bit_answers() -> None:
    title = "Nasdaq to Buy Dark Pool Stock Venue LeveL for Equity Trading"

    def poster(state, questions, key):
        assert "done" in questions
        assert "print" in questions
        assert len(questions) == 6
        assert "k_done" not in questions
        assert "event_class" not in questions
        return {
            "model": "jev-test",
            "answers": {
                "done": {"type": "noul", "noul": 0.91},
                "tip": {"type": "noul", "noul": 0.04},
            },
        }

    out = gate(
        [{"title": title, "source": "Bloomberg", "id": "nasdaq-buy"}],
        code_only=False, live=True, key="x", poster=poster,
    )
    assert out[0]["decision"] == "keep"
    assert out[0]["reason"] == "done"


def test_no_cheap_keep() -> None:
    import src.jev_bits as bits

    assert not hasattr(bits, "cheap_keep")
    assert not hasattr(bits, "FED_COLON_RE")
    assert not hasattr(bits, "OFFICIAL_PRINT_RE")


def main() -> None:
    tests = [
        test_six_frozen_questions,
        test_formula_keep_is_done_or_print_without_vetoes,
        test_listed_alone_does_not_keep,
        test_locked_titles_do_not_flip,
        test_sheet_0808_commented_keeps,
        test_sheet_0808_stay_drop,
        test_formula_keeps_barr_iran_and_drops_transcript,
        test_done_beats_ignored_legacy_keys,
        test_answers_do_not_fall_back_to_code_print,
        test_jev_answers_keep_the_sheet_misses,
        test_jev_answers_drop_the_sheet_false_keeps,
        test_jev_answers_override_regex_false_keep,
        test_gate_uses_posted_bit_answers,
        test_no_cheap_keep,
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
