"""0808 sheet comments plus the lock so later patches cannot unfix 0720.

Live path tests mock Jev BIT_QUESTIONS answers (no HTTP).
"""
from __future__ import annotations

import json
from pathlib import Path

from .jev_bits import BIT_QUESTIONS, decide
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


def test_jev_answers_keep_the_sheet_misses() -> None:
    """Frontier-shaped answers keep the last-sheet false drops."""
    cases = [
        ("Nasdaq to Buy Dark Pool Stock Venue LeveL for Equity Trading", {"k_done": 0.92}),
        ("US July PPI Below Expectations as Producer Inflation Cools Significantly", {"k_print": 0.91}),
        ("Nigeria's Dangote Refinery secures $1 billion underwriting ahead of IPO", {"k_done": 0.90}),
        ("Guardant ordered to pay $245m in DNA sequencing patent dispute", {"k_done": 0.88}),
        ("Bitdeer shares edge higher despite Q2 earnings and revenue miss", {"k_earn": 0.86}),
        ("Nvidia’s $10.2 Billion Quarterly Profit Increase Topped Its Entire 2022 Operating Profit", {"k_earn": 0.87}),
        ("Fed's Williams: No Rush on Rate Hikes, But One More Increase Likely This Year", {"k_print": 0.90}),
        ("Beth Hammack urges Fed rate hike to fight above-3% inflation", {"k_print": 0.85}),
        ("As Jackson Hole conference kicks off, three Fed officials issue inflation warnings", {"k_print": 0.88}),
        ("Consumer confidence sags to 12-year low, eroded by inflation, job anxiety", {"k_print": 0.84}),
    ]
    for title, fired in cases:
        got = _dj(title, _ans(**fired))
        assert got["decision"] == "keep", (title, got)


def test_jev_answers_drop_the_sheet_false_keeps() -> None:
    cases = [
        ("Capita Flags CSPS Costs but Touts Contract Wins, Savings and AI Growth", {"v_fluff": 0.82}),
        ("‘Impact on bilateral relations’: India flags concerns over 100% US tariffs", {"v_fluff": 0.80}),
        ("Asia stocks gain ahead of U.S. PCE inflation; regional data in focus", {"v_week": 0.88}),
        ("US core PCE inflation expected to increase, challenging the Fed", {"v_odds": 0.86}),
        ("China Won't Move Chip Stocks Anymore (NASDAQ:SMH) - Seeking Alpha", {}, "Seeking Alpha"),
        ("3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes", {"v_tipsheet": 0.93, "k_print": 0.80}),
    ]
    for row in cases:
        title, fired = row[0], row[1]
        source = row[2] if len(row) > 2 else ""
        got = _dj(title, _ans(**fired), source)
        assert got["decision"] == "drop", (title, got)


def test_jev_answers_override_regex_false_keep() -> None:
    title = "Capita Flags CSPS Costs but Touts Contract Wins, Savings and AI Growth"
    assert _d(title)["decision"] == "keep"
    assert _dj(title, _ans(v_fluff=0.9))["decision"] == "drop"


def test_gate_uses_posted_bit_answers() -> None:
    title = "Nasdaq to Buy Dark Pool Stock Venue LeveL for Equity Trading"

    def poster(state, questions, key):
        assert "k_done" in questions
        assert "event_class" not in questions
        return {
            "model": "jev-test",
            "answers": {
                "k_done": {"type": "noul", "noul": 0.91},
                "v_tipsheet": {"type": "noul", "noul": 0.04},
            },
        }

    out = gate(
        [{"title": title, "source": "Bloomberg", "id": "nasdaq-buy"}],
        code_only=False, live=True, key="x", poster=poster,
    )
    assert out[0]["decision"] == "keep"
    assert out[0]["reason"] == "k_done"


def main() -> None:
    tests = [
        test_locked_titles_do_not_flip,
        test_sheet_0808_commented_keeps,
        test_sheet_0808_stay_drop,
        test_jev_answers_keep_the_sheet_misses,
        test_jev_answers_drop_the_sheet_false_keeps,
        test_jev_answers_override_regex_false_keep,
        test_gate_uses_posted_bit_answers,
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
