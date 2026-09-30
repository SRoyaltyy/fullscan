"""0808 sheet comments plus the lock so later patches cannot unfix 0720."""
from __future__ import annotations

import json
from pathlib import Path

from .jev_bits import decide

LOCK_PATH = Path(__file__).with_name("jev_bits_lock.json")


def _d(title: str, source: str = "") -> dict:
    return decide({"title": title, "source": source}, None)


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


def main() -> None:
    tests = [
        test_locked_titles_do_not_flip,
        test_sheet_0808_commented_keeps,
        test_sheet_0808_stay_drop,
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
