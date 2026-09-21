"""Golden case studies + parse backtest for the news-impact router."""
from __future__ import annotations

from pathlib import Path

from src.lane_route import (
    NEWS_HEAD,
    NEWS_TEMPLATES,
    inbox_error,
    lanes_for,
    primary_models_for,
    prompt_for,
    token_budget,
)
from src.news_impact.backtest import markdown, run_backtest
from src.news_impact.classify import classify_text, harvest_rank_score
from src.news_impact.grade import grade_one, parse_when
from src.news_impact.pipeline import analyze_article
from src.news_impact.schema import PIPELINE_VERSION, is_tradable, is_usable

ROOT = Path(__file__).resolve().parents[1]


def _ticks(row: dict, direction: str | None = None) -> set[str]:
    out = set()
    for e in row.get("entities") or []:
        if direction and e.get("direction") != direction:
            continue
        if e.get("ticker"):
            out.add(e["ticker"])
    return out


def _cls(row: dict) -> str:
    return (row.get("classification") or {}).get("event_class") or ""


def test_lane_templates_on_news_hopper() -> None:
    assert "news_classify" in NEWS_TEMPLATES
    assert "news_impact" in NEWS_TEMPLATES
    for tmpl in NEWS_TEMPLATES:
        assert lanes_for(tmpl)[:4] == NEWS_HEAD
        assert primary_models_for("zhipu", tmpl) == ["glm-4.7-flash"]
        assert "glm-4-flash-250414" not in primary_models_for("zhipu", tmpl)
    assert token_budget("news_classify") == 400
    assert token_budget("news_impact") == 900
    assert inbox_error({"template": "news_classify"}) == (
        "news_classify needs articles[{title,body}]"
    )
    q = {
        "template": "news_classify",
        "articles": [{"title": "Oil jump", "body": "Brent spiked"}],
    }
    assert inbox_error(q) is None
    _, tmpl, _, prompt = prompt_for(q)
    assert tmpl == "news_classify"
    assert "event_class" in prompt
    assert "No tickers" in prompt or "no tickers" in prompt.lower()


def test_tsa_catches_rental_cars() -> None:
    row = analyze_article(
        {"title": "Government shutdown leads to chaos at US airports as TSA officers go unpaid on busy travel weekend"},
        persist=False,
    )
    assert _cls(row) == "blast_ops"
    assert row["classification"]["q5"] == "impulse"
    assert "CAR" in _ticks(row, "up")
    assert _ticks(row, "down") & {"AAL", "DAL", "UAL", "LUV"}
    assert row["usable"] is True


def test_wh_ban_refuses_fake_media_trade() -> None:
    row = analyze_article(
        {"title": "Donald Trump bans CNN, MS NOW, and Politico from White House"},
        persist=False,
    )
    assert row["classification"]["q5"] == "regime"
    assert row["entities"] == []
    assert row["usable"] is False


def test_hormuz_reprint_is_weather() -> None:
    row = analyze_article(
        {"title": "Why Hormuz remains high risk for ships despite US claims of mine-clearing"},
        persist=False,
    )
    assert _cls(row) == "regime_state"
    assert row["classification"]["q5"] == "regime"
    assert row["entities"] == []
    assert row["usable"] is False


def test_hormuz_ceasefire_is_regime_break() -> None:
    row = analyze_article(
        {"title": "Iran and US announce a formal ceasefire — both navies withdraw from the Hormuz strait"},
        persist=False,
    )
    assert _cls(row) == "regime_break"
    assert row["classification"]["q5"] == "regime_break"
    assert "XLE" in _ticks(row, "down")
    assert row["usable"] is True


def test_buist_meta_unscathed_nvda_not_determined() -> None:
    row = analyze_article(
        {"title": (
            "Four paying subscribers filed a class-action antitrust lawsuit, "
            "Buist et al. v. Anthropic PBC et al., accusing Anthropic, OpenAI, "
            "xAI (SpaceXAI), and Google of an illegal agreement to slow AI"
        )},
        persist=False,
    )
    assert _cls(row) == "blast_legal"
    assert "GOOGL" in _ticks(row, "down")
    assert "META" in _ticks(row, "up")
    nvda = [e for e in row["entities"] if e.get("ticker") == "NVDA"]
    assert nvda and nvda[0]["direction"] == "not_determined"
    assert nvda[0]["role"] == "arms_dealer"
    meta = [e for e in row["entities"] if e.get("ticker") == "META"][0]
    assert meta["role"] == "unscathed_rival"
    assert meta["wins_either_outcome"] is True


def test_sec_tsv_market_structure() -> None:
    row = analyze_article(
        {"title": (
            "SEC issues order granting temporary exemptive relief to Tokenized "
            "Securities Venues to trade tokenized NMS stock"
        )},
        persist=False,
    )
    assert _cls(row) == "market_structure"
    assert "COIN" in _ticks(row, "up")
    assert "CRCL" in _ticks(row, "up")
    assert "NDAQ" in _ticks(row, "mixed")
    hood = [e for e in row["entities"] if e.get("ticker") == "HOOD"]
    assert hood and hood[0]["direction"] == "mixed"


def test_jet_fuel_not_energy_bear() -> None:
    row = analyze_article(
        {"title": "Airlines and Cruise Stocks Face New Oil Shock as jet fuel lifts costs"},
        persist=False,
    )
    assert _cls(row) == "input_cost"
    assert "AAL" in _ticks(row, "down")
    assert "XLE" in _ticks(row, "up")
    assert "XLE" not in _ticks(row, "down")


def test_aame_not_technology() -> None:
    row = analyze_article(
        {"title": "Atlantic American Corporation Receives Nasdaq Notice Regarding Delayed Second Quarter Form 10-Q Filing"},
        persist=False,
    )
    assert _cls(row) == "integrity"
    ticks = {e.get("ticker") for e in row["entities"]}
    assert "AAME" in ticks
    assert "NDAQ" not in ticks
    for e in row["entities"]:
        assert e.get("ticker") != "XLK"
        assert "Technolog" not in str(e.get("name") or "")


def test_agilent_dividend_not_empty_not_sector_trade() -> None:
    row = analyze_article(
        {"title": "Agilent Announces Cash Dividend of 25.5 Cents per Share"},
        persist=False,
    )
    assert _cls(row) == "capital_return"
    assert row["entities"], "empty lists are lazy — name the issuer"
    assert all(e.get("direction") in {"not_determined", "mixed", "up"} for e in row["entities"])


def test_asml_sold_out_rescued() -> None:
    row = analyze_article(
        {"title": "ASML nearly sold out of 2027 EUV capacity amid very strong AI-driven demand, JPMorgan says"},
        persist=False,
    )
    assert _cls(row) == "capacity"
    assert "ASML" in _ticks(row, "up")
    assert row["usable"] is True


def test_bsx_cyber_rescued() -> None:
    row = analyze_article(
        {"title": "Boston Scientific says cyberattack will materially hit Q3 and full-year 2026 results"},
        persist=False,
    )
    assert _cls(row) == "blast_cyber"
    assert "BSX" in _ticks(row, "down")
    assert row["usable"] is True


def test_gold_fed_reprint_killed() -> None:
    row = analyze_article(
        {"title": "Gold price surge on Fed rate cut bets lifts Barrick Mining (B) 8.21%"},
        persist=False,
    )
    assert _cls(row) == "regime_state"
    assert row["usable"] is False


def test_harvest_rank_gov_before_az() -> None:
    sec = harvest_rank_score("SEC issues tokenized securities venue exemption")
    div = harvest_rank_score("Agilent Announces Cash Dividend of 25.5 Cents per Share")
    assert sec > div


def test_watermark_deterministic() -> None:
    row = analyze_article({"title": "Amgen gets FDA approval to update IMDELLTRA label"}, persist=False)
    assert row["lane"] == "deterministic"
    assert row["model"] == PIPELINE_VERSION
    assert row["inference_source"] == "deterministic"
    assert _cls(row) == "gate"
    assert "AMGN" in _ticks(row, "up")


def test_usable_helper() -> None:
    from src.news_impact.schema import Classification, Entity
    weather = Classification("regime_state", None, "regime", "hormuz")
    assert is_usable(weather, []) is False
    hit = Classification("blast_ops", None, "impulse", "tsa")
    assert is_usable(hit, [Entity("Avis", "CAR", "substitute", "up")]) is True
    assert is_tradable(hit, [Entity("Avis", "CAR", "substitute", "up")]) is True
    nd = Entity("broad US equity", "SPY", "named", "not_determined")
    assert is_usable(hit, [nd]) is True
    assert is_tradable(hit, [nd]) is False


def test_macro_cooler_cpi_trades_factor_basket() -> None:
    row = analyze_article(
        {"title": "Cooler CPI inflation reading bolsters case for Fed to hold rates in September"},
        persist=False,
    )
    assert _cls(row) == "factor_impulse"
    assert row["classification"]["q5"] == "impulse"
    assert row["classification"]["factor"] == "inflation"
    assert row["tradable"] is True
    assert "TLT" in _ticks(row, "up")
    assert "QQQ" in _ticks(row, "up")
    assert "UUP" in _ticks(row, "down")
    # Never a single-name from the lede.
    assert "AMZN" not in _ticks(row)


def test_macro_hot_retail_sales_is_hawkish() -> None:
    row = analyze_article(
        {"title": "Retail Sales, Imports Come In Warm Ahead of FOMC Decision"},
        persist=False,
    )
    assert _cls(row) == "factor_impulse"
    assert row["tradable"] is True
    assert "QQQ" in _ticks(row, "down")
    assert "TLT" in _ticks(row, "down")
    assert "UUP" in _ticks(row, "up")


def test_macro_fed_speech_is_weather() -> None:
    row = analyze_article(
        {"title": "Investors seek clearer Fed guidance from Warsh at Jackson Hole address"},
        persist=False,
    )
    assert row["classification"]["q5"] == "regime"
    assert row["entities"] == []
    assert row["usable"] is False
    assert row["tradable"] is False


def test_macro_hike_odds_reprint_is_weather() -> None:
    row = analyze_article(
        {"title": "Fed Rate Hike Odds Fall As Amazon Prime Day Effect Hits Retail Sales"},
        persist=False,
    )
    assert row["usable"] is False
    assert row["tradable"] is False
    assert "AMZN" not in _ticks(row)


def test_macro_tariff_rumor_not_tradable() -> None:
    row = analyze_article(
        {"title": "U.S. considers fresh round of tariffs on semiconductors, report says"},
        persist=False,
    )
    assert row["tradable"] is False
    assert row["classification"]["q5"] in {"regime", "impulse"}
    if row["usable"]:
        assert row["classification"]["event_class"] in {"rumor", "regime_state", "discard"}


def test_macro_hammack_speech_is_weather() -> None:
    row = analyze_article(
        {"title": "Fed should raise rates to restrain growth and inflation, Hammack says - Reuters"},
        persist=False,
    )
    assert row["usable"] is False
    assert row["tradable"] is False


def test_macro_if_fed_hikes_newsletter_is_weather() -> None:
    row = analyze_article(
        {"title": "Stocks that could rally if the Fed raises rates, or stocks that could win if it stays put"},
        persist=False,
    )
    assert row["usable"] is False
    assert row["tradable"] is False


def test_macro_tariff_imposed_is_risk_off() -> None:
    row = analyze_article(
        {"title": "U.S. announces tariffs on semiconductors and steel"},
        persist=False,
    )
    assert _cls(row) == "factor_impulse"
    assert row["tradable"] is True
    assert "SPY" in _ticks(row, "down")
    assert "TLT" in _ticks(row, "up")
    assert "GLD" in _ticks(row, "up")


def test_backtest_improves_sept_parses() -> None:
    """09-17 + 09-18 pipeline parses: rescue real movers, kill gold/Hormuz weather."""
    r17 = run_backtest("2026-09-17", persist=False)
    r18 = run_backtest("2026-09-18", persist=False)
    for report, label in ((r17, "09-17"), (r18, "09-18")):
        old_r = (report.get("old") or {}).get("usable_ratio") or 0
        new_r = (report.get("new") or {}).get("usable_ratio") or 0
        assert report["rescued_n"] >= 8, (label, report["rescued_n"])
        assert report["killed_n"] >= 1, (label, report["killed_n"])
        # Quality: more decisionable articles than the 6% regex usable set.
        assert new_r > old_r, (label, old_r, new_r)
        titles = " | ".join(x["title"] for x in report.get("rescued_sample") or [])
        assert "ASML" in titles or "cyberattack" in titles.lower() or "CHIPS" in titles or "EUV" in titles
    # Combined all-dates smoke (no persist).
    hall = run_backtest("all", persist=False)
    assert hall["harvested"] >= 500
    assert hall["rescued_n"] > hall["killed_n"]
    assert (hall.get("new") or {}).get("usable_ratio", 0) > (
        hall.get("old") or {}
    ).get("usable_ratio", 0)


def test_reasoning_and_times_on_article() -> None:
    row = analyze_article(
        {
            "title": "Airlines Scramble for Jet Fuel as Hormuz Disruption Drags On",
            "published_at": "Wed, 17 Sep 2026 08:00:00 -0400",
            "retrieved_at": "2026-09-17T04:17:45-04:00",
            "source": "rss_yahoo_finance",
        },
        persist=False,
    )
    assert row["published_at"].startswith("Wed, 17 Sep 2026")
    assert row["retrieved_at"].startswith("2026-09-17")
    assert row["models"] == [f"deterministic::{PIPELINE_VERSION}"]
    assert row["reasoning"] and row["reasoning"][0].startswith("Q5")
    assert "input_cost" in row["reasoning"][1]
    assert row["conclusion"]["down"]
    assert "XLE" in " ".join(row["conclusion"]["up"])
    hops = row["hop_chain"]
    assert hops and hops[0]["lane"] == "deterministic"


def test_grade_entity_agrees_on_up() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    bars = [
        {"date": "2026-09-17", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5},
        {"date": "2026-09-18", "open": 10.5, "high": 12.0, "low": 10.4, "close": 11.8},
        {"date": "2026-10-15", "open": 12.0, "high": 12.2, "low": 11.9, "close": 12.1},
    ]
    # pad to 20 sessions so 1-4w can resolve
    day = 19
    last = 12.1
    while len(bars) < 22:
        day += 1
        last += 0.1
        bars.append({
            "date": f"2026-10-{day:02d}" if day <= 31 else f"2026-11-{day-31:02d}",
            "open": last, "high": last + 0.2, "low": last - 0.1, "close": last + 0.05,
        })
    when = datetime(2026, 9, 17, 8, 0, tzinfo=et)
    g = grade_one(
        {"ticker": "AAL", "name": "AAL", "direction": "up", "horizon": "0-1d", "kind": "ticker"},
        bars, when,
    )
    assert g["entry_date"] == "2026-09-17"
    assert g["ret_1d"] == 5.0
    assert g["agree_1d"] is True
    down = grade_one(
        {"ticker": "AAL", "name": "AAL", "direction": "down", "horizon": "0-1d"},
        bars, when,
    )
    assert down["agree_1d"] is False
    dt = parse_when("Thu, 27 Aug 2026 02:33:08 +0000")
    assert dt is not None
    assert dt.year == 2026 and dt.month == 8


def test_markdown_table_columns() -> None:
    report = run_backtest("2026-09-17", persist=False)
    md = markdown(report)
    for col in (
        "| Article |", "| Published |", "| Retrieved |", "| LLM(s) |",
        "| Reasoning |", "| Conclusion |", "| Actual |",
    ):
        assert col in md, col
    assert "deterministic::news_impact_v2" in md
    assert "Usable articles" in md
    assert "Discarded / weather" in md


def test_load_parsed_keeps_times() -> None:
    from pathlib import Path
    from src.news_impact.backtest import load_parsed
    rows = load_parsed(Path("01_daily/news/2026-08-27_parsed.json"))
    assert rows
    assert any(r.get("published_at") for r in rows)
    assert all(r.get("retrieved_at") for r in rows)
    assert any(r.get("source") for r in rows)


def test_search_pack_offline() -> None:
    from src.news_impact.search_pack import pack_for_article
    pack = pack_for_article("SEC tokenized stocks", enabled=False)
    assert pack["backend"] == "off"
    assert pack["facts"] == []


def main() -> None:
    tests = [
        test_lane_templates_on_news_hopper,
        test_tsa_catches_rental_cars,
        test_wh_ban_refuses_fake_media_trade,
        test_hormuz_reprint_is_weather,
        test_hormuz_ceasefire_is_regime_break,
        test_buist_meta_unscathed_nvda_not_determined,
        test_sec_tsv_market_structure,
        test_jet_fuel_not_energy_bear,
        test_aame_not_technology,
        test_agilent_dividend_not_empty_not_sector_trade,
        test_asml_sold_out_rescued,
        test_bsx_cyber_rescued,
        test_gold_fed_reprint_killed,
        test_harvest_rank_gov_before_az,
        test_watermark_deterministic,
        test_usable_helper,
        test_macro_cooler_cpi_trades_factor_basket,
        test_macro_hot_retail_sales_is_hawkish,
        test_macro_fed_speech_is_weather,
        test_macro_hike_odds_reprint_is_weather,
        test_macro_tariff_rumor_not_tradable,
        test_macro_hammack_speech_is_weather,
        test_macro_if_fed_hikes_newsletter_is_weather,
        test_macro_tariff_imposed_is_risk_off,
        test_search_pack_offline,
        test_reasoning_and_times_on_article,
        test_grade_entity_agrees_on_up,
        test_markdown_table_columns,
        test_load_parsed_keeps_times,
        test_backtest_improves_sept_parses,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
