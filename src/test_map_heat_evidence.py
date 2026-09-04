"""Strict captain research contracts and scheduling helpers."""
from __future__ import annotations

from src.map_heat_evidence import opportunity_tickers_valid, validate_cards
from src.map_heat_postclose import honest_none_cards, next_weekday
from src.map_heat_research import extract_json, salvage_cards


TARGET = {
    "industry": "Uranium", "sector": "Energy", "action": "OVERRIDE",
    "spx_leaders": [],
    "rut_leaders": [{"ticker": "UEC"}, {"ticker": "UUUU"}],
}


def test_valid_evidence_card() -> None:
    cards, errors = validate_cards([{
        "industry": "Uranium", "sector": "Energy", "action": "OVERRIDE",
        "subsector_dir": "up", "conviction": "high",
        "captains": [{
            "ticker": "UEC", "sent": "pos", "why": "contract",
            "evidence": [{"source": "Reuters", "url": "https://reuters.com/x",
                          "published_at": "2026-08-26T06:00:00-04:00",
                          "fact": "signed a supply contract"}],
        }, {
            "ticker": "UUUU", "sent": "none",
            "search_note": "searched UUUU filings/news/X; nothing current",
            "evidence": [],
        }],
    }], [TARGET], min_coverage=1.0)
    assert not errors
    assert len(cards) == 1
    assert cards[0]["captains"][0]["index"] == "RUT"


def test_rejects_invented_ticker_and_unsupported_sentiment() -> None:
    cards, errors = validate_cards([{
        "industry": "Uranium", "action": "OVERRIDE",
        "subsector_dir": "up", "conviction": "high",
        "captains": [{"ticker": "FAKE", "sent": "pos", "evidence": []}],
    }], [TARGET], min_coverage=1.0)
    assert not cards
    assert any("invented_ticker" in e for e in errors)


def test_opportunities_must_be_captains() -> None:
    errors = opportunity_tickers_valid(
        {"opportunities": [{"tickers": ["UEC", "FAKE"]}]},
        [{"captains": [{"ticker": "UEC"}]}],
    )
    assert errors == ["opportunity_invented_ticker:FAKE"]


def test_morning_coerces_used_true_without_delta() -> None:
    raw = {
        "industry": "Uranium", "sector": "Energy", "action": "OVERRIDE",
        "subsector_dir": "up", "conviction": "medium",
        "captains": [{
            "ticker": "UEC", "sent": "none", "evidence": [],
            "search_note": "web searched; no new item",
            "x_sentiment": {"used": True, "label": "pos"},
        }],
    }
    cards, errors = validate_cards(
        [raw], [TARGET], min_coverage=1.0, require_x_record=True)
    assert not errors
    assert len(cards) == 1
    xs = cards[0]["captains"][0]["x_sentiment"]
    assert xs["used"] is False
    assert "mention_delta" in xs["reason"]


def test_morning_requires_x_record() -> None:
    raw = {
        "industry": "Uranium", "sector": "Energy", "action": "OVERRIDE",
        "subsector_dir": "up", "conviction": "medium",
        "captains": [{
            "ticker": "UEC", "sent": "none", "evidence": [],
            "search_note": "web searched; no new item",
        }],
    }
    cards, errors = validate_cards(
        [raw], [TARGET], min_coverage=1.0, require_x_record=True)
    assert not cards
    assert any("missing_x_search_record" in e for e in errors)


def test_next_weekday() -> None:
    assert next_weekday("2026-08-28") == "2026-08-31"
    assert next_weekday("2026-08-26") == "2026-08-27"
    # Labor Day 2026-09-07 is NYSE closed — Friday → Tuesday.
    assert next_weekday("2026-09-04") == "2026-09-08"


def test_extract_json_salvages_truncated_cards() -> None:
    text = (
        "```json\n{\n  \"date\": \"2026-09-08\",\n  \"cards\": [\n"
        "    {\n      \"industry\": \"Uranium\",\n      \"sector\": \"Energy\",\n"
        "      \"action\": \"OVERRIDE\",\n      \"subsector_dir\": \"up\",\n"
        "      \"conviction\": \"medium\",\n      \"captains\": [{\n"
        "        \"ticker\": \"UEC\", \"sent\": \"none\",\n"
        "        \"search_note\": \"searched UEC; nothing current\",\n"
        "        \"evidence\": []\n      }]\n    },\n"
        "    {\n      \"industry\": \"Oil & Gas E&P\",\n      \"sector\": \"Energy\",\n"
        "      \"action\": \"HEAT\",\n      \"subsector_dir\": \"down\",\n"
        "      \"conviction\": \"low\",\n      \"captains\": [{\n"
        "        \"ticker\": \"DVN\", \"sent\": \"pos\", \"why\": \"beat\",\n"
        "        \"evidence\": [{\n"
        "          \"source\": \"Reuters\",\n"
        "          \"url\": \"https://reuters.com/a\",\n"
        "          \"published_at\": \"2026-09-04T12:00:00-04:00\",\n"
        "          \"fact\": \"beat estimates\"\n"
        "        }]\n      }]\n    },\n"
        "    {\n      \"industry\": \"TRUNCATED\",\n"
        "      \"captains\": [{\"ticker\": \"XX\",\n"
    )
    obj = extract_json(text)
    assert obj is not None
    industries = {c["industry"] for c in obj["cards"]}
    assert industries == {"Uranium", "Oil & Gas E&P"}
    assert salvage_cards(text) == obj["cards"]


def test_honest_none_cards_validate() -> None:
    missing = [{
        "industry": "Uranium", "sector": "Energy", "action": "OVERRIDE",
        "spx_leaders": [],
        "rut_leaders": [{"ticker": "UEC", "why": "US energy firms dominate"}],
    }]
    raw = honest_none_cards(missing)
    cards, errors = validate_cards(raw, missing, min_coverage=1.0)
    assert not errors
    assert len(cards) == 1
    cap = cards[0]["captains"][0]
    assert cap["sent"] == "none"
    assert "Finviz stub" in cap["search_note"]


if __name__ == "__main__":
    test_valid_evidence_card()
    test_rejects_invented_ticker_and_unsupported_sentiment()
    test_opportunities_must_be_captains()
    test_morning_coerces_used_true_without_delta()
    test_morning_requires_x_record()
    test_next_weekday()
    test_extract_json_salvages_truncated_cards()
    test_honest_none_cards_validate()
    print("8 tests passed")
