"""Gold-fixture path for the Lane one-shot. The client is a stand-in for Lane.

Production never uses this client. The test fails if the stack calls
classify_article or families.analyze.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from src.news_impact.finviz_linker import candidate_rows, company_aliases
from src.news_impact.one_shot_stack import (
    GOLD_KEEP,
    GOLD_REJECT,
    _normalize_entities,
    classify_acceptable,
    confirm_instruments,
    gate0,
    gold_status,
    linker_acceptable,
    process_article,
    render_markdown,
)
from src.news_impact.axioms import load_axioms


def test_company_alias_lilly_and_novo():
    assert "eli lilly" in company_aliases("Lilly(Eli) & Co")
    assert "novo nordisk" in company_aliases("Novo Nordisk ADR")
    assert "avis budget" in company_aliases("Avis Budget Group Inc")


def test_gate0_rejects_weather_and_tape_and_keeps_fixtures():
    for art in GOLD_REJECT:
        assert gate0(art["title"], art["body"]), art["gold_id"]
    for art in GOLD_KEEP:
        assert gate0(art["title"], art["body"]) is None, art["gold_id"]
    assert gate0("Agilent Announces Cash Dividend of 25.5 Cents per Share") == "dividend_only"
    assert gate0("Alcoa Schedules Third Quarter 2026 Earnings Release and Conference Call") == "empty_ir"
    assert gate0("AAPL plunges after weak print") == "reaction_title"


def test_tsa_candidates_include_car_and_airlines():
    art = GOLD_KEEP[0]
    hit = candidate_rows(art["title"], art["body"], family="blast", event_class="blast_ops")
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert "CAR" in ticks
    assert ticks & {"AAL", "DAL", "UAL", "LUV"}
    assert len(hit["instruments"]) <= 40


def test_buist_candidates_include_googl_and_meta():
    art = next(a for a in GOLD_KEEP if a["gold_id"] == "buist")
    hit = candidate_rows(art["title"], art["body"], family="blast", event_class="blast_legal")
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert "GOOGL" in ticks
    assert "META" in ticks
    assert "VKTX" not in ticks


def test_tsv_candidates_are_venues_not_energy():
    art = next(a for a in GOLD_KEEP if a["gold_id"] == "tsv")
    hit = candidate_rows(
        art["title"], art["body"], family="structure", event_class="market_structure",
    )
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert ticks & {"COIN", "NDAQ", "ICE"}
    assert all(r["sector"] != "Energy" for r in hit["instruments"])
    assert len(hit["instruments"]) <= 40


def test_naion_links_nvo_lly_not_vktx():
    art = next(a for a in GOLD_KEEP if a["gold_id"] == "naion")
    hit = candidate_rows(art["title"], art["body"], family="blast", event_class="product_harm")
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert "NVO" in ticks
    assert "LLY" in ticks
    assert "VKTX" not in ticks
    assert "AMGN" not in ticks


def test_amrx_lanreotide_hits_amneal():
    art = next(a for a in GOLD_KEEP if a["gold_id"] == "amrx")
    hit = candidate_rows(art["title"], art["body"], family="permission", event_class="gate")
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert "AMRX" in ticks


def test_snapshot_hint_dropped_when_title_omits_the_firm():
    hit = candidate_rows(
        "VFLO rebalance adds a new holding",
        "",
        hint_ticker="MRNA",
    )
    ticks = {r["ticker"] for r in hit["instruments"]}
    assert "MRNA" not in ticks
    assert any(r["ticker"] == "MRNA" for r in hit["rejected_hints"])


class ScriptLane:
    """Records stages and returns a Lane-shaped JSON. Not a regex classifier."""

    def __init__(self):
        self.calls: list[str] = []
        self.prompts: list[tuple[str, str]] = []

    def _article_title(self, prompt: str) -> str:
        for line in prompt.splitlines():
            if line.lower().startswith("title:"):
                return line.split(":", 1)[1].strip().lower()
        return ""

    def __call__(self, stage, prompt, system, accept=None):
        self.calls.append(stage)
        self.prompts.append((stage, prompt))
        title = self._article_title(prompt)
        if stage == "classify":
            assert "candidates" not in prompt.lower()
            return self._classify(title), "zhipu", "glm-4.7-flash"
        if stage == "meta":
            assert "CANDIDATES" not in prompt
            return self._meta(title), "zhipu", "glm-4.7-flash"
        if stage == "pack_complete":
            return self._pack_complete(prompt), "zhipu", "glm-4.7-flash"
        if stage in {"linker", "filter"}:
            return self._link(prompt), "siliconflow", "Qwen/Qwen3-8B"
        return self._analyse(title), "qwen", "qwen-flash"

    def _meta(self, title: str) -> dict:
        if "tsa" in title:
            noun, question = "TSA", "Which harm does unpaid TSA impose on airports?"
        elif "openai" in title or "buist" in title:
            noun, question = "Google", "Which harm does the complaint name for Google?"
        elif "tokenized" in title:
            noun, question = "Venues", "Which expression did the SEC grant Tokenized Securities Venues?"
        elif "lanreotide" in title or "amneal" in title:
            noun, question = "Amneal", "Which clock can trade the Amneal lanreotide approval?"
        elif "naion" in title:
            noun, question = "NAION", "Which harm does the NAION wrap impose on Novo Nordisk?"
        else:
            noun, question = "article", "Which constraint does this article change?"
        return {
            "m1": {"need_context": "no"},
            "m2": [{
                "slot": "H",
                "noun": noun,
                "question": question,
                "changes": "direction",
                "blocks": ["direction"],
            }],
            "m3_dropped": [{"question": "what's the AI angle", "why": "ai_angle"}],
        }

    def _pack_complete(self, prompt: str) -> dict:
        slots = []
        for line in prompt.splitlines():
            if "question=" not in line:
                continue
            slots.append({
                "question": line.split("question=", 1)[1].strip(),
                "status": "answered",
            })
        return {
            "invert": "If that constraint had not changed, the direction would flip.",
            "slots": slots,
        }

    def _classify(self, title: str) -> dict:
        if "tsa" in title:
            return {"event_class": "blast_ops", "q5": "impulse", "sign": None,
                    "constraint": "TSA unpaid blocks flights on a travel weekend"}
        if "buist" in title or "openai" in title:
            return {"event_class": "blast_legal", "q5": "impulse", "sign": None,
                    "constraint": "class complaint names Google and the private labs"}
        if "tokenized" in title:
            return {"event_class": "market_structure", "q5": "impulse", "sign": "open",
                    "constraint": "SEC temporary venue permission for tokenized NMS stock"}
        if "lanreotide" in title or "amneal" in title:
            return {"event_class": "gate", "q5": "impulse", "sign": "open",
                    "constraint": "FDA approval lets Amneal launch lanreotide"}
        if "naion" in title:
            return {"event_class": "product_harm", "q5": "regime", "sign": None,
                    "constraint": "GLP-1 class wrap on NAION, not a new 0-1d print"}
        return {"event_class": "discard", "q5": "regime", "sign": None, "constraint": ""}

    def _link(self, prompt: str) -> dict:
        instruments = []
        for line in prompt.splitlines():
            if not line.startswith("- "):
                continue
            tick = line.split("|", 1)[0].replace("-", "").strip()
            if tick.isupper() and 1 <= len(tick) <= 5:
                instruments.append({"ticker": tick, "keep": True, "why": "named or industry hit"})
        return {"instruments": instruments}

    def _analyse(self, title: str) -> dict:
        if "tsa" in title:
            entities = [
                {"name": "American Airlines", "ticker": "AAL", "role": "named",
                 "direction": "down", "axiom_id": "A_AIR_01"},
                {"name": "Delta", "ticker": "DAL", "role": "named",
                 "direction": "down", "axiom_id": "A_AIR_01"},
                {"name": "Avis Budget", "ticker": "CAR", "role": "substitute",
                 "direction": "up", "axiom_id": "A_AIR_02"},
            ]
            answers = [
                {"id": "q1", "status": "answered", "note": "airlines cannot operate the full schedule"},
                {"id": "q2", "status": "answered", "note": "CAR is the listed rental substitute"},
                {"id": "q3", "status": "blocked", "note": "no unscathed airline named"},
                {"id": "q4", "status": "blocked", "note": "fuel demand unanswered"},
            ]
        elif "openai" in title or "buist" in title:
            entities = [
                {"name": "Alphabet", "ticker": "GOOGL", "role": "named", "direction": "down",
                 "axiom_id": "A_AT_02"},
                {"name": "OpenAI", "ticker": None, "role": "named", "direction": "down",
                 "axiom_id": "A_AT_01"},
                {"name": "Meta", "ticker": "META", "role": "unscathed_rival", "direction": "up",
                 "stays_out": True, "axiom_id": "A_AT_03"},
            ]
            answers = [
                {"id": "q1", "status": "answered", "note": "Google and the named labs"},
                {"id": "q2", "status": "blocked", "note": "no blocked channel substitute"},
                {"id": "q3", "status": "answered", "note": "META is outside the harm set"},
                {"id": "q4", "status": "blocked", "note": "compute cap unanswered"},
            ]
        elif "tokenized" in title:
            entities = [
                {"name": "Coinbase", "ticker": "COIN", "role": "named", "direction": "up",
                 "axiom_id": "A_TSV_01"},
                {"name": "Nasdaq", "ticker": "NDAQ", "role": "named", "direction": "mixed"},
            ]
            answers = [{"id": f"q{i}", "status": "answered", "note": "venues"} for i in range(1, 5)]
        elif "lanreotide" in title or "amneal" in title:
            entities = [
                {"name": "Amneal", "ticker": "AMRX", "role": "named", "direction": "up"},
            ]
            answers = [
                {"id": "q1", "status": "answered", "note": "Amneal"},
                {"id": "q2", "status": "answered", "note": "this is an approval gate"},
                {"id": "q3", "status": "answered", "note": "16:01 lands Monday"},
                {"id": "q_monday", "status": "answered", "note": "monday_open"},
            ]
        elif "naion" in title:
            entities = [
                {"name": "Novo Nordisk", "ticker": "NVO", "role": "named", "direction": "down"},
                {"name": "Eli Lilly", "ticker": "LLY", "role": "named", "direction": "down"},
            ]
            answers = [
                {"id": "q1", "status": "answered", "note": "class sponsors"},
                {"id": "q_clock", "status": "answered", "note": "not 0-1d"},
            ]
        else:
            entities, answers = [], []
        return {"entities": entities, "answers": answers}


def test_gold_stack_is_lane_not_classify_brain(monkeypatch, tmp_path: Path):
    def boom(*_a, **_k):
        raise AssertionError("deterministic brain called")

    monkeypatch.setattr("src.news_impact.families.analyze", boom)
    monkeypatch.setattr("src.news_impact.classify.classify_article", boom)
    client = ScriptLane()
    axioms = load_axioms()
    rows = []
    for art in GOLD_KEEP:
        row = process_article(
            art, client, axioms=axioms, use_pack=False, root=Path("."),
            index_names=__import__(
                "src.news_impact.finviz_linker", fromlist=["get_index"]
            ).get_index().title_names,
        )
        rows.append(row)
        assert row["keep"], (art["gold_id"], row["reject_reason"], row.get("validator_errors"))
        assert row["watermark"].startswith("lane::")
        assert all(w["watermark"].startswith("lane::") for w in row["watermarks"])
        assert row["questions"]
        assert any(q["status"] in {"answered", "blocked"} for q in row["questions"])
        assert row["instruments"]
        assert row["action"]
        assert "no action" not in row["action"].lower()
        assert row["invented_tickers"] == []
    by = {r["gold_id"]: r for r in rows}
    assert by["tsa"]["gold_status"] == "PASS"
    assert by["buist"]["gold_status"] == "PASS"
    assert by["tsv"]["gold_status"] == "PASS"
    assert by["amrx"]["gold_status"] == "PASS"
    assert by["amrx"]["clock"] == "monday_open"
    assert by["naion"]["gold_status"] == "PASS"
    assert by["naion"]["clock"] == "not_0_1d"
    assert "medium" in by["naion"]["action"]
    assert "not_0_1d" in by["naion"]["action"]
    # Classify hop never saw a candidate ticker dump.
    classify_prompts = [p for s, p in client.prompts if s == "classify"]
    assert classify_prompts
    assert all("CANDIDATES" not in p for p in classify_prompts)
    # Analyst saw one family test, not the whole catalogue.
    analyst = [p for s, p in client.prompts if s == "analyst"]
    tsa_prompt = next(p for p in analyst if "TSA" in p)
    assert "WINNER/LOSER TEST" in tsa_prompt
    assert "capacity add:" not in tsa_prompt
    for art in GOLD_REJECT:
        row = process_article(art, client, axioms=axioms, use_pack=False)
        assert row["keep"] is False
        assert row["reject_reason"] in {"hormuz_weather", "outperforms_competitors"}


def test_lane_miss_does_not_publish(monkeypatch):
    def dead(_stage, _prompt, _system):
        return None, "", ""

    art = GOLD_KEEP[0]
    row = process_article(art, dead, axioms=load_axioms(), use_pack=False)
    assert row["keep"] is False
    assert row["action"] == ""
    assert row["watermark"] == ""


def test_invented_ticker_is_stripped(monkeypatch):
    def liar(stage, prompt, system):
        if stage == "classify":
            return {"event_class": "blast_ops", "q5": "impulse", "sign": None,
                    "constraint": "TSA unpaid"}, "zhipu", "glm-4.7-flash"
        if stage == "meta":
            return {
                "m1": {"need_context": "no"},
                "m2": [{
                    "slot": "H", "noun": "TSA",
                    "question": "Which harm does unpaid TSA impose on airports?",
                    "changes": "direction", "blocks": ["direction"],
                }],
                "m3_dropped": [],
            }, "zhipu", "glm-4.7-flash"
        if stage == "pack_complete":
            slots = [
                {"question": line.split("question=", 1)[1].strip(), "status": "answered"}
                for line in prompt.splitlines() if "question=" in line
            ]
            return {
                "invert": "Paid TSA removes the airport block.",
                "slots": slots,
            }, "zhipu", "glm-4.7-flash"
        if stage in {"linker", "filter"}:
            return {"instruments": [{"ticker": "CAR", "keep": True},
                                    {"ticker": "VKTX", "keep": True}]}, "siliconflow", "Qwen/Qwen3-8B"
        return {"entities": [
            {"name": "Viking", "ticker": "VKTX", "role": "named", "direction": "up"},
            {"name": "Avis", "ticker": "CAR", "role": "substitute", "direction": "up",
             "axiom_id": "A_AIR_02"},
            {"name": "American", "ticker": "AAL", "role": "named", "direction": "down",
             "axiom_id": "A_AIR_01"},
        ], "answers": [{"id": "q1", "status": "answered", "note": "airlines"}]}, "zhipu", "glm-4.7-flash"

    art = GOLD_KEEP[0]
    row = process_article(art, liar, axioms=load_axioms(), use_pack=False)
    ticks = {e.get("ticker") for e in row["entities"]}
    assert "VKTX" not in ticks
    assert "CAR" in ticks


def test_markdown_shows_questions_hits_and_action():
    text = render_markdown(
        {
            "n_drawn": 7, "n_rejected": 2, "n_kept": 1, "invented_tickers": 0,
            "hop_histogram": {"lane::zhipu::glm-4.7-flash": 1},
            "gold": {"hormuz": "REJECTED"},
            "env": ["present ZHIPU_API_KEY len=4", "missing OPENROUTER_API_KEY"],
            "status": "SHORTFALL",
        },
        [{
            "title": "TSA unpaid",
            "known_at": "2026-09-18",
            "harvest_source": "gold_fixture",
            "q5": "impulse",
            "event_class": "blast_ops",
            "sign": None,
            "questions": [{"question": "Who is harmed?", "status": "answered", "note": "airlines"}],
            "instruments": [{"ticker": "CAR", "entity_name": "Avis Budget Group Inc",
                             "industry": "Rental & Leasing Services"}],
            "winners": ["CAR"],
            "losers": ["AAL"],
            "action": "BUY CAR, 0-1d, because TSA unpaid; clock=0-1d",
            "watermark": "lane::zhipu::glm-4.7-flash",
        }],
    )
    assert "n_drawn: 7" in text
    assert "Who is harmed?" in text
    assert "Avis Budget" in text
    assert "ACTION: BUY CAR" in text
    assert "lane::zhipu::glm-4.7-flash" in text
    assert "no action warranted" not in text.lower()


def test_env_check_redacts(monkeypatch, capsys):
    monkeypatch.setenv("ZHIPU_API_KEY", "super-secret-value")
    monkeypatch.setenv("OPENCLAW_TOKEN", "secret-token-value")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL", raising=False)
    from src.lane_one_shot import print_env
    print_env()
    out = capsys.readouterr().out
    assert "present ZHIPU_API_KEY" in out
    assert "super-secret-value" not in out
    assert "missing OPENROUTER_API_KEY" in out
    assert "present OPENCLAW_TOKEN" in out
    assert "secret-token-value" not in out
    assert "missing OPENCLAW_GATEWAY_URL" in out


def test_classify_floor_excludes_ministral_and_8b(monkeypatch):
    monkeypatch.delenv("LANE_ALLOW_PAID_DEEPSEEK", raising=False)
    from src.lane_one_shot import classify_lanes, classify_models_for
    from src.lane_route import is_classify_banned, lanes_for, primary_models_for
    order = classify_lanes()
    assert order == [
        "openclaw",
        "zhipu", "siliconflow", "openrouter", "gemini", "tokenhub",
        "mistral", "pollinations", "qwen",
    ]
    assert order[0] == "openclaw"
    assert order.index("mistral") < order.index("qwen")
    assert order.index("pollinations") < order.index("qwen")
    assert order.index("gemini") < order.index("qwen")
    assert order.index("tokenhub") < order.index("qwen")
    assert lanes_for("news_classify") == order
    assert "nvidia_nim" not in order
    assert classify_models_for("zhipu")[0] == "glm-4.7-flash"
    zhipu = classify_models_for("zhipu")
    assert zhipu[0] == "glm-4.7-flash"
    assert zhipu == ["glm-4.7-flash", "glm-4.6-flash", "glm-4.6v-flash"]
    assert "glm-4.7-flashx" not in classify_models_for("zhipu")
    assert "glm-4.5-flash" not in classify_models_for("zhipu")
    assert "glm-4-flash-250414" not in classify_models_for("zhipu")
    assert classify_models_for("siliconflow") == []
    qwen = classify_models_for("qwen")
    assert qwen[:5] == [
        "qwen-flash", "qwen3.8-flash", "qwen3.7-flash",
        "qwen3.6-flash", "qwen3.5-flash",
    ]
    assert qwen.index("qwen3.5-flash") < qwen.index("qwen3.7-flash-2026-07-15")
    assert qwen.index("qwen3.7-flash-2026-07-15") < qwen.index("qwen3-32b")
    assert "qwen3.6-flash-2026-04-16" in qwen
    assert "qwen3.5-flash-2026-02-23" in qwen
    assert qwen.index("qwen3-32b") < qwen.index("qwen3-14b")
    assert classify_models_for("gemini") == [
        "gemini-2.5-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.5-flash",
        "gemini-3.6-flash",
        "gemini-3.7-flash",
        "gemini-3.8-flash",
    ]
    assert "gemini-2.5-flash-lite" not in classify_models_for("gemini")
    assert classify_models_for("tokenhub")[0] == "glm-5.3-flash"
    assert "deepseek-v4-flash" in classify_models_for("tokenhub")
    assert "hy3" not in classify_models_for("tokenhub")
    assert "google/gemma-4-31b-it:free" in classify_models_for("openrouter")
    assert "inclusionai/ling-3.0-flash-fin:free" not in classify_models_for("openrouter")
    assert is_classify_banned("ministral-8b-2512")
    assert is_classify_banned("ministral-3b-2512")
    assert is_classify_banned("Qwen/Qwen3-8B")
    assert is_classify_banned("meta/llama-3.1-8b-instruct")
    assert is_classify_banned("nvidia/nemotron-mini-4b-instruct")
    assert is_classify_banned("gemini-fast")
    assert not is_classify_banned("glm-4.7-flash")
    assert not is_classify_banned("qwen-flash")
    assert not is_classify_banned("mistral-small-latest")
    assert not is_classify_banned("qwen3.7-flash")
    assert classify_models_for("mistral") == ["mistral-small-latest"]
    assert "ministral-8b-2512" not in classify_models_for("mistral")
    assert classify_models_for("nvidia_nim") == []
    assert classify_models_for("pollinations") == ["qwen3.7-flash", "deepseek"]
    assert "gemini-fast" not in classify_models_for("pollinations")
    assert primary_models_for("qwen", "news_to_tickers") == ["qwen-flash"]
    assert primary_models_for("mistral", "news_to_tickers")[0] == "ministral-8b-2512"
    assert primary_models_for("zhipu", "news_to_tickers") == ["glm-4.7-flash"]
    from src.lane_one_shot import _FLOOR_PROVIDERS
    assert "gemini" in _FLOOR_PROVIDERS
    assert "mistral" in _FLOOR_PROVIDERS
    assert "pollinations" in _FLOOR_PROVIDERS
    assert "nvidia_nim" not in _FLOOR_PROVIDERS
    assert "openclaw" in _FLOOR_PROVIDERS
    monkeypatch.delenv("OPENCLAW_BACKEND_MODEL", raising=False)
    assert classify_models_for("openclaw") == ["xai/grok-4.6"]
    assert not is_classify_banned("xai/grok-4.6")
    assert not is_classify_banned("xai/grok-4-fast-reasoning")
    assert is_classify_banned("ministral-8b-2512")
    monkeypatch.setenv("OPENCLAW_BACKEND_MODEL", "ministral-8b-2512")
    from src.lane_route import (
        OPENCLAW_FILTER_MODEL, OPENCLAW_FLOOR_MODEL, openclaw_allowlisted_model,
        openclaw_backend_model, openclaw_models,
    )
    assert openclaw_backend_model() == "xai/grok-4.6"
    assert openclaw_models() == ["xai/grok-4.6"]
    monkeypatch.setenv("OPENCLAW_BACKEND_MODEL", "xai/grok-4-fast-reasoning")
    assert openclaw_allowlisted_model("xai/grok-4-fast-reasoning") == "xai/grok-4.6"
    assert openclaw_allowlisted_model("xai/grok-4-fast") == "xai/grok-4.6"
    assert openclaw_allowlisted_model("grok-4-fast-reasoning") == "xai/grok-4.6"
    assert openclaw_allowlisted_model("xai/grok-4.6") == "xai/grok-4.6"
    assert classify_models_for("openclaw") == ["xai/grok-4.6"]
    assert primary_models_for("openclaw", "news_impact") == ["xai/grok-4.6"]
    assert primary_models_for("openclaw", "news_classify") == ["xai/grok-4.6"]
    assert primary_models_for("openclaw", "news_filter") == [OPENCLAW_FILTER_MODEL]
    assert OPENCLAW_FILTER_MODEL == "xai/grok-4.6"
    cfg = Path("src/config.py").read_text(encoding="utf-8")
    match = re.search(
        r'os\.environ\.get\("OPENCLAW_BACKEND_MODEL",\s*"([^"]+)"\)',
        cfg,
    )
    assert match is not None
    assert match.group(1) == "xai/grok-4.6"
    assert OPENCLAW_FLOOR_MODEL == "xai/grok-4.6"
    from src.lane_one_shot import _WM, _classify_floor_ok, _context_floor_ok
    wm = "lane::openclaw::xai/grok-4.6"
    assert _WM.match(wm)
    floor_row = {
        "watermarks": [
            {"stage": "classify", "watermark": wm},
            {"stage": "meta", "watermark": wm},
            {"stage": "pack_complete", "watermark": wm},
        ],
    }
    assert _classify_floor_ok(floor_row)
    assert _context_floor_ok(floor_row)


def test_openclaw_ask_skips_without_gateway():
    from src import lane_route
    lane_route._SKIP.clear()
    parsed, info = lane_route.ask_lane(
        "openclaw", "hi",
        {"keys": {}, "ollama_url": "", "gh_direct": ""},
        tmpl="news_classify",
    )
    assert parsed is None
    assert info is None


def test_openclaw_chat_watermark_is_backend_model(monkeypatch):
    from src import lane_route
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    monkeypatch.setenv("OPENCLAW_TOKEN", "tok")
    monkeypatch.setattr(
        "src.config.align_openclaw_token", lambda **_k: "tok",
    )
    seen = {}

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320,
                  system=None, timeout=None):
        seen["url"] = url
        seen["key"] = key
        seen["model"] = model
        seen["extra"] = extra
        seen["timeout"] = timeout
        return {"event_class": "gate"}, 200, model

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    parsed, status, info = lane_route.openclaw_chat(
        "xai/grok-4-fast-reasoning", "hi", max_tokens=900,
    )
    assert parsed == {"event_class": "gate"}
    assert status == 200
    assert info == "xai/grok-4.6"
    assert seen["model"] == "openclaw/default"
    assert seen["extra"]["x-openclaw-model"] == "xai/grok-4.6"
    parsed, status, info = lane_route.openclaw_chat(
        "xai/grok-4-fast", "hi", max_tokens=900,
    )
    assert info == "xai/grok-4.6"
    assert seen["extra"]["x-openclaw-model"] == "xai/grok-4.6"
    assert seen["key"] == "tok"
    assert "18789" in seen["url"]
    assert seen["url"].endswith("/v1/chat/completions")
    assert seen["timeout"] == 300


def test_openclaw_classify_hop_is_first(monkeypatch):
    from src import lane_route
    monkeypatch.delenv("OPENCLAW_BACKEND_MODEL", raising=False)
    seen = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        seen.append(model)
        return {"event_class": "gate"}, 200, model

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    parsed, info = lane_route.ask_lane(
        "openclaw", "{}",
        {"keys": {"openclaw": "gateway"}, "ollama_url": "", "gh_direct": ""},
        tmpl="news_classify",
    )
    assert seen == ["xai/grok-4.6"]
    assert parsed == {"event_class": "gate"}
    assert info == "xai/grok-4.6"


def test_openclaw_fast_reasoning_is_rewritten_before_send(monkeypatch):
    from src import lane_route
    monkeypatch.setenv("OPENCLAW_BACKEND_MODEL", "xai/grok-4-fast-reasoning")
    seen = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        seen.append(model)
        return {"event_class": "gate"}, 200, model

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    parsed, info = lane_route.ask_lane(
        "openclaw", "{}",
        {"keys": {"openclaw": "gateway"}, "ollama_url": "", "gh_direct": ""},
        tmpl="news_classify",
    )
    assert seen == ["xai/grok-4.6"]
    assert "grok-4-fast" not in seen[0]
    assert parsed == {"event_class": "gate"}
    assert info == "xai/grok-4.6"
    filter_models = lane_route.primary_models_for("openclaw", "news_filter")
    assert filter_models == ["xai/grok-4.6"]


def test_openclaw_fast_alias_400_keeps_provider_and_does_not_send_it(monkeypatch):
    """Gateway 400 names grok-4-fast. We never put that id on the wire."""
    from src import lane_route
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    monkeypatch.setenv("OPENCLAW_TOKEN", "tok")
    monkeypatch.setenv("OPENCLAW_BACKEND_MODEL", "xai/grok-4-fast-reasoning")
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    seen = []

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320,
                  system=None, timeout=None):
        header = (extra or {}).get("x-openclaw-model")
        seen.append(header)
        body = (
            "{'message': \"Model 'xai/grok-4-fast' is not allowed for "
            "agent 'main'.\", 'type': 'invalid_request_error'}"
        )
        return None, 400, body

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    parsed, info = lane_route.ask_lane(
        "openclaw", "{}",
        {"keys": {"openclaw": "gateway"}, "ollama_url": "", "gh_direct": ""},
        tmpl="news_classify",
    )
    assert parsed is None
    assert seen == ["xai/grok-4.6"]
    assert "grok-4-fast" not in seen[0]
    assert "openclaw" not in lane_route._SKIP
    assert "openclaw::xai/grok-4-fast" not in lane_route._MODEL_DENIED
    assert "openclaw::xai/grok-4-fast-reasoning" not in lane_route._MODEL_DENIED
    assert "openclaw::xai/grok-4.6" in lane_route._MODEL_DENIED
    assert info is None or "grok-4-fast-reasoning" not in str(info)
    lane_route._MODEL_DENIED.clear()


def test_openclaw_lane_timeout_env(monkeypatch):
    from src import lane_route
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
    monkeypatch.setenv("OPENCLAW_LANE_TIMEOUT", "450")
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    seen = {}

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320,
                  system=None, timeout=None):
        seen["timeout"] = timeout
        return {"event_class": "blast_ops"}, 200, model

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    lane_route.openclaw_chat("xai/grok-4.6", "hi")
    assert seen["timeout"] == 450


def test_extract_json_keeps_final_class_not_the_draft():
    from src.lane_route import extract_json
    from src.news_impact.one_shot_stack import classify_acceptable
    text = (
        'draft {"event_class":"labor_stop","q5":"impulse"}\n'
        '```json\n'
        '{"event_class":"blast_ops","q5":"impulse","constraint":"TSA unpaid"}\n'
        '```'
    )
    parsed = extract_json(text)
    assert parsed["event_class"] == "blast_ops"
    assert classify_acceptable(parsed, "TSA unpaid", "", gold_id="tsa")
    draft = {"event_class": "labor_stop", "q5": "impulse"}
    assert not classify_acceptable(draft, "TSA unpaid", "", gold_id="tsa")


def test_winners_and_losers_sign_entities():
    from src.news_impact.one_shot_stack import _normalize_entities
    instruments = [
        {"ticker": "NVO", "entity_name": "Novo Nordisk"},
        {"ticker": "LLY", "entity_name": "Eli Lilly"},
    ]
    entities, errors = _normalize_entities(
        {
            "entities": [
                {"name": "Novo", "ticker": "NVO", "direction": "not_determined"},
            ],
            "losers": [{"name": "Novo Nordisk", "ticker": "NVO"}, "LLY"],
        },
        instruments, set(), [], "medium",
    )
    assert errors == []
    signed = {e["ticker"]: e["direction"] for e in entities}
    assert signed["NVO"] == "down"
    assert signed["LLY"] == "down"


def test_finished_pack_keeps_signed_direction():
    from src.news_impact.meta_hop import direction_blocked
    assert direction_blocked({
        "m2": [
            {"blocks": ["direction"], "status": "answered"},
            {"blocks": ["direction"], "status": "blocked"},
        ],
        "m4": {"pack_complete": True},
    }) is False
    assert direction_blocked({
        "m2": [{"blocks": ["direction"], "status": "blocked"}],
        "m4": {"pack_complete": False},
    }) is True


def test_filter_and_analyst_ask_openclaw_first(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import ANALYST_LANES, FILTER_LANES, LiveLane
    assert FILTER_LANES[0] == "openclaw"
    assert ANALYST_LANES[0] == "openclaw"
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway"}, "", ""),
    )
    calls = []

    def fake_ask(lane, prompt, ctx, max_tokens=320, system=None, tmpl="custom", accept=None):
        calls.append((lane, tmpl))
        if lane == "openclaw":
            return {"core": ["CAR"]}, "xai/grok-4.6"
        return None, None

    monkeypatch.setattr(lane_route, "ask_lane", fake_ask)
    live = LiveLane()
    parsed, hop, model = live("filter", "prompt", "system", accept=lambda blob: True)
    assert calls[0] == ("openclaw", "news_filter")
    assert hop == "openclaw"
    assert model == "xai/grok-4.6"
    assert parsed["core"] == ["CAR"]
    calls.clear()

    def fake_analyst(lane, prompt, ctx, max_tokens=320, system=None, tmpl="custom", accept=None):
        calls.append(lane)
        if lane == "openclaw":
            return {"entities": [{"ticker": "CAR", "direction": "up"}]}, "xai/grok-4.6"
        return None, None

    monkeypatch.setattr(lane_route, "ask_lane", fake_analyst)
    parsed, hop, model = live("analyst", "prompt", "system", accept=lambda blob: True)
    assert calls[0] == "openclaw"
    assert hop == "openclaw"
    assert model == "xai/grok-4.6"


def test_dashscope_401_on_one_host_tries_the_next(monkeypatch):
    from src import lane_route

    calls = []

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320, system=None):
        calls.append(url)
        if calls and "custom.example" in url:
            return None, 401, "bad host"
        return {"ok": True}, 200, model

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    monkeypatch.setenv("DASHSCOPE_BASE_URL", "https://custom.example/compatible-mode/v1")
    lane_route._SKIP.clear()
    parsed, status, _info = lane_route.dashscope_chat("k", "qwen-flash", "hi")
    assert parsed == {"ok": True}
    assert status == 200
    assert len(calls) >= 2
    assert "qwen" not in lane_route._SKIP


def test_account_standing_keeps_siblings():
    """Standing 400 drops that model ID and still calls the next one."""
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._QWEN_STANDING_HITS = 0
    seen = []

    def call(model):
        seen.append(model)
        if model == "qwen-flash":
            return None, 400, "Access denied, please make sure your account is in good standing"
        return {"event_class": "blast_ops", "q5": "impulse"}, 200, model

    parsed, info = lane_route.hop_models(
        "qwen", ["qwen-flash", "qwen3.8-flash"], call,
    )
    assert parsed["event_class"] == "blast_ops"
    assert info == "qwen3.8-flash"
    assert seen == ["qwen-flash", "qwen3.8-flash"]
    assert "qwen" not in lane_route._SKIP
    assert "qwen::qwen-flash" in lane_route._MODEL_DENIED
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._QWEN_STANDING_HITS = 0


def test_two_standing_400s_stop_the_qwen_list():
    """Two arrearage bodies stop the rest of the list. The key stays live."""
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._QWEN_STANDING_HITS = 0
    seen = []

    def call(model):
        seen.append(model)
        return None, 400, "Access denied, please make sure your account is in good standing"

    parsed, _info = lane_route.hop_models(
        "qwen",
        ["qwen-flash", "qwen3.8-flash", "qwen3.7-flash", "qwen3.6-flash"],
        call,
    )
    assert parsed is None
    assert seen == ["qwen-flash", "qwen3.8-flash"]
    assert "qwen" not in lane_route._SKIP
    seen.clear()
    parsed, _info = lane_route.hop_models("qwen", ["qwen3.5-flash", "qwen3-32b"], call)
    assert parsed is None
    assert seen == []
    assert "qwen" not in lane_route._SKIP
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._QWEN_STANDING_HITS = 0


def test_incorrect_api_key_still_marks_the_key_dead():
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._QWEN_STANDING_HITS = 0
    seen = []

    def call(model):
        seen.append(model)
        return None, 401, "Incorrect API key provided"

    parsed, _info = lane_route.hop_models(
        "qwen", ["qwen-flash", "qwen3.8-flash"], call,
    )
    assert parsed is None
    assert seen == ["qwen-flash"]
    assert "qwen" in lane_route._SKIP
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()


def test_transient_429_clears_between_articles():
    from src import lane_route

    lane_route._RATE_LIMITED.add("mistral::mistral-small-latest")
    lane_route._MODEL_DENIED.add("qwen::qwen-flash")
    lane_route._SKIP.add("pollinations")
    lane_route._OR_DAY_CAPPED = True
    lane_route.release_transient_limits()
    assert "mistral::mistral-small-latest" not in lane_route._RATE_LIMITED
    assert "qwen::qwen-flash" in lane_route._MODEL_DENIED
    assert "pollinations" in lane_route._SKIP
    assert lane_route._OR_DAY_CAPPED is True
    lane_route._MODEL_DENIED.clear()
    lane_route._SKIP.clear()
    lane_route._OR_DAY_CAPPED = False


def test_news_impact_tries_mistral_small_before_8b():
    from src.lane_route import primary_models_for
    order = primary_models_for("mistral", "news_impact")
    assert order[0] == "mistral-small-latest"
    assert "ministral-8b-2512" in order
    assert order.index("mistral-small-latest") < order.index("ministral-8b-2512")
    assert primary_models_for("mistral", "news_classify") == ["mistral-small-latest"]


def test_429_then_403_does_not_skip_provider():
    """429 keeps the provider. A later per-model 403 does not cache _SKIP."""
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._RATE_LIMITED.clear()
    seen = []

    def call(model):
        seen.append(model)
        if model == "glm-4.7-flash":
            return None, 429, "busy"
        return None, 403, "无权访问"

    from unittest import mock

    with mock.patch.object(lane_route.time, "sleep", lambda *_a, **_k: None):
        parsed, info = lane_route.hop_models(
            "zhipu", ["glm-4.7-flash", "glm-4.6-flash"], call,
        )
    assert parsed is None and info is None
    assert seen == ["glm-4.7-flash", "glm-4.6-flash"]
    assert "zhipu" not in lane_route._SKIP
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._RATE_LIMITED.clear()


def test_200_then_402_does_not_skip_provider():
    """A 200 on the lane means a later sibling 402 must not cache _SKIP."""
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    lane_route._RATE_LIMITED.clear()
    seen = []

    def call(model):
        seen.append(model)
        if model == "glm-5.3-flash":
            return {"event_class": "regime_break", "q5": "regime_break"}, 200, model
        if model == "glm-5.3-flashx":
            return None, 402, "free quota exhausted"
        return {"event_class": "blast_ops", "q5": "impulse"}, 200, model

    parsed, info = lane_route.hop_models(
        "tokenhub",
        ["glm-5.3-flash", "glm-5.3-flashx", "deepseek-v4-flash"],
        call,
        accept=lambda row: row.get("event_class") == "blast_ops",
    )
    assert parsed["event_class"] == "blast_ops"
    assert info == "deepseek-v4-flash"
    assert seen == ["glm-5.3-flash", "glm-5.3-flashx", "deepseek-v4-flash"]
    assert "tokenhub" not in lane_route._SKIP
    assert "hy3" not in seen
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()


def test_rejected_enum_tries_the_next_model():
    from src import lane_route

    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()
    seen = []

    def call(model):
        seen.append(model)
        if model == "glm-5.3-flash":
            return {"event_class": "regime_break", "q5": "regime_break"}, 200, model
        return {"event_class": "blast_ops", "q5": "impulse"}, 200, model

    parsed, info = lane_route.hop_models(
        "tokenhub",
        ["glm-5.3-flash", "deepseek-v4-flash"],
        call,
        accept=lambda row: row.get("event_class") == "blast_ops",
    )
    assert parsed["event_class"] == "blast_ops"
    assert info == "deepseek-v4-flash"
    assert seen == ["glm-5.3-flash", "deepseek-v4-flash"]
    lane_route._SKIP.clear()
    lane_route._MODEL_DENIED.clear()


def test_dashscope_trailing_401_does_not_hide_a_soft_status(monkeypatch):
    from src import lane_route

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320, system=None):
        if "custom.example" in url:
            return None, 400, "bad request"
        return None, 401, "Incorrect API key provided"

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    monkeypatch.setenv("DASHSCOPE_BASE_URL", "https://custom.example/compatible-mode/v1")
    _parsed, status, info = lane_route.dashscope_chat("k", "qwen-flash", "hi")
    assert status == 400
    assert "all dashscope hosts" not in str(info)


def test_zhipu_429_on_one_host_tries_the_other(monkeypatch):
    from src import lane_route

    calls = []

    def fake_chat(url, key, model, prompt, extra=None, max_tokens=320, system=None):
        calls.append(url)
        if "bigmodel" in url:
            return None, 429, "busy"
        return {"ok": True}, 200, model

    monkeypatch.setattr(lane_route, "openai_chat", fake_chat)
    parsed, status, _info = lane_route.zhipu_chat("k", "glm-4.7-flash", "hi")
    assert parsed == {"ok": True}
    assert status == 200
    assert len(calls) == 2


def test_gemini_404_names_a_flash_id_not_pro():
    from src.lane_route import gemini_flash_suggestions
    text = (
        "This model models/gemini-2.5-flash-lite is no longer available. "
        "Please update your code to use models/gemini-3.1-flash-lite "
        "or models/gemini-2.5-pro"
    )
    assert "gemini-2.5-flash-lite" not in gemini_flash_suggestions(text)
    assert gemini_flash_suggestions(text) == ["gemini-3.1-flash-lite"]


def test_think_tags_do_not_hide_json():
    from src.lane_route import _choice_text
    body = {"choices": [{"message": {"content": "<think>secret</think>{\"event_class\":\"gate\"}"}}]}
    assert _choice_text(body).startswith("{")


def test_regime_break_on_tsa_is_not_an_accepted_class():
    title = GOLD_KEEP[0]["title"]
    assert classify_acceptable(
        {"event_class": "regime_break", "q5": "regime_break"}, title, "", "tsa",
    ) is False
    assert classify_acceptable(
        {"event_class": "blast_ops", "q5": "impulse"}, title, "", "tsa",
    ) is True
    assert classify_acceptable(
        {"event_class": "regime_break", "q5": "impulse"},
        "Ordinary tape reprint", "", "",
    ) is False


def test_amrx_gate_regime_break_is_accepted_and_keeps_the_clock():
    from src.news_impact.one_shot_stack import _clock, _horizon_for
    art = next(row for row in GOLD_KEEP if row["gold_id"] == "amrx")
    shape = {"event_class": "gate", "q5": "regime_break"}
    assert classify_acceptable(shape, art["title"], art["body"], "amrx") is True
    assert classify_acceptable(
        {"event_class": "gate", "q5": "impulse"}, art["title"], art["body"], "amrx",
    ) is True
    assert classify_acceptable(
        {"event_class": "regime_break", "q5": "regime_break"},
        art["title"], art["body"], "amrx",
    ) is False
    assert classify_acceptable(
        {"event_class": "labor_stop", "q5": "regime_break"},
        art["title"], art["body"], "amrx",
    ) is False
    assert classify_acceptable(shape, art["title"], art["body"], "") is False
    assert _horizon_for("gate", art["title"], "open") == "1-6m"
    assert _clock(art["title"], art["known_at"], "1-6m", "regime_break") == "monday_open"


def test_tsa_labor_stop_stays_rejected_and_repair_names_blast_ops():
    from src.news_impact.one_shot_stack import classify_repair_note
    art = GOLD_KEEP[0]
    rejected = {"event_class": "labor_stop", "q5": "regime_break"}
    assert classify_acceptable(rejected, art["title"], art["body"], "tsa") is False
    note = classify_repair_note(rejected, art["title"], art["body"])
    assert "PREVIOUS JSON WAS NOT ACCEPTED" in note
    assert "blast_ops" in note
    assert "impulse" in note
    assert "strike" in note.lower()
    assert classify_repair_note(
        {"event_class": "blast_ops", "q5": "regime_break"}, art["title"], art["body"],
    )
    assert classify_repair_note(
        {"event_class": "blast_ops", "q5": "impulse"}, art["title"], art["body"],
    ) == ""
    assert classify_repair_note(
        {"event_class": "labor_stop", "q5": "impulse"},
        "Dock workers walkout at the port", "",
    ) == ""
    assert classify_repair_note(
        {"event_class": "gate", "q5": "regime_break"}, art["title"], art["body"],
    ) == ""


def test_openclaw_repairs_labor_stop_before_the_hopper(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import LiveLane
    from src.news_impact.prompts import classifier_prompt

    monkeypatch.delenv("OPENCLAW_BACKEND_MODEL", raising=False)
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway", "zhipu": "k"}, "", ""),
    )
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    prompts = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        prompts.append(prompt)
        if "PREVIOUS JSON WAS NOT ACCEPTED" in prompt:
            return {
                "event_class": "blast_ops", "q5": "impulse", "constraint": "ops",
            }, 200, model
        return {
            "event_class": "labor_stop", "q5": "regime_break", "constraint": "unpaid",
        }, 200, model

    def hopper_should_not_run(*_a, **_k):
        raise AssertionError("classify hopper ran before the OpenClaw repair")

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    monkeypatch.setattr(lane_route, "zhipu_chat", hopper_should_not_run)
    live = LiveLane()
    art = GOLD_KEEP[0]
    prompt = classifier_prompt(art["title"], art["body"], art["known_at"])
    parsed, hop, model = live(
        "classify", prompt, "sys",
        accept=lambda blob: classify_acceptable(blob, art["title"], art["body"], "tsa"),
    )
    assert parsed["event_class"] == "blast_ops"
    assert parsed["q5"] == "impulse"
    assert (hop, model) == ("openclaw", "xai/grok-4.6")
    assert len(prompts) == 2
    assert "PREVIOUS JSON WAS NOT ACCEPTED" not in prompts[0]
    assert "PREVIOUS JSON WAS NOT ACCEPTED" in prompts[1]
    assert "blast_ops" in prompts[1]


def test_openclaw_repair_does_not_lock_labor_stop(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import LiveLane
    from src.news_impact.prompts import classifier_prompt

    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway", "zhipu": "k"}, "", ""),
    )
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    prompts = []
    hopper = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        prompts.append(prompt)
        return {"event_class": "labor_stop", "q5": "regime_break"}, 200, model

    def fake_zhipu(_key, model, prompt, max_tokens=320, system=None):
        hopper.append(model)
        return {"event_class": "blast_ops", "q5": "impulse"}, 200, model

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    monkeypatch.setattr(lane_route, "zhipu_chat", fake_zhipu)
    live = LiveLane()
    art = GOLD_KEEP[0]
    prompt = classifier_prompt(art["title"], art["body"], art["known_at"])
    parsed, hop, model = live(
        "classify", prompt, "sys",
        accept=lambda blob: classify_acceptable(blob, art["title"], art["body"], "tsa"),
    )
    assert len(prompts) == 2
    assert hopper == []
    assert parsed is None
    assert hop == ""
    assert model == ""


def test_buist_regime_repairs_on_openclaw_and_skips_hopper(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import LiveLane
    from src.news_impact.one_shot_stack import classify_repair_note
    from src.news_impact.prompts import classifier_prompt

    art = next(row for row in GOLD_KEEP if row["gold_id"] == "buist")
    rejected = {"event_class": "blast_legal", "q5": "regime"}
    note = classify_repair_note(rejected, art["title"], art["body"])
    assert "blast_legal" in note and "impulse" in note
    assert classify_acceptable(rejected, art["title"], art["body"], "buist") is False

    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway", "zhipu": "k"}, "", ""),
    )
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    prompts = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        prompts.append(prompt)
        if "PREVIOUS JSON WAS NOT ACCEPTED" in prompt:
            return {"event_class": "blast_legal", "q5": "impulse"}, 200, model
        return {"event_class": "blast_legal", "q5": "regime"}, 200, model

    def hopper_should_not_run(*_a, **_k):
        raise AssertionError("classify hopper ran after OpenClaw answered")

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    monkeypatch.setattr(lane_route, "zhipu_chat", hopper_should_not_run)
    live = LiveLane()
    prompt = classifier_prompt(art["title"], art["body"], art["known_at"])
    parsed, hop, model = live(
        "classify", prompt, "sys",
        accept=lambda blob: classify_acceptable(blob, art["title"], art["body"], "buist"),
    )
    assert parsed["q5"] == "impulse"
    assert (hop, model) == ("openclaw", "xai/grok-4.6")
    assert len(prompts) == 2


def test_openclaw_filter_retries_once_before_the_hopper(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import LiveLane

    monkeypatch.delenv("OPENCLAW_BACKEND_MODEL", raising=False)
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway", "mistral": "k"}, "", ""),
    )
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    prompts = []

    def fake_chat(model, prompt, max_tokens=320, system=None):
        prompts.append(prompt)
        if "PREVIOUS JSON WAS NOT ACCEPTED" in prompt:
            return {"core": ["CAR", "AAL"], "tangent": []}, 200, model
        return {"core": ["MSFT"], "tangent": []}, 200, model

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    cands = [
        {"ticker": "CAR", "entity_name": "Avis Budget"},
        {"ticker": "AAL", "entity_name": "American Airlines"},
        {"ticker": "MSFT", "entity_name": "Microsoft"},
    ]
    live = LiveLane()
    prompt = (
        "Title: Government shutdown leads to chaos at US airports as TSA officers go unpaid\n"
        "HITS:\n- CAR\n- AAL\n"
    )
    parsed, hop, model = live(
        "filter", prompt, "sys",
        accept=lambda blob: linker_acceptable(blob, cands, "tsa"),
    )
    assert parsed["core"] == ["CAR", "AAL"]
    assert (hop, model) == ("openclaw", "xai/grok-4.6")
    assert len(prompts) == 2
    assert "CAR" in prompts[1]


def test_openclaw_analyst_retries_once_before_the_hopper(monkeypatch):
    from src import lane_route
    from src.lane_one_shot import LiveLane
    from src.news_impact.one_shot_stack import analyst_acceptable

    monkeypatch.delenv("OPENCLAW_BACKEND_MODEL", raising=False)
    monkeypatch.setattr("src.config.align_openclaw_token", lambda **_k: "tok")
    monkeypatch.setattr(
        lane_route, "load_keys",
        lambda: ({"openclaw": "gateway", "mistral": "k"}, "", ""),
    )
    lane_route._SKIP.clear()
    lane_route._RATE_LIMITED.clear()
    lane_route._MODEL_DENIED.clear()
    prompts = []
    good = {
        "entities": [
            {"name": "Avis", "ticker": "CAR", "role": "named", "direction": "up"},
            {"name": "American", "ticker": "AAL", "role": "named", "direction": "down"},
        ],
        "answers": [{"id": "q1", "status": "answered", "note": "flights blocked"}],
    }

    def fake_chat(model, prompt, max_tokens=320, system=None):
        prompts.append(prompt)
        if "PREVIOUS JSON WAS NOT ACCEPTED" in prompt:
            return good, 200, model
        return {"entities": []}, 200, model

    monkeypatch.setattr(lane_route, "openclaw_chat", fake_chat)
    instruments = [
        {"ticker": "CAR", "entity_name": "Avis Budget"},
        {"ticker": "AAL", "entity_name": "American Airlines"},
    ]
    art = GOLD_KEEP[0]
    live = LiveLane()
    prompt = (
        "Title: Government shutdown leads to chaos at US airports as TSA officers go unpaid\n"
        "INSTRUMENTS:\n- CAR\n- AAL\n"
    )

    def _accept(blob):
        return analyst_acceptable(
            blob,
            gold_id="tsa",
            title=art["title"],
            known_at=art["known_at"],
            instruments=instruments,
            axiom_ids=set(),
            pack_facts=[],
            horizon="0-1d",
            q5="impulse",
            event_class="blast_ops",
            sign=None,
            hint_ticker="",
            index_names=None,
        )

    parsed, hop, model = live("analyst", prompt, "sys", accept=_accept)
    assert parsed["entities"][0]["ticker"] == "CAR"
    assert (hop, model) == ("openclaw", "xai/grok-4.6")
    assert len(prompts) == 2


def test_rejected_openclaw_enum_is_not_a_floor_miss():
    from src.news_impact.one_shot_stack import GOLD_EXTRA, plan_history

    art = next(row for row in GOLD_EXTRA if row["gold_id"] == "maduro_capture")

    def empty_lane(stage, prompt, system, accept=None):
        return None, "", ""

    row = process_article(
        art, empty_lane, axioms=load_axioms(), use_pack=False, root=Path("."),
    )
    assert row["reject_reason"] == "lane_classify_missing"
    assert row["keep"] is False
    assert row["history"]["history_state"] == "first_print"
    assert row["gold_status"] == "PASS"
    assert plan_history(art["title"], art["body"], art["known_at"], "")["history_state"] == "first_print"

    wrap = next(row for row in GOLD_EXTRA if row["gold_id"] == "maduro_wrap")

    def regime_lane(stage, prompt, system, accept=None):
        if stage == "classify":
            return {"event_class": "regime_state", "q5": "regime"}, "openclaw", "xai/grok-4.6"
        return None, "", ""

    wrapped = process_article(
        wrap, regime_lane, axioms=load_axioms(), use_pack=False, root=Path("."),
    )
    assert wrapped["reject_reason"] == "q5_regime"
    assert wrapped["history"]["history_state"] == "reprint"
    assert wrapped["gold_status"] == "PASS"
    assert wrapped["keep"] is False

    wang = next(row for row in GOLD_EXTRA if row["gold_id"] == "wang_fuk")

    def discard_lane(stage, prompt, system, accept=None):
        if stage == "classify":
            return {"event_class": "discard", "q5": "regime"}, "openclaw", "xai/grok-4.6"
        return None, "", ""

    fired = process_article(
        wang, discard_lane, axioms=load_axioms(), use_pack=False, root=Path("."),
    )
    assert fired["reject_reason"] == "lane_discard"
    assert fired["history"]["transmission"] == "none"
    assert fired["gold_status"] == "PASS"
    assert not str(fired.get("action") or "").startswith("BUY")


def test_empty_linker_json_is_not_accepted():
    cands = [
        {"ticker": "CAR", "entity_name": "Avis Budget"},
        {"ticker": "AAL", "entity_name": "American Airlines"},
    ]
    assert confirm_instruments({"instruments": []}, cands) == []
    assert linker_acceptable({"instruments": []}, cands, "tsa") is False
    assert linker_acceptable({"keep": ["CAR"]}, cands, "tsa") is False
    assert linker_acceptable({"tickers": ["CAR", "AAL"]}, cands, "tsa") is True
    assert linker_acceptable(["CAR"], cands, "") is True


def test_harm_set_binds_meta_without_inventing():
    instruments = [
        {
            "ticker": "META",
            "entity_name": "Meta Platforms Inc",
            "aliases": ["meta platforms"],
            "score": 3,
            "type": "equity",
        },
        {
            "ticker": "GOOGL",
            "entity_name": "Alphabet Inc",
            "aliases": ["alphabet"],
            "score": 9,
            "type": "parent",
        },
        {
            "ticker": "GOOG",
            "entity_name": "Alphabet Inc",
            "aliases": ["alphabet"],
            "score": 4,
            "type": "equity",
        },
    ]
    entities, errors = _normalize_entities(
        {"entities": [
            {"name": "Meta", "ticker": None, "role": "unscathed", "direction": "up",
             "stays_out": True, "axiom_id": "A_AT_03"},
            {"name": "Alphabet", "ticker": None, "role": "harm set", "direction": "down"},
            {"name": "OpenAI", "ticker": None, "role": "named", "direction": "down"},
        ]},
        instruments,
        {"A_AT_03"},
        [],
        "0-1d",
    )
    assert errors == []
    by = {e["name"]: e for e in entities}
    assert by["Meta"]["ticker"] == "META"
    assert by["Meta"]["role"] == "unscathed_rival"
    assert by["Alphabet"]["ticker"] == "GOOGL"
    assert by["Alphabet"]["role"] == "harm_set"
    assert by["OpenAI"]["ticker"] is None


def test_amrx_gold_requires_gate_not_just_monday_clock():
    row = {
        "gold_id": "amrx",
        "event_class": "regime_break",
        "clock": "monday_open",
        "action": "BUY AMRX, 1-6m, because approval; clock=monday_open",
        "entities": [{"ticker": "AMRX", "direction": "up", "horizon": "1-6m"}],
        "instruments": [{"ticker": "AMRX", "sector": "Healthcare"}],
    }
    assert gold_status(row) == "FAIL"
    row["event_class"] = "gate"
    assert gold_status(row) == "PASS"


def test_board_check_rejects_shortfall_and_mistral_only():
    from src.lane_one_shot import assess_board
    short = render_markdown(
        {
            "n_drawn": 400, "n_rejected": 353, "n_kept": 47, "invented_tickers": 0,
            "hop_histogram": {"lane::mistral::ministral-8b-2512": 140},
            "gold": {
                "tsa": "FAIL", "buist": "FAIL", "tsv": "FAIL", "amrx": "FAIL",
                "naion": "PASS", "hormuz": "REJECTED", "outperforms": "REJECTED",
            },
            "status": "SHORTFALL",
        },
        [],
    )
    problems = assess_board(short)
    assert any(p.startswith("n_kept=") for p in problems)
    assert any("tsa" in p for p in problems)
    assert any("floor model" in p or "classify histogram" in p for p in problems)


def test_classify_prompt_stays_short():
    from src.news_impact.prompts import classifier_prompt
    art = GOLD_KEEP[0]
    text = classifier_prompt(art["title"], art["body"], art["known_at"])
    assert "event_class enum" in text
    assert "Q5 first" in text
    assert "M1" not in text
    assert "M4" not in text
    assert "M5" not in text
    assert "harm_set" not in text
    assert "bullshit" not in text.lower()
    assert len(text.encode("utf-8")) < 4000


def test_meta_prompt_sends_the_full_pack():
    from src.news_impact.prompts import meta_prompt
    art = GOLD_KEEP[0]
    text = meta_prompt(art["title"], art["body"], "blast", "blast_ops", "TSA unpaid")
    for marker in ("M1", "M2", "M3", "M4", "M5"):
        assert marker in text
    for slot in (
        "C constraint", "T time", "H harm", "E expression", "S substitute",
        "R rival", "A ammo", "D durability", "I invert", "P priced",
        "Y-S salience", "Y-T transmission",
    ):
        assert slot in text, slot
    assert "invert" in text.lower()
    assert "bullshit_filter" in text
    assert "ai_angle" in text
    assert "who_should_i_buy" in text
    assert "already_in_article" in text
    assert "undated_weather" in text
    assert "theme_fishing" in text
    assert "no_direction_change" in text


def test_blast_analyst_prompt_includes_flip_questions_and_tsa_car():
    from src.news_impact.one_shot_stack import analyst_prompt as stack_analyst
    art = GOLD_KEEP[0]
    text = stack_analyst(
        art["title"], art["body"], "blast", "blast_ops", None, "impulse",
        "TSA unpaid", [], [], [], [],
    )
    assert "Who is in the harm set" in text
    assert "TSA → CAR" in text or "TSA → CAR" in text.replace("->", "→")
    assert "CAR" in text
    assert "harm_set" in text
    assert "unscathed_rival" in text
    assert "arms_dealer" in text
    assert "unit_vs_parent" in text
    assert "tradeable_expression" in text
    assert "Y-S" in text and "Y-T" in text
    assert "META stays_out" in text
    assert "capacity add:" not in text
    structure = stack_analyst(
        "SEC venues", "", "structure", "market_structure", None, "impulse",
        "venues", [], [], [], [],
    )
    assert "Never attach Energy" in structure
    assert "STRUCTURE family" in structure
    permission = stack_analyst(
        "Amneal lanreotide", "", "permission", "gate", "open", "regime_break",
        "FDA", [], [], [], [],
    )
    assert "Monday" in permission
    assert "AMRX" in permission
    assert "16:01" in permission


def test_prompt_audit_logs_sha_and_edges():
    import hashlib
    from src.news_impact.one_shot_stack import prompt_audit
    body = "\n".join(f"line {i}" for i in range(50))
    rec = prompt_audit("meta", body)
    assert rec["sha256"] == hashlib.sha256(body.encode("utf-8")).hexdigest()
    assert rec["bytes"] == len(body.encode("utf-8"))
    assert rec["head"] == [f"line {i}" for i in range(40)]
    assert rec["tail"] == [f"line {i}" for i in range(10, 50)]
    assert len(rec["head"]) == 40 and len(rec["tail"]) == 40


def test_gold_row_records_prompt_log():
    art = GOLD_KEEP[0]
    row = process_article(art, ScriptLane(), axioms=load_axioms(), use_pack=False)
    stages = [rec["stage"] for rec in row["prompt_log"]]
    assert "classify" in stages
    assert "meta" in stages
    assert "analyst" in stages
    classify = next(rec for rec in row["prompt_log"] if rec["stage"] == "classify")
    assert len(classify["sha256"]) == 64
    assert "M1" not in "\n".join(classify["head"])
    meta = next(rec for rec in row["prompt_log"] if rec["stage"] == "meta")
    joined = "\n".join(meta["head"] + meta["tail"])
    assert "M1" in joined and "M5" in joined
    assert "Y-S salience" in joined


def test_gold_job_is_ecs_and_pins_grok_4_6():
    text = Path(".github/workflows/lane_one_shot_100.yml").read_text(encoding="utf-8")
    gold = text.split("jobs:", 1)[1].split("\n  shard:", 1)[0]
    assert "runs-on: [self-hosted, ecs]" in gold
    assert "ubuntu-latest" not in gold
    assert "OPENCLAW_BACKEND_MODEL: xai/grok-4.6" in gold
    assert "xai/grok-4-fast-reasoning" not in gold
    assert "xai/grok-4-fast-reasoning" not in text
    assert text.count("OPENCLAW_BACKEND_MODEL: xai/grok-4.6") == 4
    assert "ref: ${{ github.sha }}" in gold
    assert "fetch-depth: 1" in gold
    assert "clean: true" in gold
    assert 'git -c safe.directory="${GITHUB_WORKSPACE}" rev-parse HEAD' in gold
    assert 'test "${head}" = "${{ github.sha }}"' in gold
