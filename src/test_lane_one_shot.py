"""Gold-fixture path for the Lane one-shot. The client is a stand-in for Lane.

Production never uses this client. The test fails if the stack calls
classify_article or families.analyze.
"""
from __future__ import annotations

import json
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
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    from src.lane_one_shot import print_env
    print_env()
    out = capsys.readouterr().out
    assert "present ZHIPU_API_KEY" in out
    assert "super-secret-value" not in out
    assert "missing OPENROUTER_API_KEY" in out


def test_classify_floor_excludes_ministral_and_8b(monkeypatch):
    monkeypatch.delenv("LANE_ALLOW_PAID_DEEPSEEK", raising=False)
    from src.lane_one_shot import classify_lanes, classify_models_for
    from src.lane_route import is_classify_banned, lanes_for, primary_models_for
    order = classify_lanes()
    assert order == ["zhipu", "siliconflow", "openrouter", "qwen", "gemini", "tokenhub"]
    assert lanes_for("news_classify") == order
    assert "mistral" not in order
    assert classify_models_for("zhipu")[0] == "glm-4.7-flash"
    assert "glm-4.6-flash" in classify_models_for("zhipu")
    assert "glm-4.6v-flash" not in classify_models_for("zhipu")
    assert "glm-4.7-flashx" not in classify_models_for("zhipu")
    assert "glm-4.5-flash" not in classify_models_for("zhipu")
    assert "glm-4-flash-250414" not in classify_models_for("zhipu")
    assert classify_models_for("siliconflow") == []
    qwen = classify_models_for("qwen")
    assert qwen[:5] == [
        "qwen-flash", "qwen3.8-flash", "qwen3.7-flash",
        "qwen3.6-flash", "qwen3.5-flash",
    ]
    assert qwen.index("qwen3.5-flash") < qwen.index("qwen3-32b")
    assert qwen.index("qwen3-32b") < qwen.index("qwen3-14b")
    assert classify_models_for("gemini") == ["gemini-2.5-flash"]
    assert "gemini-2.5-flash-lite" not in classify_models_for("gemini")
    assert classify_models_for("tokenhub")[0] == "glm-5.3-flash"
    assert "deepseek-v4-flash" in classify_models_for("tokenhub")
    assert "hy3" not in classify_models_for("tokenhub")
    assert "google/gemma-4-31b-it:free" in classify_models_for("openrouter")
    assert "inclusionai/ling-3.0-flash-fin:free" not in classify_models_for("openrouter")
    assert is_classify_banned("ministral-8b-2512")
    assert is_classify_banned("Qwen/Qwen3-8B")
    assert not is_classify_banned("glm-4.7-flash")
    assert not is_classify_banned("qwen-flash")
    assert primary_models_for("qwen", "news_to_tickers") == ["qwen-flash"]
    assert primary_models_for("mistral", "news_classify") == []
    assert primary_models_for("zhipu", "news_to_tickers") == ["glm-4.7-flash"]
    from src.lane_one_shot import _FLOOR_PROVIDERS
    assert "gemini" in _FLOOR_PROVIDERS


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
