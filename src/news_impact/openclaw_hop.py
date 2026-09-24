"""News-impact hops through OpenClaw / SuperGrok (not the $0 Lane stack)."""
from __future__ import annotations

from src import config
from src.lane_route import extract_json
from src.openclaw_models import pick_cheapest_above_30b

from .prompts import (
    ANALYST_SYSTEM,
    CLASSIFIER_SYSTEM,
    LAST,
    META_SYSTEM,
    PACK_COMPLETE_SYSTEM,
    PROMPT_VERSION,
    analyst_prompt,
    classifier_prompt,
    meta_prompt,
    pack_complete_prompt,
)
from .schema import Classification


def news_model() -> str:
    return (config.OPENCLAW_NEWS_MODEL
            or pick_cheapest_above_30b()
            or "xai/grok-4.3")


def gateway_ready() -> bool:
    config.align_openclaw_token()
    return bool(config.OPENCLAW_GATEWAY_URL)


def _fp(hop_name: str) -> dict:
    meta = LAST.get(hop_name) or {}
    return {
        "prompt_version": meta.get("version") or PROMPT_VERSION,
        "prompt_sha16": meta.get("sha256_16") or "",
        "prompt_bytes": meta.get("bytes") or 0,
        "prompt_lines": meta.get("lines") or 0,
    }


def _complete(system: str, user: str, max_tokens: int, stage: str) -> tuple[str, str]:
    from src import deepseek_client
    model = news_model()
    text = deepseek_client.openclaw_complete(
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        max_tokens=max_tokens,
        temperature=0.0,
        stage_label=stage,
        backend_model=model,
    )
    return text or "", model


def hop(tmpl: str, art: dict, family: str = "", cls: Classification | None = None,
        pack: dict | None = None, questions: list | None = None
        ) -> tuple[dict | None, str, str, list[dict]]:
    """One SuperGrok turn. Returns (parsed, lane, model, hop_log). Fail soft."""
    if not gateway_ready():
        return None, "", "", [{
            "role": tmpl, "lane": "openclaw", "model": "",
            "ok": False, "skip": "no_gateway",
        }]
    title = str(art.get("title") or "")
    body = str(art.get("body") or "")
    known = str(art.get("known_at") or art.get("published_at") or "")
    event_class = cls.event_class if cls else ""
    sign = cls.sign if cls else None
    q5 = cls.q5 if cls else "impulse"
    constraint = cls.constraint if cls else ""
    facts = (pack or {}).get("facts") or []
    if tmpl == "news_classify":
        prompt = classifier_prompt(title, body, known)
        system = CLASSIFIER_SYSTEM
        budget = 400
        fp_name = "classify"
    elif tmpl == "news_meta":
        prompt = meta_prompt(title, body, family, event_class, constraint)
        system = META_SYSTEM
        budget = 700
        fp_name = "meta"
    elif tmpl == "news_pack_complete":
        prompt = pack_complete_prompt(title, questions or [], facts)
        system = PACK_COMPLETE_SYSTEM
        budget = 500
        fp_name = "pack_complete"
    else:
        prompt = analyst_prompt(
            title, body, family, event_class, sign, q5, constraint, facts,
        )
        system = ANALYST_SYSTEM
        budget = 2000
        fp_name = "analyst"
    text, model = _complete(system, prompt, budget, tmpl)
    parsed = extract_json(text) if text else None
    stamp = _fp(fp_name)
    if not isinstance(parsed, dict):
        return None, "openclaw", model, [{
            "role": tmpl, "lane": "openclaw", "model": model,
            "ok": False, "skip": "empty" if not text else "unparseable",
            "excerpt": (text or "")[:160],
            **stamp,
        }]
    return parsed, "openclaw", model, [{
        "role": tmpl, "lane": "openclaw", "model": model,
        "ok": True, "excerpt": (text or "")[:160],
        **stamp,
    }]
