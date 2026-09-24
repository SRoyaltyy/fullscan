"""News-impact hops through OpenClaw / SuperGrok (not the $0 Lane stack)."""
from __future__ import annotations

from src import config
from src.lane_route import extract_json
from src.openclaw_models import pick_cheapest_above_30b

from .prompts import ANALYST_SYSTEM, CLASSIFIER_SYSTEM, analyst_prompt, classifier_prompt
from .schema import Classification


def news_model() -> str:
    return (config.OPENCLAW_NEWS_MODEL
            or pick_cheapest_above_30b()
            or "xai/grok-4.20-0309-non-reasoning")


def gateway_ready() -> bool:
    config.align_openclaw_token()
    return bool(config.OPENCLAW_GATEWAY_URL)


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
        pack: dict | None = None) -> tuple[dict | None, str, str, list[dict]]:
    """One SuperGrok turn. Returns (parsed, lane, model, hop_log). Fail soft."""
    if not gateway_ready():
        return None, "", "", [{
            "role": tmpl, "lane": "openclaw", "model": "",
            "ok": False, "skip": "no_gateway",
        }]
    if tmpl == "news_classify":
        prompt = classifier_prompt(
            str(art.get("title") or ""),
            str(art.get("body") or ""),
            str(art.get("known_at") or art.get("published_at") or ""),
        )
        system = CLASSIFIER_SYSTEM
        budget = 400
    else:
        prompt = analyst_prompt(
            str(art.get("title") or ""),
            str(art.get("body") or ""),
            family,
            cls.event_class if cls else "",
            cls.sign if cls else None,
            cls.q5 if cls else "impulse",
            cls.constraint if cls else "",
            (pack or {}).get("facts") or [],
        )
        system = ANALYST_SYSTEM
        budget = 900
    text, model = _complete(system, prompt, budget, tmpl)
    parsed = extract_json(text) if text else None
    if not isinstance(parsed, dict):
        return None, "openclaw", model, [{
            "role": tmpl, "lane": "openclaw", "model": model,
            "ok": False, "skip": "empty" if not text else "unparseable",
            "excerpt": (text or "")[:160],
        }]
    return parsed, "openclaw", model, [{
        "role": tmpl, "lane": "openclaw", "model": model,
        "ok": True, "excerpt": (text or "")[:160],
    }]
