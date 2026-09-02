"""Strict contracts for captain research artifacts.

The model is not allowed to turn price movement into "sentiment" without
showing current evidence. These validators are shared by the post-close
baseline, morning refresh, QC, and tests.
"""
from __future__ import annotations

from urllib.parse import urlparse

SENTIMENTS = {"pos", "neg", "mixed", "none"}
DIRECTIONS = {"up", "down", "flat"}
CONVICTIONS = {"high", "medium", "low"}
ACTIONS = {"OVERRIDE", "SPLIT", "HEAT"}


def _url_ok(value: object) -> bool:
    try:
        u = urlparse(str(value or ""))
        return u.scheme in ("http", "https") and bool(u.netloc)
    except ValueError:
        return False


def validate_cards(cards: list[dict], targets: list[dict],
                   min_coverage: float = 0.8,
                   require_x_record: bool = False
                   ) -> tuple[list[dict], list[str]]:
    """Return validated cards and errors.

    Every industry must be requested; every ticker must be one of the supplied
    SPX/RUT captains; non-`none` sentiment needs a URL + timestamp + fact.
    """
    allowed: dict[str, dict[str, str]] = {}
    for t in targets:
        caps: dict[str, str] = {}
        for idx, key in (("SPX", "spx_leaders"), ("RUT", "rut_leaders")):
            for c in t.get(key) or []:
                tick = str(c.get("ticker") or "").upper()
                if tick:
                    caps[tick] = idx
        allowed[str(t.get("industry") or "")] = caps

    clean: list[dict] = []
    errors: list[str] = []
    seen: set[str] = set()
    for raw in cards or []:
        if not isinstance(raw, dict):
            errors.append("card_not_object")
            continue
        industry = str(raw.get("industry") or "")
        if not industry or industry not in allowed:
            errors.append(f"unexpected_industry:{industry or '?'}")
            continue
        if industry in seen:
            errors.append(f"duplicate_industry:{industry}")
            continue
        direction = str(raw.get("subsector_dir") or "").lower()
        conviction = str(raw.get("conviction") or "").lower()
        action = str(raw.get("action") or "").upper()
        if direction not in DIRECTIONS:
            errors.append(f"{industry}:bad_direction")
            continue
        if conviction not in CONVICTIONS:
            errors.append(f"{industry}:bad_conviction")
            continue
        if action not in ACTIONS:
            errors.append(f"{industry}:bad_action")
            continue
        caps_out = []
        for cap in raw.get("captains") or []:
            ticker = str(cap.get("ticker") or "").upper()
            if ticker not in allowed[industry]:
                errors.append(f"{industry}:invented_ticker:{ticker or '?'}")
                continue
            sent = str(cap.get("sent") or "none").lower()
            if sent not in SENTIMENTS:
                errors.append(f"{industry}:{ticker}:bad_sentiment")
                continue
            evidence = cap.get("evidence") or []
            good_ev = [
                e for e in evidence
                if isinstance(e, dict) and _url_ok(e.get("url"))
                and str(e.get("published_at") or "").strip()
                and str(e.get("fact") or "").strip()
            ]
            # `none` means research found nothing; that negative result still
            # needs an explicit search note, but not a fabricated source.
            searched = str(cap.get("search_note") or "").strip()
            if sent != "none" and not good_ev:
                errors.append(f"{industry}:{ticker}:sentiment_without_evidence")
                continue
            if sent == "none" and not (good_ev or searched):
                errors.append(f"{industry}:{ticker}:no_search_record")
                continue
            x_sent = cap.get("x_sentiment")
            if require_x_record and not isinstance(x_sent, dict):
                errors.append(f"{industry}:{ticker}:missing_x_search_record")
                continue
            # Grok often claims used=true without a URL or mention delta.
            # That is not evidence — coerce unused instead of killing the
            # captain and then the whole industry card.
            if require_x_record and isinstance(x_sent, dict) and x_sent.get("used") is True:
                urls = x_sent.get("sample_urls") or []
                has_url = any(_url_ok(u) for u in urls)
                try:
                    float(x_sent.get("mention_delta_24h"))
                    has_delta = True
                except (TypeError, ValueError):
                    has_delta = False
                if not has_url or not has_delta:
                    why = []
                    if not has_url:
                        why.append("no sample url")
                    if not has_delta:
                        why.append("no mention_delta_24h")
                    cap = dict(cap)
                    cap["x_sentiment"] = {
                        **x_sent,
                        "used": False,
                        "reason": (str(x_sent.get("reason") or "").strip()
                                   or "used=true coerced unused: " + ", ".join(why)),
                    }
                    x_sent = cap["x_sentiment"]
            if (require_x_record and x_sent.get("used") is not True
                    and not str(x_sent.get("reason") or "").strip()):
                errors.append(f"{industry}:{ticker}:x_unavailable_without_reason")
                continue
            cap = dict(cap)
            cap["ticker"] = ticker
            cap["index"] = allowed[industry][ticker]
            cap["sent"] = sent
            cap["evidence"] = good_ev[:4]
            caps_out.append(cap)
        if not caps_out:
            errors.append(f"{industry}:no_valid_captains")
            continue
        card = dict(raw)
        card["industry"] = industry
        card["action"] = action
        card["subsector_dir"] = direction
        card["conviction"] = conviction
        card["captains"] = caps_out
        clean.append(card)
        seen.add(industry)

    needed = max(1, int(len(allowed) * min_coverage + 0.999))
    if len(clean) < needed:
        errors.append(f"coverage:{len(clean)}/{len(allowed)}<required:{needed}")
    return clean, errors


def opportunity_tickers_valid(payload: dict, cards: list[dict]) -> list[str]:
    allowed = {
        str(c.get("ticker") or "").upper()
        for card in cards for c in (card.get("captains") or [])
    }
    errors = []
    for opp in payload.get("opportunities") or []:
        for t in opp.get("tickers") or []:
            if str(t).upper() not in allowed:
                errors.append(f"opportunity_invented_ticker:{t}")
    return errors
