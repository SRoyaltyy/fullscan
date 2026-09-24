"""xAI SuperGrok models for the OpenClaw gateway.

Sector essays stay on ``xai/grok-4.6``. News hops and the classroom ping
use the cheapest *general* text model that is still above 30B parameters.

Prices and IDs come from https://docs.x.ai/docs/models (2026-09-24).
Every listed general text model is flagship-class (>30B). Mini / voice /
image / coding-only SKUs are excluded from the news picker.

Official API cheapest general completion is ``grok-4.20-0309-non-reasoning``
($1.25 / $2.50, no reasoning tax). SuperGrok via this OpenClaw box
answered on ``xai/grok-4.3`` (same price tier) and did not serve 4.20
(live ping 2026-09-24, run 35971523287). News hops pin 4.3.
``grok-build-0.1`` is cheaper on paper but coding-only — not used here.
"""
from __future__ import annotations

from dataclasses import dataclass

MIN_PARAMS_B = 30


@dataclass(frozen=True)
class XaiModel:
    openclaw_id: str
    input_usd: float
    output_usd: float
    min_params_b: int
    kind: str  # general | coding | legacy_fast
    notes: str = ""

    @property
    def bare(self) -> str:
        return self.openclaw_id.split("/", 1)[-1]

    @property
    def sort_key(self) -> tuple[float, float, str]:
        return (self.input_usd, self.output_usd, self.openclaw_id)


CATALOG: tuple[XaiModel, ...] = (
    XaiModel("xai/grok-4.20-0309-non-reasoning", 1.25, 2.50, 30, "general",
             "cheapest general completion; no reasoning tax"),
    XaiModel("xai/grok-4.3", 1.25, 2.50, 30, "general"),
    XaiModel("xai/grok-4.20-0309-reasoning", 1.25, 2.50, 30, "general"),
    XaiModel("xai/grok-4.20-multi-agent-0309", 1.25, 2.50, 30, "general"),
    XaiModel("xai/grok-4.5", 2.00, 6.00, 30, "general"),
    XaiModel("xai/grok-4.6", 2.00, 6.00, 30, "general",
             "sector default — keep for essays"),
    XaiModel("xai/grok-4.7", 2.00, 6.00, 30, "general"),
)

LEGACY_FAST: tuple[XaiModel, ...] = (
    XaiModel("xai/grok-4.1-fast", 0.20, 0.50, 30, "legacy_fast"),
    XaiModel("xai/grok-4-fast", 0.20, 0.50, 30, "legacy_fast"),
    XaiModel("xai/grok-3", 1.25, 2.50, 30, "legacy_fast"),
)

REFUSE_SUBSTRINGS = (
    "mini", "imagine", "voice", "tts", "stt", "build-0.1", "code-fast",
    "openclaw",
)

DEFAULT_NEWS_MODEL = "xai/grok-4.3"
SECTOR_MODEL = "xai/grok-4.6"
SCREEN_TRY: tuple[str, ...] = (
    "xai/grok-4.1-fast",
    "xai/grok-4-fast",
    DEFAULT_NEWS_MODEL,
)

BLIND_TRY_ORDER: tuple[str, ...] = (
    DEFAULT_NEWS_MODEL,
    "xai/grok-4.20-0309-non-reasoning",
    SECTOR_MODEL,
    "xai/grok-4.7",
)

# Hops that may use Fast. Classify / analyst / meta may not.
FAST_TMPLS = frozenset({
    "news_usability_batch",
    "news_pack_complete",
})
FLOOR_TMPLS = frozenset({
    "news_classify",
    "news_classify_batch",
    "news_meta",
    "news_impact",
})


def normalize_model_id(raw: str) -> str:
    name = str(raw or "").strip()
    if not name:
        return ""
    if "/" not in name:
        return f"xai/{name}"
    return name


def is_refused(model_id: str) -> bool:
    low = normalize_model_id(model_id).lower()
    return any(s in low for s in REFUSE_SUBSTRINGS)


def by_id(model_id: str) -> XaiModel | None:
    want = normalize_model_id(model_id)
    for row in (*CATALOG, *LEGACY_FAST):
        if row.openclaw_id == want or row.bare == want.split("/", 1)[-1]:
            return row
    return None


def above_30b(model_id: str) -> bool:
    if is_refused(model_id):
        return False
    row = by_id(model_id)
    if row is None:
        return normalize_model_id(model_id).startswith("xai/grok")
    return row.min_params_b >= MIN_PARAMS_B and row.kind != "coding"


def pick_cheapest_above_30b(available: list[str] | None = None) -> str:
    if not available:
        return DEFAULT_NEWS_MODEL
    known = {normalize_model_id(x) for x in available if x}
    known |= {normalize_model_id(x).split("/", 1)[-1] for x in available if x}
    hits: list[XaiModel] = []
    for row in (*LEGACY_FAST, *CATALOG):
        if row.min_params_b < MIN_PARAMS_B or is_refused(row.openclaw_id):
            continue
        if row.bare == "grok-3":
            continue
        if row.openclaw_id in known or row.bare in known:
            hits.append(row)
    if hits:
        hits.sort(key=lambda r: r.sort_key)
        return hits[0].openclaw_id
    for raw in available:
        nid = normalize_model_id(raw)
        if nid and above_30b(nid) and "grok-3" not in nid:
            return nid
    return DEFAULT_NEWS_MODEL


def try_order(available: list[str] | None = None) -> list[str]:
    first = pick_cheapest_above_30b(available)
    out: list[str] = []
    for mid in (first, *BLIND_TRY_ORDER):
        if mid and mid not in out and not is_refused(mid):
            out.append(mid)
    return out


def model_for_hop(tmpl: str, available: list[str] | None = None) -> str:
    """Fast for batch usability / M4. Floor for classify, meta, analyst."""
    if tmpl in FAST_TMPLS:
        if not available:
            return SCREEN_TRY[0]
        known = {normalize_model_id(x) for x in available if x}
        known |= {normalize_model_id(x).split("/", 1)[-1] for x in available if x}
        for mid in SCREEN_TRY:
            if mid in known or mid.split("/", 1)[-1] in known:
                return mid
        return DEFAULT_NEWS_MODEL
    if tmpl in FLOOR_TMPLS:
        return DEFAULT_NEWS_MODEL
    return DEFAULT_NEWS_MODEL
