"""Morning factor-mine scan: oppset_union aisle + Clock-B gates.

KEEP prove is evidence this *methodology* works — not a recipe to pin:

  * #282 ``union_e_fresh_h3`` long KEEP ran on the restored multi-src
    panel ∪ Theme Radar Clock-B oppset aisle
  * #278 ``c4_downside`` short KEEP is Clock-B #4 (``clk_neg_weak_fail``)

This module is the **ticket / open-pack** scan path. It does not remine
the cash book, does not change ``flatten_robust`` / Webull / paper_open,
and does not publish a parallel e_fresh list next to flatten.

Morning factor-mine picks:

  1. Aisle = session panel rows ∪ T−1 oppset-flagged names
  2. Stamp Clock-B #279 combo flags on every aisle row
  3. Filter / rank with those gates (longs drop ``clk_ext_veto``; KEEP
     long signals and Clock-B #4 shorts float first) *before* top_n

``FULLSCAN_OPPSET_UNION`` stays the remine opt-in. Tickets always use
the KEEP aisle so open-pack factor-mine buys can diverge after restamp.
"""
from __future__ import annotations

from . import clock_b_tells as cbt
from . import factor_mine as fm
from . import oppset_clock_b as opp

METHODOLOGY = "oppset_union+clock_b"
AISLE = "panel ∪ Theme Radar Clock-B oppset"
LIVE_UNTOUCHED = "flatten_robust"

# James core longs from #279 plus the #282 e_fresh-style earnings atom.
KEEP_LONG_GATES = (
    "clk_mom_break_peer",
    "clk_fresh_cat_coil",
    "clk_hold_vs_sector",
    "clk_nr7_mom",
)
KEEP_SHORT_GATE = "clk_neg_weak_fail"
LONG_VETO = "clk_ext_veto"


def e_fresh_atom(row: dict) -> bool:
    """#282 KEEP long atom: knowable E within 1 session, not 🚨."""
    days = row.get("erd_days_since_E")
    flag = row.get("erd_flag_E")
    if days is None or int(days) > 1:
        return False
    if flag is None or int(flag) < 0:
        return False
    return not bool(row.get("alarm"))


def keep_long_score(row: dict) -> float:
    """How many KEEP-proven long methods fire. Not a Book%."""
    n = 0.0
    for key in KEEP_LONG_GATES:
        if cbt.combo_true(row, key):
            n += 1.0
    if e_fresh_atom(row):
        n += 1.0
    if fm.on_oppset(row):
        n += 0.5
    return n


def keep_short_hit(row: dict) -> bool:
    """#278 / Clock-B #4 short KEEP methodology."""
    return bool(cbt.combo_true(row, KEEP_SHORT_GATE))


def long_veto(row: dict, rec: dict | None = None) -> bool:
    """Clock-B #5 extreme-extension veto. Shorts that *require* it stay."""
    req = (rec or {}).get("require") or {}
    if req.get(LONG_VETO):
        return False
    side = str((rec or {}).get("side") or "long")
    if side == "short":
        return False
    return bool(cbt.combo_true(row, LONG_VETO))


def _slim_oppset_row(date: str, hit: dict) -> dict:
    """Flagged name not already on the session panel. T−1 membership only."""
    row = {
        "date": date,
        "ticker": hit["ticker"],
        "sources": ["oppset"],
        "boxes": {},
        "oppset": True,
        "opp_any": bool(hit.get("any_opp")),
        "opp_rvol": hit.get("rvol"),
        "opp_gap_pct": hit.get("gap_pct"),
        "opp_change_pct": hit.get("change_pct"),
        "opp_finviz_asof": hit.get("finviz_asof"),
        "src_rank": 50,
    }
    cbt.stamp_row(row)
    return row


def aisle_rows(date: str, panel_rows: list | None,
               index: dict | None = None) -> list[dict]:
    """KEEP aisle: session panel ∪ Theme Radar Clock-B oppset.

    Stamps oppset + Clock-B flags. Unions flagged tickers missing from
    the panel as slim T−1 membership rows (no same-day Gap/RelVol).
    """
    idx = index if index is not None else opp.load_index()
    out: list[dict] = []
    seen: set[str] = set()
    for raw in panel_rows or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        opp.stamp_row(row, idx)
        if not row.get("_clock_b"):
            cbt.stamp_row(row)
        t = str(row.get("ticker") or "").strip().upper()
        if t:
            seen.add(t)
            row["ticker"] = t
        row.setdefault("date", date)
        out.append(row)
    d = str(date or "")[:10]
    for (morning, ticker), hit in idx.items():
        if morning != d:
            continue
        t = str(ticker or "").strip().upper()
        if not t or t in seen:
            continue
        slim = _slim_oppset_row(d, {**hit, "ticker": t})
        out.append(slim)
        seen.add(t)
    return out


def morning_rank_key(row: dict, rec: dict) -> tuple:
    """KEEP-methodology tier first, then the recipe's existing rank."""
    side = str(rec.get("side") or "long")
    if side == "short":
        tier = 0 if keep_short_hit(row) else 1
        score = -keep_long_score(row)
    else:
        score = keep_long_score(row)
        tier = 0 if score > 0 else 1
        score = -score
    return (tier, score) + tuple(fm.rank_key(row, rec))


def pick_morning(rows: list[dict], rec: dict) -> list[dict]:
    """Factor-mine morning pick: Clock-B filter + KEEP rank, then top_n."""
    gated = [r for r in (rows or []) if not long_veto(r, rec)]
    kept = [r for r in gated if fm.matches(r, rec)]
    kept.sort(key=lambda r: morning_rank_key(r, rec))
    return kept[: int(rec.get("top_n") or fm.TOP_N_DEFAULT)]


def scan_meta(look: dict | None = None, rows: list | None = None) -> dict:
    n = len(rows or [])
    n_opp = sum(1 for r in (rows or []) if fm.on_oppset(r))
    n_clk = sum(1 for r in (rows or []) if r.get("_clock_b"))
    return {
        "methodology": METHODOLOGY,
        "aisle": AISLE,
        "live_untouched": LIVE_UNTOUCHED,
        "n_aisle": n,
        "n_oppset": n_opp,
        "n_clock_b": n_clk,
        "look_source": (look or {}).get("source"),
        "gates": (
            "Clock-B #279; long veto clk_ext_veto; "
            "short KEEP clk_neg_weak_fail; "
            "long KEEP atoms e_fresh + James core 1/2/6/10"
        ),
        "note": (
            "fee-KEEP recipes prove this aisle+gate path. "
            "Not a pinned e_fresh list. flatten_robust stays LIVE money."
        ),
    }
