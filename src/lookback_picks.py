"""Book picks slice for lookback: every buy/sell name and the layer scores that day."""
from __future__ import annotations

from .book_lookback import (
    BOX_COLS,
    _boxes,
    _catalyst,
    _digest_hits,
    _finviz_row,
    _frame_row,
    _fwd_map,
    _heat_hits,
    _icon,
    _judge_tilt,
    _news_actions,
    _signals,
    _tick,
)


def book_picks(date, frame, book, panel, missing):
    out = {}
    if not book:
        return out
    for h, entry in (book.get("books") or {}).items():
        sides = {}
        for side in ("buy", "sell"):
            rows = []
            for i, rec in enumerate(entry.get(side) or [], 1):
                t = _tick(rec.get("ticker"))
                fr = _frame_row(frame, t)
                ev = {
                    "news_actions": _news_actions(date, t),
                    "digest": _digest_hits(date, t),
                    "judge_tilt": _judge_tilt(date, t),
                    "finviz": _finviz_row(date, t),
                    "heat": _heat_hits(date, t),
                    "catalyst": _catalyst(date, t),
                }
                sig = _signals(fr)
                try:
                    score = round(float(rec.get("score") or 0), 3)
                except (TypeError, ValueError):
                    score = 0.0
                fwd = _fwd_map(panel, date, t)
                rows.append({
                    "rank": i,
                    "ticker": t,
                    "side": side,
                    "book_score": score,
                    "sector": rec.get("sector") or (None if fr is None else fr.get("sector")),
                    "size": rec.get("size") or (None if fr is None else fr.get("size")),
                    "reasons": rec.get("reasons"),
                    "signals": sig,
                    "boxes": _boxes(sig, ev, side == "buy", missing),
                    "fwd_pct": {
                        k: (None if v is None else round(v * 100, 2))
                        for k, v in fwd.items()
                    },
                })
            sides[side] = rows
        out[h] = sides
    return out


def render_picks(picks: dict) -> list[str]:
    if not picks:
        return ["## Book picks — scores that day", "", "_No stock_book.json for this date._", ""]
    L = [
        "## Book picks — scores that day",
        "",
        "What the ranker actually put in buy/sell, with every layer score and the same color boxes.",
        "`book` is the combined ranker score. `1d` is what the name did *after* that session.",
        "",
    ]
    # 1d first; other horizons only if they exist
    order = [h for h in ("1d", "3d", "1w", "2w", "1m") if h in picks]
    for extra in picks:
        if extra not in order:
            order.append(extra)
    for h in order:
        sides = picks.get(h) or {}
        L.append(f"### {h} book")
        L.append("")
        for side in ("buy", "sell"):
            rows = sides.get(side) or []
            L.append(f"**{side.upper()}** ({len(rows)})")
            L.append("")
            if not rows:
                L.append("_empty_")
                L.append("")
                continue
            heads = " | ".join(
                ["#", "Ticker", "book", "1d"] + [lab for _, lab in BOX_COLS]
            )
            bars = "|".join(["---"] * (4 + len(BOX_COLS)))
            L.append("| " + heads + " |")
            L.append("|" + bars + "|")
            for r in rows:
                fwd = (r.get("fwd_pct") or {}).get("1d")
                fwd_s = "n/a" if fwd is None else f"{fwd:+.1f}%"
                cells = [_icon((r.get("boxes") or {}).get(k, "missing")) for k, _ in BOX_COLS]
                L.append(
                    f"| {r['rank']} | {r['ticker']} | {r['book_score']:+.3f} | {fwd_s} | "
                    + " | ".join(cells) + " |"
                )
            L.append("")
            L.append("| Ticker | size | sector | reasons |")
            L.append("|--------|------|--------|---------|")
            for r in rows:
                why = str(r.get("reasons") or "").replace("|", "/")
                L.append(
                    f"| {r['ticker']} | {r.get('size') or '?'} | {r.get('sector') or '?'} | `{why}` |"
                )
            L.append("")
        if h != "1d":
            # keep 3d/1w to the score board only; skip extra reason dumps to stay readable
            break
    return L
