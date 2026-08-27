"""Book picks slice for lookback: every buy/sell name and the layer scores that day.

Run after src.book_lookback so the winner report already exists:
  python -m src.lookback_picks --date 2026-08-20
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .book_learn import _load_panel, load_frame
from .book_lookback import (
    BOX_COLS,
    BOOK_DIR,
    DAILY,
    SCORE,
    _boxes,
    _catalyst,
    _digest_hits,
    _finviz_row,
    _frame_row,
    _fwd_map,
    _heat_hits,
    _icon,
    _inventory,
    _jload,
    _judge_tilt,
    _missing_layers,
    _news_actions,
    _parse_date,
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
        break
    return L


def run(date: str | None = None) -> dict:
    date = _parse_date(date)
    frame = load_frame(date)
    book = _jload("data", "stock_book", f"{date}_stock_book.json")
    panel = _load_panel()
    missing = _missing_layers(_inventory(date))
    picks = book_picks(date, frame, book, panel, missing)
    block = "\n".join(render_picks(picks)) + "\n"
    for path in (
        SCORE / "BOOK_LOOKBACK.md",
        DAILY / f"{date}_lookback.md",
    ):
        if path.exists():
            text = path.read_text(encoding="utf-8")
            if "## Book picks" in text:
                pre, _mid, rest = text.partition("## Book picks")
                # drop old picks through Files or end
                cut = rest.find("\n## Files")
                tail = rest[cut:] if cut >= 0 else ""
                text = pre.rstrip() + "\n\n" + block + tail.lstrip("\n")
            elif "## Files" in text:
                text = text.replace("## Files", block + "## Files", 1)
            else:
                text = text.rstrip() + "\n\n" + block
            path.write_text(text, encoding="utf-8")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Book lookback — " + date + "\n\n" + block, encoding="utf-8")
    js = BOOK_DIR / f"{date}_lookback.json"
    if js.exists():
        try:
            payload = json.loads(js.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"date": date}
        payload["picks"] = picks
        js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(block[:4000])
    print(f"[lookback-picks] wrote picks for {date}")
    return picks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    run(date=args.date)


if __name__ == "__main__":
    main()
