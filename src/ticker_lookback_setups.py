"""Featured lookback setups — the ones that paid market-wide.

Wired into the Action so a run shows:
  • which setups to use (verdict + market edge)
  • the mine date window those edges came from
  • every date in *this* run that printed one

Edges come from ``03_scoreboard/ticker_lookback_mine.json`` when present
(2659 liquid names, 2026-07-31 → 2026-08-27). Hardcoded fallbacks match
that published mine so the Action still renders if the JSON is missing.
"""
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
MINE_JSON = ROOT / "03_scoreboard" / "ticker_lookback_mine.json"

# Unique featured set. ``blue|neutral`` is the 3-day *stretch* (not region).
# ``first_crack`` covers the published ``alarm|good`` fade (same idea).
FEATURED: list[dict[str, Any]] = [
    {
        "id": "tag_factor:blue|heat=bad",
        "mine_bucket": "tag_factor",
        "mine_key": "blue|heat=bad",
        "label": "🔵 + heat red",
        "short": "🔵+heat🔴",
        "highlight": ["heat"],
        "when": "Blue on a name whose heat is red (heat=bad).",
        "verdict": "long",
        "fallback": {"n": 138, "edge_1d": 3.092, "edge_3d": None, "edge_1w": None},
    },
    {
        "id": "pair:vol=good|ab=good",
        "mine_bucket": "pair",
        "mine_key": "vol=good|ab=good",
        "label": "vol green + AB green",
        "short": "vol+AB",
        "highlight": ["vol", "ab"],
        "when": "Yesterday's tape: both vol and analyst-barometer green.",
        "verdict": "long",
        "fallback": {"n": 349, "edge_1d": 2.763, "edge_3d": 4.218, "edge_1w": 1.755},
    },
    {
        "id": "pair:gen=bad|vol=good",
        "mine_bucket": "pair",
        "mine_key": "gen=bad|vol=good",
        "label": "vol green + gen red",
        "short": "vol+gen🔴",
        "highlight": ["vol", "gen"],
        "when": "Yesterday's tape is green vol against a red general-market box.",
        "verdict": "long",
        "fallback": {"n": 441, "edge_1d": 1.906, "edge_3d": 2.656, "edge_1w": 5.991},
    },
    {
        "id": "tag_factor:blue|heat=good",
        "mine_bucket": "tag_factor",
        "mine_key": "blue|heat=good",
        "label": "🔵 + heat green",
        "short": "🔵+heat🟢",
        "highlight": ["heat"],
        "when": "Blue on a name whose heat is green.",
        "verdict": "long",
        "fallback": {"n": 372, "edge_1d": 1.759, "edge_3d": None, "edge_1w": None},
    },
    {
        "id": "pair:join=bad|vol=good",
        "mine_bucket": "pair",
        "mine_key": "join=bad|vol=good",
        "label": "vol green + join red",
        "short": "vol+join🔴",
        "highlight": ["vol", "join"],
        "when": "Yesterday's tape is green vol against a red join box.",
        "verdict": "long",
        "fallback": {"n": 467, "edge_1d": 1.738, "edge_3d": 3.142, "edge_1w": 3.443},
    },
    {
        "id": "tag_factor:alarm|heat=bad",
        "mine_bucket": "tag_factor",
        "mine_key": "alarm|heat=bad",
        "label": "🚨 + heat red",
        "short": "🚨+heat🔴",
        "highlight": ["heat"],
        "when": "Alarm on a name whose heat is red — fade, do not chase.",
        "verdict": "fade",
        "fallback": {"n": 87, "edge_1d": -1.506, "edge_3d": None, "edge_1w": None},
    },
    {
        "id": "factor:judge=neutral",
        "mine_bucket": "factor",
        "mine_key": "judge=neutral",
        "label": "judge yellow",
        "short": "jdg🟡",
        "highlight": ["judge"],
        "when": "Pre-open judge box is yellow (mixed / no clean read).",
        "verdict": "long",
        "fallback": {"n": 762, "edge_1d": 1.351, "edge_3d": 1.634, "edge_1w": None},
    },
    {
        "id": "tag_context:first_crack",
        "mine_bucket": "tag_context",
        "mine_key": "first_crack",
        "label": "🚨 first crack (still-green row)",
        "short": "first crack",
        "highlight": [],
        "when": "Alarm on a still-green 09:30 row — first crack. Same idea as alarm|good.",
        "verdict": "fade",
        "fallback": {"n": 984, "edge_1d": -1.223, "edge_3d": -1.264, "edge_1w": -2.028},
    },
    {
        "id": "tag_stretch:blue|neutral",
        "mine_bucket": "tag_stretch",
        "mine_key": "blue|neutral",
        "label": "🔵 on mixed 3-day stretch",
        "short": "🔵 stretch",
        "highlight": [],
        "when": "Blue while the 3-day color stretch is mixed (not a clean green/red run).",
        "verdict": "long",
        "fallback": {"n": 4149, "edge_1d": 0.882, "edge_3d": 1.185, "edge_1w": 1.690},
    },
]

_BOOK_CACHE: list[dict[str, Any]] | None = None
_MINE_CACHE: dict[str, Any] | None = None
_MINE_LOADED = False


def _load_mine() -> dict[str, Any] | None:
    global _MINE_CACHE, _MINE_LOADED
    if _MINE_LOADED:
        return _MINE_CACHE
    _MINE_LOADED = True
    if not MINE_JSON.is_file():
        _MINE_CACHE = None
        return None
    try:
        _MINE_CACHE = json.loads(MINE_JSON.read_text())
    except (OSError, json.JSONDecodeError):
        _MINE_CACHE = None
    return _MINE_CACHE


def mine_window(mine: dict[str, Any] | None = None) -> dict[str, Any]:
    mine = mine if mine is not None else _load_mine()
    meta = (mine or {}).get("meta") or {}
    return {
        "from_date": meta.get("from_date") or "2026-07-31",
        "to_date": meta.get("to_date") or "2026-08-27",
        "n_tickers": meta.get("n_names") or meta.get("n_tickers") or 2659,
        "n_printed": meta.get("n_rows") or meta.get("n_printed") or 29244,
        "source": str(MINE_JSON.relative_to(ROOT)) if MINE_JSON.is_file() else "hardcoded",
    }


def _mine_row(mine: dict[str, Any] | None, bucket: str, key: str) -> dict[str, Any] | None:
    if not mine:
        return None
    rows = (mine.get("buckets") or mine.get("by_bucket") or {}).get(bucket) or []
    for row in rows:
        if row.get("key") == key:
            return row
    return None


def _stats_from_row(row: dict[str, Any] | None, fallback: dict[str, Any]) -> dict[str, Any]:
    src = row or fallback
    return {
        "n": src.get("n", fallback["n"]),
        "edge_1d": src.get("edge", src.get("edge_1d", fallback["edge_1d"])),
        "edge_3d": src.get("3d_mean_xs", src.get("edge_3d", fallback.get("edge_3d"))),
        "edge_1w": src.get("1w_mean_xs", src.get("edge_1w", fallback.get("edge_1w"))),
        "read": src.get("verdict") or src.get("read") or "",
    }


def featured_book(mine: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    global _BOOK_CACHE
    use_cache = mine is None and _BOOK_CACHE is not None
    if use_cache:
        return [dict(s) for s in _BOOK_CACHE]
    mine = mine if mine is not None else _load_mine()
    window = mine_window(mine)
    out: list[dict[str, Any]] = []
    for spec in FEATURED:
        stats = _stats_from_row(
            _mine_row(mine, spec["mine_bucket"], spec["mine_key"]),
            spec["fallback"],
        )
        out.append({
            "id": spec["id"],
            "label": spec["label"],
            "short": spec.get("short") or spec["label"],
            "highlight": list(spec.get("highlight") or []),
            "when": spec["when"],
            "verdict": spec["verdict"],
            "mine_bucket": spec["mine_bucket"],
            "mine_key": spec["mine_key"],
            "mine_from": window["from_date"],
            "mine_to": window["to_date"],
            **stats,
        })
        if not stats["read"]:
            out[-1]["read"] = spec["verdict"]
    if mine is None or _load_mine() is mine:
        _BOOK_CACHE = [dict(s) for s in out]
    return out


def _box_tone(day: dict[str, Any], key: str) -> str:
    raw = ((day.get("boxes") or {}).get(key))
    if isinstance(raw, dict):
        return str(raw.get("tone") or "")
    return str(raw or "")


def _is_blue(day: dict[str, Any]) -> bool:
    sig = day.get("signal_improved")
    if isinstance(sig, dict):
        return sig.get("signal") == "blue" or bool(sig.get("hit"))
    return bool(sig)


def _tag_context(day: dict[str, Any]) -> list[str]:
    raw = day.get("tag_context") or []
    if isinstance(raw, str):
        return [raw] if raw else []
    return [str(x) for x in raw]


def _fwd(day: dict[str, Any]) -> dict[str, Any]:
    return day.get("forward_returns") or day.get("price_changes") or day.get("fwd") or {}


def match_day(day: dict[str, Any], book: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Which featured setups this 09:30 row printed."""
    region = day.get("region") or {}
    stretch = day.get("stretch") or {}
    hits: list[str] = []

    if _is_blue(day) and _box_tone(day, "heat") == "bad":
        hits.append("tag_factor:blue|heat=bad")
    if _box_tone(day, "vol") == "good" and _box_tone(day, "ab") == "good":
        hits.append("pair:vol=good|ab=good")
    if _box_tone(day, "gen") == "bad" and _box_tone(day, "vol") == "good":
        hits.append("pair:gen=bad|vol=good")
    if _is_blue(day) and _box_tone(day, "heat") == "good":
        hits.append("tag_factor:blue|heat=good")
    if _box_tone(day, "join") == "bad" and _box_tone(day, "vol") == "good":
        hits.append("pair:join=bad|vol=good")
    if day.get("signal_alarm") and _box_tone(day, "heat") == "bad":
        hits.append("tag_factor:alarm|heat=bad")
    if _box_tone(day, "judge") == "neutral":
        hits.append("factor:judge=neutral")
    if "first_crack" in _tag_context(day) or (
        day.get("signal_alarm") and region.get("tone") == "good"
    ):
        hits.append("tag_context:first_crack")
    if _is_blue(day) and stretch.get("tone") == "neutral":
        hits.append("tag_stretch:blue|neutral")

    catalog = book if book is not None else {s["id"]: s for s in featured_book()}
    return [{**catalog[i], "hit": True} for i in hits if i in catalog]


def _payload_names(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return payload.get("names") or payload.get("tickers") or []


def attach_setups(payload: dict[str, Any]) -> dict[str, Any]:
    """Stamp each day + the payload with featured setups and this-run dates."""
    book_list = featured_book()
    catalog = {s["id"]: s for s in book_list}
    window = mine_window()
    payload["setup_book"] = book_list
    payload["setup_window"] = window
    hits: list[dict[str, Any]] = []
    for t in _payload_names(payload):
        for d in t.get("days") or []:
            matched = match_day(d, book=catalog)
            d["setups"] = matched
            fwd = _fwd(d)
            for m in matched:
                hits.append({
                    "ticker": t.get("ticker"),
                    "date": d.get("date"),
                    "setup_id": m["id"],
                    "label": m["label"],
                    "verdict": m["verdict"],
                    "edge_1d": m["edge_1d"],
                    "edge_3d": m.get("edge_3d"),
                    "edge_1w": m.get("edge_1w"),
                    "n": m["n"],
                    "mine_from": m["mine_from"],
                    "mine_to": m["mine_to"],
                    "this_1d": fwd.get("1d"),
                    "this_3d": fwd.get("3d"),
                    "this_1w": fwd.get("1w"),
                })
    hits.sort(key=lambda h: (str(h.get("date") or ""), str(h.get("ticker") or ""), str(h.get("setup_id") or "")))
    payload["setup_hits"] = hits
    by: dict[str, list[dict[str, Any]]] = {}
    for h in hits:
        by.setdefault(h["setup_id"], []).append(h)
    this_run: list[dict[str, Any]] = []
    for s in book_list:
        rows = by.get(s["id"]) or []
        xs = [h["this_1d"] for h in rows if h.get("this_1d") is not None]
        this_run.append({
            **s,
            "hits_this_run": len(rows),
            "this_run_mean_1d": round(sum(xs) / len(xs), 3) if xs else None,
            "hit_dates": [
                {"ticker": h["ticker"], "date": h["date"], "this_1d": h.get("this_1d")}
                for h in rows
            ],
        })
    payload["setup_this_run"] = this_run
    return payload


def ensure_setups(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("setup_book") is None:
        attach_setups(payload)
    return payload


def pct(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):+.2f}"
    except (TypeError, ValueError):
        return "—"


def render_setup_markdown(payload: dict[str, Any], include_dates: bool = True) -> str:
    """Legend of setups that paid. Dates belong on the color chart unless asked."""
    ensure_setups(payload)
    window = payload.get("setup_window") or mine_window()
    book = payload.get("setup_book") or featured_book()
    this_run = payload.get("setup_this_run") or []
    hits = payload.get("setup_hits") or []
    lines = [
        "## Setups that paid market-wide",
        "",
        f"Mine window: **{window.get('from_date')} → {window.get('to_date')}** "
        f"· {window.get('n_tickers')} liquid names · {window.get('n_printed')} printed days.",
        "These overlay the red/yellow/green chart — they do not replace it. "
        "Edge is excess vs the same-day universe median, minus the +0.27 sample-mean tilt. "
        "Bare 🔵 / 🚨 / ⚪ and 🔵-on-red (`turn`) did **not** replicate.",
        "",
        "| Setup | Use | Market n | 1d edge | 3d xs | 1w xs | This run | This-run +1d |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    run_by = {s["id"]: s for s in this_run}
    for s in book:
        r = run_by.get(s["id"]) or {}
        lines.append(
            f"| {s['label']} | **{s['verdict']}** | {s['n']} | {pct(s['edge_1d'])} | "
            f"{pct(s.get('edge_3d'))} | {pct(s.get('edge_1w'))} | "
            f"{r.get('hits_this_run') or 0} | {pct(r.get('this_run_mean_1d'))} |"
        )
    if not include_dates:
        return "\n".join(lines)
    lines += ["", "### Dates these setups printed (this run)", ""]
    if not hits:
        lines.append("_None of the featured setups printed on these names in this window._")
        return "\n".join(lines)
    lines += [
        "| Date | Ticker | Setup | Use | This +1d | Market 1d edge | Mine |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for h in hits:
        lines.append(
            f"| {h.get('date')} | {h.get('ticker')} | {h.get('label')} | **{h.get('verdict')}** | "
            f"{pct(h.get('this_1d'))} | {pct(h.get('edge_1d'))} "
            f"(n={h.get('n')}) | {h.get('mine_from')} → {h.get('mine_to')} |"
        )
    return "\n".join(lines)


def box_highlights(day: dict[str, Any]) -> dict[str, str]:
    """Factor box → verdict for cells this setup lights up on the color chart."""
    out: dict[str, str] = {}
    for s in day.get("setups") or []:
        verdict = str(s.get("verdict") or "")
        for key in s.get("highlight") or []:
            if key not in out or verdict == "fade":
                out[key] = verdict
    return out


def row_setup_class(day: dict[str, Any]) -> str:
    hits = day.get("setups") or []
    if not hits:
        return ""
    verdicts = {s.get("verdict") for s in hits}
    if verdicts == {"fade"}:
        return "has-setup setup-fade"
    if verdicts == {"long"}:
        return "has-setup setup-long"
    return "has-setup setup-mixed"


def setup_chips_html(day: dict[str, Any]) -> str:
    chips = []
    for s in day.get("setups") or []:
        tone = "good" if s.get("verdict") == "long" else "bad"
        label = s.get("short") or s.get("label")
        title = (
            f"{s.get('label')} · {s.get('verdict')} · "
            f"mkt {pct(s.get('edge_1d'))} (n={s.get('n')}) · "
            f"{s.get('mine_from')} → {s.get('mine_to')}"
        )
        chips.append(
            f'<span class="setup-chip {tone}" title="{html.escape(title)}">'
            f'{html.escape(str(label))} {html.escape(pct(s.get("edge_1d")))}</span>'
        )
    return " ".join(chips)


def ticker_setup_lines(rec: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for d in rec.get("days") or []:
        fwd = _fwd(d)
        for s in d.get("setups") or []:
            rows.append({
                "date": d.get("date"),
                "label": s.get("label"),
                "verdict": s.get("verdict"),
                "this_1d": fwd.get("1d"),
                "edge_1d": s.get("edge_1d"),
                "n": s.get("n"),
            })
    return rows


def render_setup_html(payload: dict[str, Any]) -> str:
    """Compact legend only. Dates sit on the color-chart rows, not a second table."""
    ensure_setups(payload)
    window = payload.get("setup_window") or mine_window()
    book = payload.get("setup_book") or featured_book()
    this_run = {s["id"]: s for s in (payload.get("setup_this_run") or [])}
    book_rows = []
    for s in book:
        r = this_run.get(s["id"]) or {}
        tone = "good" if s.get("verdict") == "long" else "bad"
        book_rows.append(
            f'<tr><td>{html.escape(s["label"])}</td>'
            f'<td class="{tone}"><strong>{html.escape(s["verdict"])}</strong></td>'
            f'<td>{s["n"]}</td><td>{html.escape(pct(s.get("edge_1d")))}</td>'
            f'<td>{html.escape(pct(s.get("edge_3d")))}</td>'
            f'<td>{html.escape(pct(s.get("edge_1w")))}</td>'
            f'<td>{r.get("hits_this_run") or 0}</td>'
            f'<td>{html.escape(pct(r.get("this_run_mean_1d")))}</td></tr>'
        )
    return f"""
<section class="setups" id="setups">
<h2>Setups that paid market-wide</h2>
<p>Mine window: <strong>{html.escape(str(window.get("from_date")))} → {html.escape(str(window.get("to_date")))}</strong>
 · {html.escape(str(window.get("n_tickers")))} liquid names
 · {html.escape(str(window.get("n_printed")))} printed days.
 Overlay on the red/yellow/green chart below — gold ring on the boxes that fired.</p>
<p class="muted">Edge is excess vs the same-day universe median, minus the +0.27 sample-mean tilt.
Bare 🔵 / 🚨 / ⚪ and 🔵-on-red (<code>turn</code>) did <strong>not</strong> replicate.</p>
<div class="sheet"><table>
<thead><tr><th>Setup</th><th>Use</th><th>Market n</th><th>1d edge</th><th>3d xs</th><th>1w xs</th><th>This run</th><th>This-run +1d</th></tr></thead>
<tbody>{''.join(book_rows)}</tbody></table></div>
</section>"""


def setup_labels(day: dict[str, Any]) -> str:
    return "; ".join(
        f'{s.get("short") or s.get("label")} {pct(s.get("edge_1d"))}'
        for s in (day.get("setups") or [])
    )
