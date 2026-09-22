"""Lane-on-Elite effectiveness backtest. Research-only.

Same rubric as the #314 deterministic board (Q5, one event_class, family
winners/losers, no scores). Clock is the next RTH after News Time.
Horizons 0-1d and 2/3/4/5d, 1-4w secondary. One entity per session.

Tier A is not the 293k raw wraps and not the full ~9.5k impulse book.
Two current-flash hops per title do not finish in one Actions runner, so
Tier A is:

  * every unique Elite title #314 graded on the 0-1d rubric
  * plus every converge/clash group
  * plus AMRX lanreotide
  * plus the 2026-09-18 SECZ/COIN/CRCL/SCHW stack

Hop order on 429: abandon that provider, try the next current hopper.
Never glm-4-flash-250414 / old flash / pre-2025. A row is Lane only when
its watermark is lane::model::inference_source from a live current model.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.news_impact.grade import (
    evaluation_targets,
    grade_results,
    skips_01d_horizon,
    ungraded_reason,
)
from src.news_impact.lane_env import require_lane_env
from src.news_impact.ledger import build_ledger, clash_converge_ids, session_of
from src.news_impact.theme_radar_board import (
    HORIZONS,
    _fmt_ret,
    _gradeable_call,
    _pct,
    _ripper,
)
from src.news_impact.unique_title import is_signed_listed

SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_THEME_RADAR_LANE.md")
BOARD_JSON = Path("01_daily/news/all_news_impact_theme_radar_lane.json")
MANIFEST = Path("01_daily/news/lane_tier/manifest.json")
DATES = Path("01_daily/news/lane_tier/dates.json")
SHARD_DIR = Path("01_daily/news/lane_shards")
PUBLISHED_314 = Path("01_daily/news/all_news_impact_theme_radar.json")

STACK_SESSION = "2026-09-18"
STACK_TICKERS = frozenset({"SECZ", "COIN", "CRCL", "SCHW"})
SHORT = {"0-1d", "2d", "3d", "4d", "5d"}
TAPE_KEYS = (
    "entry_date", "entry_open", "entry_clock", "through",
    "ret_1d", "ret_2d", "ret_3d", "ret_4d", "ret_5d",
    "ret_20d", "ret_63d", "ret_horizon",
)
AGREE_KEYS = (
    ("ret_1d", "agree_1d"),
    ("ret_2d", "agree_2d"),
    ("ret_3d", "agree_3d"),
    ("ret_4d", "agree_4d"),
    ("ret_5d", "agree_5d"),
    ("ret_20d", "agree_20d"),
    ("ret_63d", "agree_63d"),
    ("ret_horizon", "agree_horizon"),
)


def _agree(direction: str, ret: Any) -> bool | None:
    if ret is None or direction not in {"up", "down"}:
        return None
    try:
        value = float(ret)
    except (TypeError, ValueError):
        return None
    if direction == "up":
        return value > 0
    return value < 0


def watermark_of(row: dict | None) -> str:
    if not row:
        return ""
    lane = str(row.get("lane") or "")
    model = str(row.get("model") or "")
    src = str(row.get("inference_source") or "")
    if not lane and not model and not src:
        return ""
    return f"{lane}::{model}::{src}"


def is_lane_ok(row: dict | None) -> bool:
    """Live current-flash hop. Deterministic and banned models are not Lane."""
    if not row:
        return False
    lane = str(row.get("lane") or "")
    model = str(row.get("model") or "")
    if lane in {"", "deterministic"} or not model:
        return False
    try:
        from src.lane_route import is_banned_primary, is_resumable_free_watermark
    except Exception:  # noqa: BLE001
        return False
    if is_banned_primary(model):
        return False
    return is_resumable_free_watermark(lane, model)


def is_01d_graded(row: dict) -> bool:
    for g in row.get("performance") or []:
        if not isinstance(g, dict) or g.get("ret_1d") is None:
            continue
        if _gradeable_call(g, row, apply_skip=True):
            return True
    return False


def tier_reasons(row: dict, graded_ids: set[str], clash_ids: set[str]) -> list[str]:
    reasons: list[str] = []
    aid = str(row.get("article_id") or "")
    if aid and aid in graded_ids:
        reasons.append("graded_0_1d")
    if aid and aid in clash_ids:
        reasons.append("converge_or_clash")
    title = str(row.get("title") or "").lower()
    tick = str(row.get("ticker_hint") or "").upper()
    if tick == "AMRX" and "lanreotide" in title:
        reasons.append("amrx_lanreotide")
    session = session_of(row)
    published = str(row.get("published_at") or "")
    if tick in STACK_TICKERS and (
        session == STACK_SESSION or published.startswith(STACK_SESSION)
    ):
        reasons.append("stack_2026-09-18")
    return reasons


def select_tier(
    rows: list[dict],
    graded_ids: set[str],
    clash_ids: set[str],
) -> list[tuple[dict, list[str]]]:
    out: list[tuple[dict, list[str]]] = []
    for row in rows:
        reasons = tier_reasons(row, graded_ids, clash_ids)
        if reasons:
            out.append((row, reasons))
    return out


def _calls(rows: list[dict], horizon: str) -> dict[tuple[str, str], bool]:
    spec = next((h for h in HORIZONS if h[0] == horizon), None)
    if spec is None:
        return {}
    _label, _ret, agree_k, apply_skip = spec
    out: dict[tuple[str, str], bool] = {}
    for row in rows:
        aid = str(row.get("article_id") or "")
        if not aid:
            continue
        for g in row.get("performance") or []:
            if not isinstance(g, dict):
                continue
            tick = str(g.get("ticker") or "").upper()
            if not tick or not _gradeable_call(g, row, apply_skip=apply_skip):
                continue
            ret_k = spec[1]
            if g.get(ret_k) is None:
                continue
            out[(aid, tick)] = g.get(agree_k) is True
    return out


def _pack_rate(hits: int, n: int) -> dict[str, Any]:
    return {
        "hits": hits,
        "n": n,
        "hit_rate": round(hits / n, 4) if n else None,
        "text": _pct(hits, n),
    }


def _rate_of(index: dict[tuple[str, str], bool], keys: set[tuple[str, str]] | None = None) -> dict[str, Any]:
    use = index if keys is None else {k: index[k] for k in keys if k in index}
    n = len(use)
    hits = sum(1 for v in use.values() if v)
    return _pack_rate(hits, n)


def paired_rates(det_rows: list[dict], lane_rows: list[dict]) -> dict[str, Any]:
    """Hit rates on the same article+ticker calls. Not a new universe."""
    out: dict[str, Any] = {}
    pool_det_h = pool_det_n = pool_lane_h = pool_lane_n = 0
    own_det_h = own_det_n = own_lane_h = own_lane_n = 0
    for label, *_ in HORIZONS:
        det_i = _calls(det_rows, label)
        lane_i = _calls(lane_rows, label)
        both = set(det_i) & set(lane_i)
        strict_det = _rate_of(det_i, both)
        strict_lane = _rate_of(lane_i, both)
        own_det = _rate_of(det_i)
        own_lane = _rate_of(lane_i)
        out[label] = {
            "strict": {"det": strict_det, "lane": strict_lane, "n_calls": len(both)},
            "own": {"det": own_det, "lane": own_lane},
        }
        if label in {"2d", "3d", "4d", "5d"}:
            pool_det_h += strict_det["hits"]
            pool_det_n += strict_det["n"]
            pool_lane_h += strict_lane["hits"]
            pool_lane_n += strict_lane["n"]
            own_det_h += own_det["hits"]
            own_det_n += own_det["n"]
            own_lane_h += own_lane["hits"]
            own_lane_n += own_lane["n"]
    out["2-5d"] = {
        "strict": {
            "det": _pack_rate(pool_det_h, pool_det_n),
            "lane": _pack_rate(pool_lane_h, pool_lane_n),
            "n_calls": pool_det_n,
        },
        "own": {
            "det": _pack_rate(own_det_h, own_det_n),
            "lane": _pack_rate(own_lane_h, own_lane_n),
        },
        "note": "Pooled call x horizon across 2d, 3d, 4d, and 5d. Not unique articles.",
    }
    return out


def hop_histogram(rows: list[dict]) -> dict[str, int]:
    bag: Counter[str] = Counter()
    for row in rows:
        if not is_lane_ok(row):
            continue
        bag[f"{row.get('lane')}::{row.get('model')}"] += 1
    return dict(bag.most_common())


def watermark_histogram(rows: list[dict]) -> dict[str, int]:
    bag: Counter[str] = Counter()
    for row in rows:
        token = watermark_of(row)
        if token:
            bag[token] += 1
    return dict(bag.most_common())


def lane_rippers(rows: list[dict]) -> dict[str, Any]:
    """≥5% and ≥10% on Lane-ok calls only. Same direction as the call."""
    rippers: dict[str, dict[str, list]] = {
        label: {"ge_5": [], "ge_10": []}
        for label, *_ in HORIZONS
        if label != "1-4w"
    }
    for row in rows:
        if not is_lane_ok(row):
            continue
        title = str(row.get("title") or "")[:180]
        published = str(row.get("published_at") or "")
        for g in row.get("performance") or []:
            if not isinstance(g, dict):
                continue
            if not _gradeable_call(g, row, apply_skip=False):
                continue
            direction = str(g.get("direction") or "")
            for label, ret_k, _agree_k, _skip in HORIZONS:
                if label not in rippers:
                    continue
                ret = g.get(ret_k)
                if ret is None:
                    continue
                item = {
                    "ticker": g.get("ticker"),
                    "direction": direction,
                    "ret": ret,
                    "entry_date": g.get("entry_date"),
                    "published_at": published,
                    "title": title,
                    "event_class": (row.get("classification") or {}).get("event_class"),
                    "watermark": watermark_of(row),
                }
                if _ripper(direction, ret, 10):
                    rippers[label]["ge_10"].append(item)
                elif _ripper(direction, ret, 5):
                    rippers[label]["ge_5"].append(item)
    for bag in rippers.values():
        for key in ("ge_5", "ge_10"):
            bag[key].sort(key=lambda r: -abs(float(r["ret"])))
    return rippers


def _amrx_lane(rows: list[dict]) -> dict[str, Any]:
    hits = [
        r for r in rows
        if str(r.get("ticker_hint") or "").upper() == "AMRX"
        and "lanreotide" in str(r.get("title") or "").lower()
    ]
    if not hits:
        return {"ok": False, "note": "AMRX lanreotide missing from the Lane tier"}
    row = hits[0]
    cls = row.get("classification") or {}
    perf = [
        g for g in (row.get("performance") or [])
        if isinstance(g, dict) and str(g.get("ticker") or "").upper() == "AMRX"
    ]
    g = perf[0] if perf else {}
    return {
        "ok": is_lane_ok(row) and "2026-09-18 16:01" in str(row.get("published_at") or ""),
        "lane_ok": is_lane_ok(row),
        "watermark": watermark_of(row),
        "ticker": "AMRX",
        "title": row.get("title"),
        "news_time": row.get("published_at"),
        "entry_date": g.get("entry_date"),
        "entry_clock": g.get("entry_clock") or row.get("entry_clock"),
        "event_class": cls.get("event_class"),
        "q5": cls.get("q5"),
        "sign": cls.get("sign"),
        "direction": g.get("direction"),
        "ret_1d": g.get("ret_1d"),
        "agree_1d": g.get("agree_1d"),
        "ret_2d": g.get("ret_2d"),
        "agree_2d": g.get("agree_2d"),
        "ret_3d": g.get("ret_3d"),
        "agree_3d": g.get("agree_3d"),
        "ret_4d": g.get("ret_4d"),
        "agree_4d": g.get("agree_4d"),
        "ret_5d": g.get("ret_5d"),
        "agree_5d": g.get("agree_5d"),
        "ret_20d": g.get("ret_20d"),
        "agree_20d": g.get("agree_20d"),
        "clock_rule": (
            "Friday 2026-09-18 16:01 is after the cash close. "
            "Entry is Monday 2026-09-21 open to close for 0-1d."
        ),
    }


def _stack_rows(items: list[dict]) -> list[dict]:
    out = []
    for item in items:
        reasons = item.get("reasons") or []
        if "stack_2026-09-18" not in reasons and "amrx_lanreotide" not in reasons:
            continue
        lane = item.get("lane") or {}
        det = item.get("det") or {}
        g = next(
            (
                p for p in (lane.get("performance") or [])
                if isinstance(p, dict)
                and str(p.get("ticker") or "").upper() == str(lane.get("ticker_hint") or "").upper()
            ),
            {},
        )
        out.append({
            "ticker": lane.get("ticker_hint") or det.get("ticker_hint"),
            "title": lane.get("title") or det.get("title"),
            "published_at": lane.get("published_at") or det.get("published_at"),
            "session": item.get("session"),
            "reasons": reasons,
            "watermark": watermark_of(lane),
            "lane_ok": is_lane_ok(lane),
            "event_class": (lane.get("classification") or {}).get("event_class"),
            "direction": g.get("direction"),
            "ret_1d": g.get("ret_1d"),
            "agree_1d": g.get("agree_1d"),
        })
    return out


def build_report(items: list[dict], env: dict | None, published: dict | None) -> dict[str, Any]:
    """items: {session, reasons, det, lane, attempted}."""
    det_rows = [it["det"] for it in items if it.get("det")]
    lane_rows = []
    n_ok = n_fail = n_left = n_missing = 0
    for it in items:
        lane = it.get("lane")
        if not it.get("attempted"):
            n_missing += 1
            continue
        lane_rows.append(lane or {})
        if is_lane_ok(lane):
            n_ok += 1
        else:
            n_fail += 1
            if str((lane or {}).get("lane") or "deterministic") in {"", "deterministic"}:
                n_left += 1
    n_tier = len(items)
    reason_counts = Counter()
    for it in items:
        for r in it.get("reasons") or []:
            reason_counts[r] += 1
    rates = paired_rates(det_rows, [r for r in lane_rows if r])
    ledger_lane = build_ledger([r for r in lane_rows if r])
    ledger_det = build_ledger(det_rows)
    rippers = lane_rippers([r for r in lane_rows if r])
    env = env or {}
    ship = n_missing == 0 and n_ok == n_tier and n_tier > 0
    return {
        "book": "theme_radar_elite_lane",
        "tier": "graded_0_1d_plus_converge_clash",
        "tier_note": (
            "Impulse + up/down + listed is about 9.5k unique Elite titles. "
            "Two current-flash hops on that set do not finish in one Actions "
            "runner, so Tier A is the #314 0-1d graded articles plus every "
            "converge/clash group plus AMRX lanreotide plus the 2026-09-18 "
            "SECZ/COIN/CRCL/SCHW stack. Raw 293k wraps are not rescored."
        ),
        "n_tier": n_tier,
        "n_attempted": n_tier - n_missing,
        "n_missing": n_missing,
        "n_lane_ok": n_ok,
        "n_lane_fail": n_fail,
        "n_deterministic_leftover": n_left,
        "reason_counts": dict(reason_counts),
        "ship_lane": ship,
        "env": env.get("env") or env,
        "loaded_hoppers": env.get("loaded_hoppers") or [],
        "hop_histogram": hop_histogram([r for r in lane_rows if r]),
        "watermark": watermark_histogram([r for r in lane_rows if r]),
        "paired": rates,
        "rippers": {
            label: {
                "ge_5_n": len(bag["ge_5"]),
                "ge_10_n": len(bag["ge_10"]),
                "ge_5": bag["ge_5"],
                "ge_10": bag["ge_10"],
            }
            for label, bag in rippers.items()
        },
        "amrx": _amrx_lane([r for r in lane_rows if r]),
        "stack": _stack_rows(items),
        "ledger_lane": {
            "counts": ledger_lane.get("counts"),
            "hit_rates": ledger_lane.get("hit_rates"),
            "top_converge": ledger_lane.get("top_converge"),
            "rule": ledger_lane.get("rule"),
            "scope": (
                "Ledger is the Tier A rows under Lane tags, not the full "
                "Elite singleton book. Converge/clash membership started from "
                "the #314 groups; Lane may retag a group."
            ),
        },
        "ledger_det_same_rows": {
            "counts": ledger_det.get("counts"),
            "hit_rates": ledger_det.get("hit_rates"),
        },
        "published_314": _published_slice(published),
        "clock": (
            "Entry is the next regular session after News Time. "
            "Published at/after 09:30 ET, including 09:30+30m, waits for the "
            "next RTH. 0-1d is that session's open to close. "
            "2d/3d/4d/5d are the close N sessions later. 1-4w is 20 sessions."
        ),
    }


def _published_slice(published: dict | None) -> dict[str, Any]:
    if not published:
        return {}
    funnel = published.get("funnel") or {}
    return {
        "funnel": funnel,
        "hit_rates": published.get("hit_rates") or {},
        "ledger_counts": (published.get("ledger") or {}).get("counts") or {},
        "probe": (published.get("lane") or {}).get("probe") or {},
        "note": (
            "Full Elite book from #314. Not the paired Tier A universe. "
            "That run's probe can read keys_present while attempted stays 0 "
            "because the push path did not pass --lane."
        ),
    }


def _ripper_lines(rippers: dict, limit: int = 12) -> list[str]:
    lines = [
        "Lane-ok calls only. Same direction as the call. "
        "5% rows are ≥5% and <10%. 10% rows are ≥10%.",
        "",
    ]
    for label in ("0-1d", "2d", "3d", "4d", "5d"):
        bag = rippers.get(label) or {}
        g5 = bag.get("ge_5") or []
        g10 = bag.get("ge_10") or []
        lines += [
            f"### {label}",
            "",
            f"- ≥5% and <10%: **{len(g5)}**",
            f"- ≥10%: **{len(g10)}**",
            "",
            "| threshold | ticker | date | ret | class | watermark | title |",
            "| --- | --- | --- | ---: | --- | --- | --- |",
        ]
        shown = 0
        for thresh, rows in (("≥10%", g10), ("≥5%", g5)):
            for item in rows:
                if shown >= limit:
                    break
                title = str(item.get("title") or "").replace("|", "/")
                lines.append(
                    f"| {thresh} | {item.get('ticker')} | "
                    f"{item.get('entry_date') or ''} | {_fmt_ret(item.get('ret'))} | "
                    f"{item.get('event_class') or ''} | `{item.get('watermark') or ''}` | "
                    f"{title[:90]} |"
                )
                shown += 1
            if shown >= limit:
                break
        extra = len(g5) + len(g10) - shown
        if not g5 and not g10:
            lines.append("| — | — | — | — | — | — | none |")
        elif extra > 0:
            lines.append(f"| … | | | | | | {extra} more in the JSON |")
        lines.append("")
    return lines


def markdown_report(report: dict[str, Any]) -> str:
    paired = report.get("paired") or {}
    p01 = (paired.get("0-1d") or {}).get("strict") or {}
    p25 = (paired.get("2-5d") or {}).get("strict") or {}
    det01 = (p01.get("det") or {})
    lane01 = (p01.get("lane") or {})
    det25 = (p25.get("det") or {})
    lane25 = (p25.get("lane") or {})
    amrx = report.get("amrx") or {}
    lines = [
        "# Theme-radar Elite Lane backtest",
        "",
        f"**Tier A n = {report.get('n_tier')} articles. "
        f"Lane-ok {report.get('n_lane_ok')} / "
        f"Lane-fail {report.get('n_lane_fail')} / "
        f"deterministic leftover {report.get('n_deterministic_leftover')} / "
        f"missing {report.get('n_missing')}.**",
        "",
        f"**0-1d paired Lane {lane01.get('text') or 'n/a'} vs deterministic "
        f"{det01.get('text') or 'n/a'} on the same calls "
        f"(n={p01.get('n_calls') or 0}).**",
        "",
        f"**2-5d pooled paired Lane {lane25.get('text') or 'n/a'} vs deterministic "
        f"{det25.get('text') or 'n/a'} (n={p25.get('n_calls') or 0}).**",
        "",
        report.get("tier_note") or "",
        "",
        "Watermark on every attempted row is `lane::model::inference_source`. "
        "A deterministic watermark is a leftover, not a Lane call. "
        "theme-radar is read-only. The repos are not merged.",
        "",
        "## Env (redacted)",
        "",
        "| name | status |",
        "| --- | --- |",
    ]
    env = report.get("env") or {}
    if not env:
        lines.append("| — | no shard recorded an env check |")
    for name, status in env.items():
        if name.startswith("_"):
            continue
        lines.append(f"| {name} | {status} |")
    loaded = ", ".join(report.get("loaded_hoppers") or []) or "(none)"
    lines += [
        "",
        f"Loaded current-flash hoppers: **{loaded}**.",
        "",
        "## Tier",
        "",
        "| reason | articles |",
        "| --- | ---: |",
    ]
    reasons = report.get("reason_counts") or {}
    if not reasons:
        lines.append("| — | 0 |")
    for key, n in sorted(reasons.items()):
        lines.append(f"| {key} | {n} |")
    lines += [
        "",
        f"Exact Tier A n = **{report.get('n_tier')}**.",
        "",
        "| | n |",
        "| --- | ---: |",
        f"| Lane-ok | {report.get('n_lane_ok')} |",
        f"| Lane-fail | {report.get('n_lane_fail')} |",
        f"| deterministic leftover | {report.get('n_deterministic_leftover')} |",
        f"| missing (not attempted) | {report.get('n_missing')} |",
        "",
        "## Hop histogram (provider::model)",
        "",
        "Lane-ok rows only. New calls are strict free tier: "
        "Zhipu glm-4.7-flash, SiliconFlow THUDM/GLM-Z1-9B-0414 and "
        "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B, OpenRouter :free. "
        "DashScope qwen-flash is not called (free grant exhausted). "
        "A 402 tries the next free id on that provider. "
        "A 403 or 429 abandons the provider. "
        "DeepSeek, TokenHub, Gemini, Qwen/Qwen3-8B, and deepseek-chat "
        "are not called.",
        "",
    ]
    hist = report.get("hop_histogram") or {}
    if not hist:
        lines.append("- (no live Lane hop)")
    for key, n in hist.items():
        lines.append(f"- `{key}`: {n}")
    lines += [
        "",
        "## Paired rubric (same rows)",
        "",
        "Strict columns use article+ticker calls that are gradeable on both "
        "the #314 deterministic tag and the Lane tag. Own columns let the "
        "denominator move when Lane changes class or direction. "
        "This is the Tier A subset, not the full Elite book.",
        "",
        "| horizon | det strict | lane strict | n | det own | lane own |",
        "| --- | --- | --- | ---: | --- | --- |",
    ]
    order = [label for label, *_ in HORIZONS] + ["2-5d"]
    for label in order:
        slot = paired.get(label) or {}
        strict = slot.get("strict") or {}
        own = slot.get("own") or {}
        lines.append(
            f"| {label} | {(strict.get('det') or {}).get('text') or 'n/a'} | "
            f"{(strict.get('lane') or {}).get('text') or 'n/a'} | "
            f"{strict.get('n_calls') or 0} | "
            f"{(own.get('det') or {}).get('text') or 'n/a'} | "
            f"{(own.get('lane') or {}).get('text') or 'n/a'} |"
        )
    pub = report.get("published_314") or {}
    pub_rates = pub.get("hit_rates") or {}
    pub_01 = pub_rates.get("0-1d") or {}
    lines += [
        "",
        "### Published #314 full book (not this universe)",
        "",
        pub.get("note") or "",
        "",
        f"- full-book 0-1d rubric: {pub_01.get('hits', 'n/a')}/{pub_01.get('n_calls', 'n/a')}",
        f"- impulse + up/down + listed: {(pub.get('funnel') or {}).get('impulse_updown_listed', 'n/a')}",
        f"- graded articles 0-1d: {((pub.get('funnel') or {}).get('graded_articles') or {}).get('0-1d', 'n/a')}",
        f"- ledger counts: `{json.dumps(pub.get('ledger_counts') or {}, sort_keys=True)}`",
        "",
        "## Rippers (Lane calls only)",
        "",
    ]
    lines += _ripper_lines(report.get("rippers") or {})
    lines += [
        "## AMRX",
        "",
        "| field | value |",
        "| --- | --- |",
        f"| watermark | `{amrx.get('watermark') or ''}` |",
        f"| lane_ok | {amrx.get('lane_ok')} |",
        f"| title | {amrx.get('title') or ''} |",
        f"| News Time | {amrx.get('news_time') or ''} |",
        f"| entry_date | {amrx.get('entry_date') or ''} |",
        f"| class | {amrx.get('event_class')} / q5={amrx.get('q5')} / sign={amrx.get('sign')} |",
        f"| direction | {amrx.get('direction')} |",
        f"| 0-1d | {_fmt_ret(amrx.get('ret_1d'))} agree={amrx.get('agree_1d')} |",
        f"| 2d | {_fmt_ret(amrx.get('ret_2d'))} agree={amrx.get('agree_2d')} |",
        f"| 5d | {_fmt_ret(amrx.get('ret_5d'))} agree={amrx.get('agree_5d')} |",
        f"| 1-4w | {_fmt_ret(amrx.get('ret_20d'))} agree={amrx.get('agree_20d')} |",
        "",
        amrx.get("clock_rule") or "",
        "",
        "## 2026-09-18 stack",
        "",
        "SECZ / COIN / CRCL / SCHW rows whose session or News Time is "
        "2026-09-18, plus the AMRX lanreotide row. Watermark is Lane when the hop landed.",
        "",
        "| ticker | session | lane_ok | watermark | direction | 0-1d | title |",
        "| --- | --- | --- | --- | --- | ---: | --- |",
    ]
    stack = report.get("stack") or []
    if not stack:
        lines.append("| — | — | — | — | — | — | none |")
    for row in stack:
        title = str(row.get("title") or "").replace("|", "/")
        lines.append(
            f"| {row.get('ticker')} | {row.get('session')} | {row.get('lane_ok')} | "
            f"`{row.get('watermark') or ''}` | {row.get('direction') or ''} | "
            f"{_fmt_ret(row.get('ret_1d'))} | {title[:90]} |"
        )
    lines += [
        "",
        "## Converge / singleton / clash under Lane tags",
        "",
        (report.get("ledger_lane") or {}).get("scope") or "",
        "",
        (report.get("ledger_lane") or {}).get("rule") or "",
        "",
        "| bucket | Lane groups | det groups (same rows) | Lane 0-1d | det 0-1d | Lane 5d | det 5d |",
        "| --- | ---: | ---: | --- | --- | --- | --- |",
    ]
    lc = (report.get("ledger_lane") or {}).get("counts") or {}
    dc = (report.get("ledger_det_same_rows") or {}).get("counts") or {}
    lr = (report.get("ledger_lane") or {}).get("hit_rates") or {}
    dr = (report.get("ledger_det_same_rows") or {}).get("hit_rates") or {}
    for bucket in ("singleton", "converge", "clash"):
        def _cell(bag: dict, hz: str) -> str:
            slot = (bag.get(bucket) or {}).get(hz) or {}
            return _pct(slot.get("hits") or 0, slot.get("n") or 0)
        lines.append(
            f"| {bucket} | {lc.get(bucket, 0)} | {dc.get(bucket, 0)} | "
            f"{_cell(lr, '0-1d')} | {_cell(dr, '0-1d')} | "
            f"{_cell(lr, '5d')} | {_cell(dr, '5d')} |"
        )
    lines += [
        "",
        "### Top Lane converge names",
        "",
        "| ticker | date | n_bull | n_bear | net | 0-1d | 5d |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    top = (report.get("ledger_lane") or {}).get("top_converge") or []
    if not top:
        lines.append("| — | — | — | — | — | — | — |")
    for row in top[:15]:
        lines.append(
            f"| {row.get('ticker')} | {row.get('date')} | {row.get('n_bull')} | "
            f"{row.get('n_bear')} | {row.get('net')} | {_fmt_ret(row.get('ret_1d'))} | "
            f"{_fmt_ret(row.get('ret_5d'))} |"
        )
    lines += [
        "",
        "## Clock",
        "",
        report.get("clock") or "",
        "",
        "## Hopper watermark (lane::model::inference_source)",
        "",
    ]
    wm = report.get("watermark") or {}
    if not wm:
        lines.append("- (none)")
    for key, n in wm.items():
        lines.append(f"- `{key}`: {n}")
    lines += [
        "",
        f"ship_lane = **{report.get('ship_lane')}** "
        "(true only when every Tier A row has a live current-flash watermark).",
        "",
    ]
    return "\n".join(lines)


def _slim_perf(g: dict) -> dict:
    keep = {
        "ticker", "direction", "event_class", "q5", "tradeable_expression",
        "role", "kind", "horizon", "skip_01d", "graded", "ungraded_reason",
        "note", "entry_clock",
    } | set(TAPE_KEYS) | {a for _r, a in AGREE_KEYS}
    return {k: g.get(k) for k in keep if k in g}


def slim_row(row: dict, *, keep_body: bool = False) -> dict:
    out = {
        "article_id": row.get("article_id"),
        "title": row.get("title"),
        "published_at": row.get("published_at"),
        "retrieved_at": row.get("retrieved_at"),
        "known_at": row.get("known_at"),
        "ticker_hint": row.get("ticker_hint"),
        "harvest_source": row.get("harvest_source"),
        "source": row.get("source"),
        "source_file": row.get("source_file"),
        "sectors": row.get("sectors") or [],
        "macro_themes": row.get("macro_themes") or [],
        "macro_factor": row.get("macro_factor") or "",
        "entry_clock": row.get("entry_clock"),
        "classification": row.get("classification") or {},
        "entities": row.get("entities") or [],
        "lane": row.get("lane"),
        "model": row.get("model"),
        "inference_source": row.get("inference_source"),
        "performance": [
            _slim_perf(g) for g in (row.get("performance") or []) if isinstance(g, dict)
        ],
        "hop_chain": [
            {
                "role": h.get("role"),
                "lane": h.get("lane"),
                "model": h.get("model"),
                "ok": h.get("ok"),
                "skip": h.get("skip"),
            }
            for h in (row.get("hop_chain") or [])
            if isinstance(h, dict)
        ],
    }
    if keep_body:
        out["body"] = row.get("body") or ""
    return out


def retag_tape(det: dict, lane: dict) -> tuple[dict, list[str]]:
    """Keep the #314 returns for tickers both sides name. Recompute agree.

    New Lane tickers are returned as missing so the caller can fetch them.
    """
    by_tick: dict[str, dict] = {}
    for g in det.get("performance") or []:
        if isinstance(g, dict) and g.get("ticker"):
            by_tick.setdefault(str(g["ticker"]).upper(), g)
    kept: list[dict] = []
    missing: list[str] = []
    for target in evaluation_targets(lane):
        tick = str(target.get("ticker") or "").upper()
        src = by_tick.get(tick)
        has_tape = bool(src) and any(src.get(k) is not None for k in (
            "ret_1d", "ret_2d", "ret_5d", "ret_20d",
        ))
        if not has_tape:
            if tick:
                missing.append(tick)
            continue
        g = {k: src.get(k) for k in TAPE_KEYS if k in src}
        g.update({
            "ticker": tick,
            "direction": target.get("direction") or "not_determined",
            "event_class": target.get("event_class") or "",
            "q5": target.get("q5") or "",
            "tradeable_expression": target.get("tradeable_expression") or "direct",
            "role": target.get("role") or "named",
            "kind": target.get("kind") or "ticker",
            "horizon": target.get("horizon") or "0-1d",
        })
        for ret_k, agree_k in AGREE_KEYS:
            if ret_k in g or ret_k in src:
                g[ret_k] = src.get(ret_k)
                g[agree_k] = _agree(str(g.get("direction") or ""), g.get(ret_k))
        g["skip_01d"] = skips_01d_horizon(g, lane)
        reason = ungraded_reason(g, lane)
        g["graded"] = reason is None
        g["ungraded_reason"] = reason or ""
        kept.append(g)
    out = dict(lane)
    out["performance"] = kept
    return out, missing


def attach_tape(det: dict, lane: dict, fetch: bool = True) -> dict:
    retagged, missing = retag_tape(det, lane)
    if missing and fetch:
        fetched = grade_results([lane], fetch=True)
        if fetched:
            got = fetched[0]
            got["article_id"] = det.get("article_id") or got.get("article_id")
            return got
    retagged["article_id"] = det.get("article_id") or retagged.get("article_id")
    return retagged


def _art_from_row(row: dict) -> dict:
    return {
        "title": row.get("title") or "",
        "body": row.get("body") or "",
        "published_at": row.get("published_at") or "",
        "retrieved_at": row.get("retrieved_at") or "",
        "known_at": row.get("known_at") or row.get("published_at") or "",
        "ticker_hint": row.get("ticker_hint") or "",
        "harvest_source": row.get("harvest_source") or "theme_radar_elite",
        "source": row.get("source") or "theme_radar_elite",
        "source_file": row.get("source_file") or "",
        "sectors": row.get("sectors") or [],
        "macro_themes": row.get("macro_themes") or [],
    }


def prepare_manifest(date: str = "all", fetch: bool = True) -> dict[str, Any]:
    """Deterministic route of unique Elite titles, then cut Tier A. No Lane."""
    from src.news_impact.pipeline import analyze_article
    from src.news_impact.theme_radar import dedupe_elite, load_theme_radar

    label = "all" if str(date).lower() in {"all", "*", "history", ""} else str(date)
    raw = load_theme_radar(None if label == "all" else label, allow_remote=False)
    raw = [a for a in raw if str(a.get("title") or "").strip()]
    unique = dedupe_elite(raw)
    print(f"[lane-tier] routing {len(unique)} unique (raw {len(raw)})", flush=True)
    results: list[dict] = []
    arts_by_id: dict[str, dict] = {}
    for i, art in enumerate(unique, 1):
        row = analyze_article(art, use_lane=False, use_search=False, persist=False)
        results.append(row)
        arts_by_id[str(row.get("article_id") or "")] = art
        if i % 5000 == 0 or i == len(unique):
            print(f"[lane-tier] routed {i}/{len(unique)}", flush=True)
    clash_ids = clash_converge_ids(results)
    need = [r for r in results if is_signed_listed(r)]
    print(f"[lane-tier] grading {len(need)} signed listed", flush=True)
    graded = grade_results(need, fetch=fetch) if need else []
    graded_by = {str(r.get("article_id") or ""): r for r in graded}
    merged = []
    for row in results:
        taped = graded_by.get(str(row.get("article_id") or ""))
        merged.append(taped if taped is not None else row)
    graded_ids = {str(r.get("article_id") or "") for r in merged if is_01d_graded(r)}
    graded_ids.discard("")
    picked = select_tier(merged, graded_ids, clash_ids)
    if label != "all":
        picked = [
            (row, reasons) for row, reasons in picked
            if session_of(row) == label or str(row.get("published_at") or "").startswith(label)
        ]
    # Grade forced rows that were not in the signed-listed pass.
    extra = [
        row for row, _reasons in picked
        if str(row.get("article_id") or "") not in graded_by and not row.get("performance")
    ]
    if extra and fetch:
        print(f"[lane-tier] grading {len(extra)} forced extras", flush=True)
        extra_g = grade_results(extra, fetch=True)
        extra_by = {str(r.get("article_id") or ""): r for r in extra_g}
        picked = [
            (extra_by.get(str(row.get("article_id") or ""), row), reasons)
            for row, reasons in picked
        ]
    by_date: dict[str, list] = {}
    reason_counts: Counter[str] = Counter()
    n_calls = 0
    for row, reasons in picked:
        for r in reasons:
            reason_counts[r] += 1
        n_calls += sum(
            1 for g in (row.get("performance") or [])
            if isinstance(g, dict) and _gradeable_call(g, row, apply_skip=True) and g.get("ret_1d") is not None
        )
        session = session_of(row) or str(row.get("published_at") or "")[:10] or "undated"
        art = arts_by_id.get(str(row.get("article_id") or "")) or _art_from_row(row)
        by_date.setdefault(session, []).append({
            "session": session,
            "reasons": reasons,
            "art": _art_from_row({**art, **{
                "title": row.get("title") or art.get("title"),
                "published_at": row.get("published_at") or art.get("published_at"),
                "known_at": row.get("known_at") or art.get("known_at"),
                "ticker_hint": row.get("ticker_hint") or art.get("ticker_hint"),
            }}),
            "det": slim_row(row, keep_body=False),
        })
    dates = sorted(by_date)
    manifest = {
        "tier": "graded_0_1d_plus_converge_clash",
        "date_filter": label,
        "n_raw": len(raw),
        "n_unique": len(unique),
        "n_impulse_updown_listed": sum(1 for r in merged if is_signed_listed(r)),
        "n_graded_0_1d_articles": len(graded_ids),
        "n_converge_clash_articles": len(clash_ids),
        "n_tier": len(picked),
        "n_01d_calls_in_tier": n_calls,
        "reason_counts": dict(reason_counts),
        "dates": dates,
        "by_date": by_date,
        "why_not_full_impulse": (
            f"{sum(1 for r in merged if is_signed_listed(r))} impulse+listed titles. "
            "Tier A hops the 0-1d graded set plus converge/clash plus the named cases."
        ),
    }
    _require_named_cases(manifest, label)
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False) + "\n", encoding="utf-8")
    DATES.write_text(json.dumps(dates) + "\n", encoding="utf-8")
    print(
        f"[lane-tier] n={manifest['n_tier']} dates={len(dates)} "
        f"graded_articles={manifest['n_graded_0_1d_articles']} "
        f"reasons={manifest['reason_counts']}",
        flush=True,
    )
    return manifest


def _require_named_cases(manifest: dict, label: str) -> None:
    if label != "all":
        return
    reasons = manifest.get("reason_counts") or {}
    if not reasons.get("amrx_lanreotide"):
        raise SystemExit("AMRX lanreotide missing from Tier A")
    if not reasons.get("stack_2026-09-18"):
        raise SystemExit("2026-09-18 SECZ/COIN/CRCL/SCHW stack missing from Tier A")
    ticks = set()
    for rows in (manifest.get("by_date") or {}).values():
        for item in rows:
            if "stack_2026-09-18" not in (item.get("reasons") or []):
                continue
            det = item.get("det") or {}
            ticks.add(str(det.get("ticker_hint") or "").upper())
    missing = sorted(STACK_TICKERS - ticks)
    if missing:
        raise SystemExit(f"stack tickers missing from Tier A: {', '.join(missing)}")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def hop_shard(date: str, manifest_path: Path, out_path: Path, fetch: bool = True) -> dict[str, Any]:
    """Hop one session. 429 leaves the provider inside Lane. Checkpoint as we go."""
    from src import lane_route as lane
    from src.news_impact.pipeline import analyze_article

    info = require_lane_env(strict=True)
    lane._SKIP.clear()
    manifest = _load_json(manifest_path)
    rows = list((manifest.get("by_date") or {}).get(date) or [])
    done: dict[str, dict] = {}
    if out_path.exists():
        try:
            prev = _load_json(out_path)
        except json.JSONDecodeError:
            prev = {}
        for item in prev.get("rows") or []:
            det = (item.get("det") or {})
            hopped = item.get("lane") or {}
            aid = str(det.get("article_id") or hopped.get("article_id") or "")
            if aid and is_lane_ok(hopped):
                done[aid] = item
    out_rows: list[dict] = []
    n = len(rows)
    print(f"[lane-hop] {date} n={n} resume_ok={len(done)}", flush=True)
    for i, item in enumerate(rows, 1):
        det = item.get("det") or {}
        aid = str(det.get("article_id") or "")
        if aid in done:
            out_rows.append(done[aid])
            continue
        art = item.get("art") or _art_from_row(det)
        try:
            hopped = analyze_article(art, use_lane=True, use_search=False, persist=False)
        except Exception as exc:  # noqa: BLE001
            hopped = dict(det)
            hopped["lane"] = "deterministic"
            hopped["model"] = det.get("model") or "news_impact_v2"
            hopped["inference_source"] = "deterministic"
            hopped["lane_error"] = str(exc)[:200]
        hopped["article_id"] = aid or hopped.get("article_id")
        hopped = attach_tape(det, hopped, fetch=fetch)
        packed = {
            "session": item.get("session") or date,
            "reasons": item.get("reasons") or [],
            "det": det,
            "lane": slim_row(hopped, keep_body=not is_lane_ok(hopped)),
            "attempted": True,
        }
        out_rows.append(packed)
        token = watermark_of(packed["lane"])
        print(f"[lane-hop] {date} {i}/{n} {token}", flush=True)
        if i % 15 == 0 or i == n:
            _write_shard(out_path, date, info, out_rows, n)
    shard = _write_shard(out_path, date, info, out_rows, n)
    print(
        f"[lane-hop] {date} lane_ok={shard['n_lane_ok']} "
        f"fail={shard['n_lane_fail']} of {shard['n']}",
        flush=True,
    )
    if shard["n"] and shard["n_lane_ok"] < shard["n"]:
        raise SystemExit(1)
    return shard


def _write_shard(path: Path, date: str, info: dict, rows: list[dict], n_expected: int) -> dict:
    n_ok = sum(1 for r in rows if is_lane_ok(r.get("lane")))
    shard = {
        "date": date,
        "env": {
            "env": info.get("env") or {},
            "loaded_hoppers": info.get("loaded_hoppers") or [],
            "ready": info.get("ready"),
            "bug": info.get("bug"),
        },
        "n_expected": n_expected,
        "n": len(rows),
        "n_lane_ok": n_ok,
        "n_lane_fail": len(rows) - n_ok,
        "rows": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(shard, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)
    return shard


def _iter_shards(folder: Path) -> list[dict]:
    out = []
    if not folder.exists():
        return out
    for path in sorted(folder.rglob("*.json")):
        if path.name.endswith(".tmp"):
            continue
        try:
            blob = _load_json(path)
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(blob, dict) and "rows" in blob:
            out.append(blob)
    return out


def merge_shards(
    manifest_path: Path,
    shard_dir: Path,
    md_path: Path = SCOREBOARD,
    json_path: Path = BOARD_JSON,
) -> dict[str, Any]:
    manifest = _load_json(manifest_path)
    by_id: dict[str, dict] = {}
    env = None
    for shard in _iter_shards(shard_dir):
        if env is None and shard.get("env"):
            env = shard["env"]
        for item in shard.get("rows") or []:
            det = item.get("det") or {}
            aid = str(det.get("article_id") or (item.get("lane") or {}).get("article_id") or "")
            if not aid:
                continue
            prev = by_id.get(aid)
            if prev is None or (is_lane_ok((item.get("lane"))) and not is_lane_ok(prev.get("lane"))):
                by_id[aid] = item
    items = []
    for rows in (manifest.get("by_date") or {}).values():
        for src in rows:
            det = src.get("det") or {}
            aid = str(det.get("article_id") or "")
            got = by_id.get(aid)
            if got is None:
                items.append({
                    "session": src.get("session"),
                    "reasons": src.get("reasons") or [],
                    "det": det,
                    "lane": None,
                    "attempted": False,
                })
            else:
                items.append({
                    "session": got.get("session") or src.get("session"),
                    "reasons": got.get("reasons") or src.get("reasons") or [],
                    "det": got.get("det") or det,
                    "lane": got.get("lane"),
                    "attempted": True,
                })
    published = {}
    if PUBLISHED_314.exists():
        try:
            published = _load_json(PUBLISHED_314)
        except json.JSONDecodeError:
            published = {}
    report = build_report(items, env, published)
    report["n_impulse_updown_listed"] = manifest.get("n_impulse_updown_listed")
    report["n_unique"] = manifest.get("n_unique")
    report["n_raw"] = manifest.get("n_raw")
    report["n_graded_0_1d_articles_book"] = manifest.get("n_graded_0_1d_articles")
    report["why_not_full_impulse"] = manifest.get("why_not_full_impulse")
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown_report(report), encoding="utf-8")
    json_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        f"[lane-merge] tier={report['n_tier']} lane_ok={report['n_lane_ok']} "
        f"fail={report['n_lane_fail']} leftover={report['n_deterministic_leftover']} "
        f"missing={report['n_missing']} ship={report['ship_lane']} → {md_path}",
        flush=True,
    )
    return report
