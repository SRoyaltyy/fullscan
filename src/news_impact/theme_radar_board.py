"""Lane + theme-radar Elite scoreboard. Research-only.

Reads slim Elite headlines (never theme-radar .raw.csv, never a repo merge).
Current rubric: Q5 → one event_class → that family's winners/losers,
listed expression, reaction-in-title kill, FOMC collapse, no scores.

Grade clock: next RTH after News Time. Published at/after 09:30 ET
(the 09:30+30m window included) is not eligible for that cash session.
Friday 16:01 → Monday open→close.

Horizons from that entry open: 0-1d (same-session close), 2d/3d/4d/5d
(close N sessions later), 1-4w secondary (20 sessions).
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .grade import grade_results, skips_01d_horizon, ungraded_reason
from .hygiene import collapse_macro_stories, is_reaction_title
from .ledger import build_ledger, clash_converge_ids
from .pipeline import analyze_article
from .theme_radar import dedupe_elite, load_theme_radar
from .unique_title import is_signed_listed

SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_THEME_RADAR.md")
RATES = Path("01_daily/news/all_news_impact_theme_radar.json")

# (label, return key, agree key, apply the short-window skip)
HORIZONS = (
    ("0-1d", "ret_1d", "agree_1d", True),
    ("2d", "ret_2d", "agree_2d", True),
    ("3d", "ret_3d", "agree_3d", True),
    ("4d", "ret_4d", "agree_4d", True),
    ("5d", "ret_5d", "agree_5d", True),
    ("1-4w", "ret_20d", "agree_20d", False),
)
SHORT = {"0-1d", "2d", "3d", "4d", "5d"}


def _pct(h: int, n: int) -> str:
    if not n:
        return "n/a"
    return f"{h}/{n} = {h / n:.1%}"


def _weather(row: dict) -> bool:
    cls = row.get("classification") or {}
    q5 = str(cls.get("q5") or "")
    ev = str(cls.get("event_class") or "")
    return q5 == "regime" or ev in {"discard", "regime_state", "rumor"}


def _gradeable_call(g: dict, row: dict, apply_skip: bool) -> bool:
    if ungraded_reason(g, row) is not None:
        return False
    if apply_skip and (g.get("skip_01d") or skips_01d_horizon(g, row)):
        return False
    return True


def _lane_live(row: dict) -> bool:
    lane = str(row.get("lane") or "")
    return bool(lane) and lane != "deterministic"


def lane_available() -> dict[str, Any]:
    """Current-flash keys present? No network. 429 is detected on the hop.

    GITHUB_TOKEN alone is not a Lane provider. Actions always injects it.
    """
    try:
        from src.news_impact.lane_env import readiness
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "quota_dead": True, "reason": f"import:{exc}"}
    info = readiness()
    if info.get("bug"):
        return {
            "ok": False,
            "quota_dead": True,
            "reason": "env_mapping_bug",
            "missed_hoppers": info.get("missed_hoppers") or [],
        }
    if not info.get("ready"):
        return {"ok": False, "quota_dead": True, "reason": "no_keys"}
    return {"ok": True, "quota_dead": False, "reason": "keys_present"}


def _hop_elite_subset(unique: list[dict], results: list[dict]) -> tuple[list[dict], dict]:
    """Deterministic roles first. Lane current-flash only on clash/converge groups.

    A dead hop leaves that provider (Lane's own 429 rule) and the next article
    still runs. This clash-only pass is not the Tier A Lane scoreboard.
    """
    probed = lane_available()
    meta = {
        "attempted": 0,
        "live": 0,
        "fail": 0,
        "stopped": False,
        "probe": probed,
        "scope": "clash_converge",
    }
    if not probed.get("ok"):
        return results, meta
    wanted = clash_converge_ids(results)
    out = list(results)
    for i, (art, row) in enumerate(zip(unique, results)):
        if str(row.get("article_id") or "") not in wanted:
            continue
        meta["attempted"] += 1
        hopped = analyze_article(art, use_lane=True, use_search=False, persist=False)
        if _lane_live(hopped):
            out[i] = hopped
            meta["live"] += 1
        else:
            meta["fail"] = int(meta.get("fail") or 0) + 1
    return out, meta


def _path_text(use_lane: bool, hop: dict) -> str:
    probe = hop.get("probe") or {}
    if not use_lane:
        return (
            "deterministic router on every unique Elite title. "
            "Indirect roles are the family template "
            "(substitute / stays_out / arms_dealer / peer / sector basket), "
            "not a Lane tag. Lane was not requested on this run, so this file "
            "is not a Lane scoreboard even when provider secrets are present. "
            "The Lane-on-Elite test writes "
            "03_scoreboard/NEWS_IMPACT_THEME_RADAR_LANE.md."
        )
    if probe.get("reason") in {"no_keys", "env_mapping_bug"}:
        return (
            "Lane was requested and the current-flash provider check failed "
            f"(reason={probe.get('reason')}). "
            "Grades stay on the deterministic router. This is not a Lane result. "
            "GITHUB_TOKEN alone does not count as a hopper."
        )
    if hop.get("live"):
        return (
            "deterministic roles on the full Elite book, then Lane current-flash "
            f"on clash/converge groups only ({hop.get('live')} live, "
            f"{hop.get('fail', 0)} failed, {hop.get('attempted')} attempted). "
            "429 leaves that provider and the next current hopper is tried. "
            "This clash-only pass is not the Tier A Lane scoreboard."
        )
    if hop.get("stopped") or probe.get("quota_dead"):
        return (
            "deterministic roles first. Lane on clash/converge groups was "
            "attempted and did not return a live current-flash model "
            f"(reason={probe.get('reason') or 'empty/429'}). "
            "Provider left on 429. Grades stay on the deterministic router. "
            "This is not a Lane result."
        )
    return (
        "deterministic router on every unique Elite title. "
        "Lane was not applied. Indirect roles are the family template."
    )


def _watermark(results: list[dict]) -> dict[str, int]:
    bag: Counter[str] = Counter()
    for row in results:
        lane = str(row.get("lane") or "deterministic")
        model = str(row.get("model") or "news_impact_v2")
        src = str(row.get("harvest_source") or "theme_radar_elite")
        bag[f"{lane}::{model}::{src}"] += 1
    return dict(bag.most_common())


def _amrx_case(results: list[dict]) -> dict[str, Any]:
    hits = [
        r for r in results
        if str(r.get("ticker_hint") or "").upper() == "AMRX"
        and "lanreotide" in str(r.get("title") or "").lower()
    ]
    if not hits:
        return {"ok": False, "note": "AMRX lanreotide row missing from the Elite book"}
    row = hits[0]
    cls = row.get("classification") or {}
    perf = []
    for g in row.get("performance") or []:
        if not isinstance(g, dict):
            continue
        if str(g.get("ticker") or "").upper() != "AMRX":
            continue
        perf.append(g)
    g = perf[0] if perf else {}
    return {
        "ok": "2026-09-18 16:01" in str(row.get("published_at") or ""),
        "ticker": "AMRX",
        "title": row.get("title"),
        "news_time": row.get("published_at"),
        "harvest_source": row.get("harvest_source"),
        "entry_clock": g.get("entry_clock") or row.get("entry_clock"),
        "entry_date": g.get("entry_date"),
        "entry_open": g.get("entry_open"),
        "event_class": cls.get("event_class"),
        "q5": cls.get("q5"),
        "sign": cls.get("sign"),
        "direction": g.get("direction"),
        "horizon": g.get("horizon"),
        "skip_01d": g.get("skip_01d"),
        "graded": g.get("graded"),
        "ungraded_reason": g.get("ungraded_reason") or "",
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
        "note": g.get("note") or "",
        "clock_rule": (
            "Friday 2026-09-18 16:01 is after the cash close. "
            "Entry is Monday 2026-09-21 open→close for 0-1d, never Friday cash. "
            "Published at/after 09:30 ET (09:30+30m included) waits for the next RTH."
        ),
    }


def _collect(results: list[dict]) -> dict[str, Any]:
    """Hit rates, per-horizon graded article counts, rippers."""
    rubric = {
        label: {"hits": 0, "n": 0, "articles": set()}
        for label, *_ in HORIZONS
    }
    broad = {
        label: {"hits": 0, "n": 0, "articles": set()}
        for label, *_ in HORIZONS
        if label in SHORT
    }
    rippers: dict[str, dict[str, list]] = {
        label: {"ge_5": [], "ge_10": []} for label, *_ in HORIZONS if label != "1-4w"
    }
    for row in results:
        title = str(row.get("title") or "")[:180]
        published = str(row.get("published_at") or "")
        for g in row.get("performance") or []:
            if not isinstance(g, dict):
                continue
            for label, ret_k, agree_k, apply_skip in HORIZONS:
                ret = g.get(ret_k)
                if ret is None:
                    continue
                if not _gradeable_call(g, row, apply_skip=False):
                    continue
                aid = str(row.get("article_id") or title)
                if label in broad:
                    broad[label]["n"] += 1
                    broad[label]["hits"] += int(g.get(agree_k) is True)
                    broad[label]["articles"].add(aid)
                    if label in rippers:
                        direction = str(g.get("direction") or "")
                        item = {
                            "ticker": g.get("ticker"),
                            "direction": direction,
                            "ret": ret,
                            "entry_date": g.get("entry_date"),
                            "published_at": published,
                            "title": title,
                            "event_class": (row.get("classification") or {}).get("event_class"),
                        }
                        if _ripper(direction, ret, 10):
                            rippers[label]["ge_10"].append(item)
                        elif _ripper(direction, ret, 5):
                            rippers[label]["ge_5"].append(item)
                if not _gradeable_call(g, row, apply_skip=apply_skip):
                    continue
                rubric[label]["n"] += 1
                rubric[label]["hits"] += int(g.get(agree_k) is True)
                rubric[label]["articles"].add(aid)
    def _pack(bag: dict) -> dict:
        out = {}
        for label, slot in bag.items():
            hits, n = slot["hits"], slot["n"]
            out[label] = {
                "hits": hits,
                "n_calls": n,
                "hit_rate": round(hits / n, 4) if n else None,
                "graded_articles": len(slot["articles"]),
            }
        return out
    for label, slots in rippers.items():
        for key in ("ge_5", "ge_10"):
            slots[key].sort(key=lambda r: -abs(float(r["ret"])))
    return {"rubric": _pack(rubric), "broad_short": _pack(broad), "rippers": rippers}


def _ripper(direction: str, ret: float, thresh: float) -> bool:
    if direction == "up":
        return float(ret) >= thresh
    if direction == "down":
        return float(ret) <= -thresh
    return False


def build_payload(
    raw: list[dict],
    unique: list[dict],
    results: list[dict],
    *,
    use_lane: bool,
    hop: dict,
) -> dict[str, Any]:
    non_weather = 0
    reaction = 0
    impulse = 0
    for row in results:
        if is_reaction_title(str(row.get("title") or "")):
            reaction += 1
        if not _weather(row):
            non_weather += 1
        if is_signed_listed(row):
            impulse += 1
    stats = _collect(results)
    mix = Counter(str(a.get("harvest_source") or "?") for a in raw)
    unique_mix = Counter(str(r.get("harvest_source") or "?") for r in results)
    macro = collapse_macro_stories(results)
    ledger = build_ledger(results)
    funnel = {
        "elite_raw_titles": len(raw),
        "unique": len(unique),
        "non_weather": non_weather,
        "reaction_in_title": reaction,
        "impulse_updown_listed": impulse,
        "graded_articles": {
            label: (stats["rubric"].get(label) or {}).get("graded_articles") or 0
            for label, *_ in HORIZONS
        },
    }
    return {
        "book": "theme_radar_elite",
        "path": _path_text(use_lane, hop),
        "lane": hop,
        "watermark": _watermark(results),
        "funnel": funnel,
        "hit_rates": stats["rubric"],
        "hit_rates_broad_short": stats["broad_short"],
        "rippers": stats["rippers"],
        "amrx": _amrx_case(results),
        "harvest_source_mix": {
            "raw": dict(mix),
            "unique": dict(unique_mix),
        },
        "ledger": ledger,
        "macro_headline": {
            "n_stories": macro.get("n_stories"),
            "reprints_collapsed": macro.get("reprints_collapsed"),
            "headline_n_1d": macro.get("headline_n_1d"),
            "headline_hit_1d": macro.get("headline_hit_1d"),
            "headline_hit_rate_1d": macro.get("headline_hit_rate_1d"),
        },
        "clock": (
            "Entry is the next regular session after News Time. "
            "Published at/after 09:30 ET, including 09:30+30m (10:00), "
            "is not eligible for that cash session. "
            "0-1d is that session's open→close. "
            "2d/3d/4d/5d are the close N sessions after the entry session. "
            "1-4w is 20 sessions and stays secondary."
        ),
        "grade_rule": (
            "Rubric hit rates count a call only when q5=impulse, direction "
            "in {up, down}, tradeable_expression=direct, tape exists, and "
            "(for 0-1d and 2-5d) the class natural window is not 1-4w/1-6m. "
            "factor_impulse and index proxies stay ungraded. "
            "FOMC reprints collapse to one (factor, session, sign) and do not "
            "enter these rates. Reaction-in-title rows are killed by the router. "
            "Broad 2-5d rates keep long-horizon classes in the denominator so "
            "the windows Cyrus asked for are visible."
        ),
    }


def _fmt_ret(v: Any) -> str:
    if v is None:
        return "n/a"
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def _ripper_lines(rippers: dict, limit: int = 25) -> list[str]:
    lines = [
        "Same direction as the call and |forward return| at the threshold. "
        "5% rows are ≥5% and <10%. 10% rows are ≥10%.",
        "",
    ]
    for label in ("0-1d", "2d", "3d", "4d", "5d"):
        bag = rippers.get(label) or {}
        g5 = bag.get("ge_5") or []
        g10 = bag.get("ge_10") or []
        lines.append(f"### {label}")
        lines.append("")
        lines.append(f"- ≥5% and <10%: **{len(g5)}**")
        lines.append(f"- ≥10%: **{len(g10)}**")
        lines.append("")
        lines.append("| threshold | ticker | date | ret | class | title |")
        lines.append("| --- | --- | --- | ---: | --- | --- |")
        shown = 0
        for thresh, rows in (("≥10%", g10), ("≥5%", g5)):
            for item in rows:
                if shown >= limit:
                    break
                title = str(item.get("title") or "").replace("|", "/")
                lines.append(
                    f"| {thresh} | {item.get('ticker')} | "
                    f"{item.get('entry_date') or item.get('published_at') or ''} | "
                    f"{_fmt_ret(item.get('ret'))} | {item.get('event_class') or ''} | "
                    f"{title[:110]} |"
                )
                shown += 1
            if shown >= limit:
                break
        if not g5 and not g10:
            lines.append("| — | — | — | — | — | none |")
        elif shown < len(g5) + len(g10):
            lines.append(
                f"| … | | | | | {len(g5) + len(g10) - shown} more in the JSON |"
            )
        lines.append("")
    return lines


def _ledger_lines(ledger: dict) -> list[str]:
    counts = ledger.get("counts") or {}
    rates = ledger.get("hit_rates") or {}
    lines = [
        ledger.get("rule") or "",
        "",
        "Direct = the article names the ticker. "
        "Indirect = substitute / stays_out / arms_dealer / peer / sector basket "
        "from the family template. "
        + (ledger.get("theme_note") or ""),
        "",
        f"Router theme keys (not traded): **{ledger.get('n_theme_groups') or 0}**. "
        f"Name groups: **{ledger.get('n_name_groups') or 0}**.",
        "",
        "| bucket | groups |",
        "| --- | ---: |",
        f"| singleton | {counts.get('singleton', 0)} |",
        f"| converge | {counts.get('converge', 0)} "
        f"(up {counts.get('converge_up', 0)}, down {counts.get('converge_down', 0)}) |",
        f"| clash | {counts.get('clash', 0)} |",
        "",
        "Hit rates grade the entity once per session. "
        "Converge uses that side. Clash uses the net side (net 0 is ungraded). "
        "Long-horizon classes are out of these denominators.",
        "",
        "| bucket | 0-1d | 2d | 3d | 4d | 5d |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for bucket in ("singleton", "converge", "clash"):
        cells = []
        bag = rates.get(bucket) or {}
        for hz in ("0-1d", "2d", "3d", "4d", "5d"):
            slot = bag.get(hz) or {}
            cells.append(_pct(slot.get("hits") or 0, slot.get("n") or 0))
        lines.append(f"| {bucket} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "### Top converge names",
        "",
        "| ticker | date | n_bull | n_bear | net | 0-1d | 5d |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    top = ledger.get("top_converge") or []
    if not top:
        lines.append("| — | — | — | — | — | — | — |")
    for row in top:
        lines.append(
            f"| {row.get('ticker')} | {row.get('date')} | {row.get('n_bull')} | "
            f"{row.get('n_bear')} | {row.get('net')} | {_fmt_ret(row.get('ret_1d'))} | "
            f"{_fmt_ret(row.get('ret_5d'))} |"
        )
    return lines


def _amrx_week_lines(week: dict) -> list[str]:
    if not week:
        return []
    same = week.get("same_session") or {}
    lines = [
        "### AMRX week",
        "",
        week.get("note") or "",
        "",
        f"Window {week.get('week_window')}. "
        f"Lanreotide session `{week.get('session')}`. "
        f"Same-session label `{same.get('label')}` "
        f"n_bull={same.get('n_bull')} n_bear={same.get('n_bear')} "
        f"direct={same.get('n_direct')} indirect={same.get('n_indirect')} "
        f"0-1d {_fmt_ret(same.get('ret_1d'))}.",
        "",
    ]
    others = week.get("other_titles") or []
    if others:
        lines.append("Other stories on that session:")
        lines.append("")
        for title in others:
            lines.append(f"- {title}")
        lines.append("")
    lines.append("| session | label | n_bull | n_bear | stories | 0-1d | titles |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
    rows = week.get("week") or []
    if not rows:
        lines.append("| — | — | — | — | — | — | none |")
    for row in rows:
        titles = "; ".join(str(t) for t in (row.get("titles") or [])[:3]).replace("|", "/")
        lines.append(
            f"| {row.get('date')} | {row.get('label')} | {row.get('n_bull')} | "
            f"{row.get('n_bear')} | {row.get('n_stories')} | {_fmt_ret(row.get('ret_1d'))} | "
            f"{titles} |"
        )
    lines.append("")
    return lines


def markdown(payload: dict[str, Any]) -> str:
    funnel = payload["funnel"]
    rates = payload["hit_rates"]
    broad = payload["hit_rates_broad_short"]
    mix_u = (payload.get("harvest_source_mix") or {}).get("unique") or {}
    mix_r = (payload.get("harvest_source_mix") or {}).get("raw") or {}
    amrx = payload.get("amrx") or {}
    graded = funnel.get("graded_articles") or {}
    lines = [
        "# Theme-radar Elite news-impact",
        "",
        "Leak-free Lane/router backtest on **every** theme-radar snapshot date. "
        "Headlines only (Ticker, News Title, Daily Digest, News Time). "
        "theme-radar is read-only. The repos are not merged.",
        "",
        "## Path",
        "",
        payload.get("path") or "",
        "",
        "## Clock",
        "",
        payload.get("clock") or "",
        "",
        payload.get("grade_rule") or "",
        "",
        "## Funnel",
        "",
        "| step | n |",
        "| --- | ---: |",
        f"| Elite raw titles | {funnel.get('elite_raw_titles')} |",
        f"| unique (ticker + title, earliest News Time) | {funnel.get('unique')} |",
        f"| non-weather | {funnel.get('non_weather')} |",
        f"| reaction-in-title (killed by the router) | {funnel.get('reaction_in_title')} |",
        f"| impulse + up/down + listed | {funnel.get('impulse_updown_listed')} |",
        f"| graded articles 0-1d | {graded.get('0-1d', 0)} |",
        f"| graded articles 2d | {graded.get('2d', 0)} |",
        f"| graded articles 3d | {graded.get('3d', 0)} |",
        f"| graded articles 4d | {graded.get('4d', 0)} |",
        f"| graded articles 5d | {graded.get('5d', 0)} |",
        f"| graded articles 1-4w | {graded.get('1-4w', 0)} |",
        "",
        "## Hit rates",
        "",
        "Rubric column skips long-horizon classes on 0-1d and 2-5d. "
        "Broad column keeps those classes so the 2-5d windows stay visible. "
        "Rates are calls (one listed ticker), not articles.",
        "",
        "| horizon | rubric hits | rubric n | rubric rate | broad hits | broad n | broad rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, *_ in HORIZONS:
        rub = rates.get(label) or {}
        br = broad.get(label) or {}
        if label == "1-4w":
            lines.append(
                f"| {label} | {rub.get('hits', 0)} | {rub.get('n_calls', 0)} | "
                f"{_pct(rub.get('hits') or 0, rub.get('n_calls') or 0)} | — | — | — |"
            )
        else:
            lines.append(
                f"| {label} | {rub.get('hits', 0)} | {rub.get('n_calls', 0)} | "
                f"{_pct(rub.get('hits') or 0, rub.get('n_calls') or 0)} | "
                f"{br.get('hits', 0)} | {br.get('n_calls', 0)} | "
                f"{_pct(br.get('hits') or 0, br.get('n_calls') or 0)} |"
            )
    lines += ["", "## Convergence / clash ledger", ""]
    lines += _ledger_lines(payload.get("ledger") or {})
    lines += [
        "",
        "## Rippers",
        "",
    ]
    lines += _ripper_lines(payload.get("rippers") or {})
    lines += [
        "## AMRX",
        "",
        "| field | value |",
        "| --- | --- |",
        f"| ticker | {amrx.get('ticker') or 'AMRX'} |",
        f"| title | {amrx.get('title') or ''} |",
        f"| News Time | {amrx.get('news_time') or ''} |",
        f"| entry_date | {amrx.get('entry_date') or ''} |",
        f"| entry_clock | {amrx.get('entry_clock') or ''} |",
        f"| class | {amrx.get('event_class')} / q5={amrx.get('q5')} / sign={amrx.get('sign')} |",
        f"| direction | {amrx.get('direction')} |",
        f"| natural horizon | {amrx.get('horizon')} |",
        f"| 0-1d | {_fmt_ret(amrx.get('ret_1d'))} agree={amrx.get('agree_1d')} "
        f"skip_01d={amrx.get('skip_01d')} |",
        f"| 2d | {_fmt_ret(amrx.get('ret_2d'))} agree={amrx.get('agree_2d')} |",
        f"| 3d | {_fmt_ret(amrx.get('ret_3d'))} agree={amrx.get('agree_3d')} |",
        f"| 4d | {_fmt_ret(amrx.get('ret_4d'))} agree={amrx.get('agree_4d')} |",
        f"| 5d | {_fmt_ret(amrx.get('ret_5d'))} agree={amrx.get('agree_5d')} |",
        f"| 1-4w | {_fmt_ret(amrx.get('ret_20d'))} agree={amrx.get('agree_20d')} |",
        f"| note | {amrx.get('note') or ''} |",
        "",
        amrx.get("clock_rule") or "",
        "",
    ]
    lines += _amrx_week_lines((payload.get("ledger") or {}).get("amrx_week") or {})
    lines += [
        "",
        "## Harvest source mix",
        "",
        "Elite is in the book. `unused_readonly` applies only when no snapshot file is on disk.",
        "",
        "| source | raw titles | unique |",
        "| --- | ---: | ---: |",
    ]
    sources = sorted(set(mix_r) | set(mix_u))
    for src in sources:
        lines.append(f"| {src} | {mix_r.get(src, 0)} | {mix_u.get(src, 0)} |")
    if not sources:
        lines.append("| — | 0 | 0 |")
    elite_n = int(mix_u.get("theme_radar_elite") or 0)
    lines += [
        "",
        f"theme_radar_elite unique n = **{elite_n}**.",
        "",
        "## Hopper watermark (lane::model::source)",
        "",
    ]
    wm = payload.get("watermark") or {}
    if not wm:
        lines.append("- (none)")
    for k, n in wm.items():
        lines.append(f"- `{k}`: {n}")
    mh = payload.get("macro_headline") or {}
    lines += [
        "",
        "## FOMC / macro collapse",
        "",
        f"- stories={mh.get('n_stories')} reprints_collapsed={mh.get('reprints_collapsed')}",
        f"- headline basket 0-1d: {_pct(mh.get('headline_hit_1d') or 0, mh.get('headline_n_1d') or 0)}",
        "",
        "Reprints of the same factor/session/sign are one story. They are not in the hit rates above.",
        "",
    ]
    return "\n".join(lines)


def _jsonable_rippers(rippers: dict) -> dict:
    out = {}
    for label, bag in (rippers or {}).items():
        out[label] = {
            "ge_5_n": len(bag.get("ge_5") or []),
            "ge_10_n": len(bag.get("ge_10") or []),
            "ge_5": bag.get("ge_5") or [],
            "ge_10": bag.get("ge_10") or [],
        }
    return out


def run(
    date: str = "all",
    use_lane: bool = False,
    fetch: bool = True,
    limit: int = 0,
) -> dict[str, Any]:
    label = "all" if str(date).lower() in {"all", "*", "history", ""} else date
    raw = load_theme_radar(None if label == "all" else label, allow_remote=label != "all")
    raw = [a for a in raw if str(a.get("title") or "").strip()]
    unique = dedupe_elite(raw)
    if limit and limit > 0:
        unique = unique[:limit]
    results = []
    n_u = len(unique)
    print(f"[theme_radar] routing {n_u} unique titles (raw {len(raw)})", flush=True)
    for i, art in enumerate(unique, 1):
        results.append(analyze_article(
            art, use_lane=False, use_search=False, persist=False,
        ))
        if i % 5000 == 0 or i == n_u:
            print(f"[theme_radar] routed {i}/{n_u}", flush=True)
    hop = {"attempted": 0, "live": 0, "stopped": False, "probe": lane_available()}
    if use_lane:
        results, hop = _hop_elite_subset(unique, results)
        print(
            f"[theme_radar] lane attempted={hop.get('attempted')} "
            f"live={hop.get('live')} stopped={hop.get('stopped')}",
            flush=True,
        )
    need = [r for r in results if is_signed_listed(r)]
    print(f"[theme_radar] grading {len(need)} signed listed", flush=True)
    graded = grade_results(need, fetch=fetch) if need else []
    by_id = {str(r.get("article_id") or ""): r for r in graded}
    merged = []
    for row in results:
        taped = by_id.get(str(row.get("article_id") or ""))
        merged.append(taped if taped is not None else row)
    payload = build_payload(raw, unique, merged, use_lane=use_lane, hop=hop)
    payload["date"] = label
    blob = dict(payload)
    blob["rippers"] = _jsonable_rippers(payload.get("rippers") or {})
    RATES.parent.mkdir(parents=True, exist_ok=True)
    SCOREBOARD.parent.mkdir(parents=True, exist_ok=True)
    text = markdown(payload)
    SCOREBOARD.write_text(text, encoding="utf-8")
    RATES.write_text(json.dumps(blob, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    elite_n = (payload["harvest_source_mix"]["unique"] or {}).get("theme_radar_elite", 0)
    print(
        "theme_radar_board",
        "elite_unique", elite_n,
        "impulse", payload["funnel"]["impulse_updown_listed"],
        "amrx_entry", (payload.get("amrx") or {}).get("entry_date"),
        "amrx_1d", (payload.get("amrx") or {}).get("ret_1d"),
        "ledger", (payload.get("ledger") or {}).get("counts"),
        "amrx_stack", ((payload.get("ledger") or {}).get("amrx_week") or {}).get("stacked_same_session"),
        "→", SCOREBOARD,
    )
    return payload
