"""Per-session name ledger: converge / clash / singleton.

One listed ticker is one entity per entry session. Reprints of the same
sentence (Elite row and a Finviz wrap) are one vote. Regime and weather
do not vote. Sector and theme groups exist only when the router already
emitted that key — the Elite Sector column is not turned into a trade.

Direct = the article names the ticker (role named, expression direct).
Indirect = substitute / stays_out / arms_dealer / peer / sector basket
from the family template.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from .grade import entry_calendar_date, signal_dt
from .horizons import skips_short_window

INDIRECT_ROLES = frozenset({
    "substitute",
    "complement",
    "unscathed_rival",
    "arms_dealer",
    "incumbent_intermediary",
    "new_venue",
    "supplier",
    "customer",
    "competitor",
    "parent_of_named",
})

_HORIZONS = (
    ("0-1d", "ret_1d"),
    ("2d", "ret_2d"),
    ("3d", "ret_3d"),
    ("4d", "ret_4d"),
    ("5d", "ret_5d"),
)

# Snapshot week of the lanreotide print through its Monday entry session.
AMRX_WEEK = ("2026-09-14", "2026-09-21")


def story_id(title: str) -> str:
    """Normalize a headline so a wrap and the Elite sentence are one story."""
    s = re.sub(r"[^a-z0-9]+", " ", str(title or "").lower()).strip()
    return s[:160]


def session_of(row: dict) -> str:
    """Elite News Time → next RTH session.

    Published at/after 09:30 ET rolls to the next calendar day, then Saturday
    and Sunday roll to Monday. The tape grade uses that same next bar.
    """
    when = signal_dt(row)
    if when is None:
        return ""
    day = entry_calendar_date(when)
    try:
        dt = datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        return day
    if dt.weekday() == 5:
        dt += timedelta(days=2)
    elif dt.weekday() == 6:
        dt += timedelta(days=1)
    return dt.isoformat()


def channel_of(entity: dict) -> str:
    role = str(entity.get("role") or "named")
    expr = str(entity.get("tradeable_expression") or "direct")
    if entity.get("stays_out") or role in INDIRECT_ROLES or expr == "proxy":
        return "indirect"
    if role == "named":
        return "direct"
    return "indirect"


def label_of(n_bull: int, n_bear: int) -> str:
    if n_bull >= 1 and n_bear >= 1:
        return "clash"
    if n_bull >= 2 and n_bear == 0:
        return "converge_up"
    if n_bear >= 2 and n_bull == 0:
        return "converge_down"
    return "singleton"


def _impulse(row: dict) -> bool:
    cls = row.get("classification") or {}
    if str(cls.get("q5") or "") != "impulse":
        return False
    if str(cls.get("event_class") or "") in {"discard", "regime_state", "rumor"}:
        return False
    return True


def _votes(row: dict) -> list[dict]:
    """Signed listed expressions this article actually emits. No invented themes."""
    if not _impulse(row):
        return []
    cls = row.get("classification") or {}
    ev = str(cls.get("event_class") or "")
    out = []
    seen: set[str] = set()
    for e in row.get("entities") or []:
        if not isinstance(e, dict):
            continue
        tick = str(e.get("ticker") or "").strip().upper()
        direction = str(e.get("direction") or "")
        if not tick or direction not in {"up", "down"} or tick in seen:
            continue
        if str(e.get("tradeable_expression") or "direct") == "none":
            continue
        seen.add(tick)
        kind = "sector_etf" if str(e.get("tradeable_expression") or "") == "proxy" else "ticker"
        out.append({
            "kind": kind,
            "key": tick,
            "direction": direction,
            "channel": channel_of(e),
            "event_class": ev,
        })
    factor = str(cls.get("factor") or row.get("macro_factor") or "").strip()
    themes = [str(t).strip() for t in (row.get("macro_themes") or []) if str(t).strip()]
    # Theme key only when the router set one. Price stays on the ETFs above.
    for key in ([factor] if factor else []) + themes:
        out.append({
            "kind": "theme",
            "key": key.lower(),
            "direction": "",
            "channel": "indirect",
            "event_class": ev,
            "theme_only": True,
        })
    return out


def _rets_for(row: dict, ticker: str, session: str) -> dict[str, Any] | None:
    for g in row.get("performance") or []:
        if not isinstance(g, dict):
            continue
        if str(g.get("ticker") or "").upper() != ticker:
            continue
        if str(g.get("entry_date") or "") != session:
            continue
        return {k: g.get(k) for _, k in _HORIZONS}
    return None


def _agree(direction: str, ret: float | None) -> bool | None:
    if ret is None or direction not in {"up", "down"}:
        return None
    if direction == "up":
        return float(ret) > 0
    return float(ret) < 0


def build_ledger(results: list[dict]) -> dict[str, Any]:
    """Stack impulse votes onto one entity per session. Reprints do not vote twice."""
    stories: dict[tuple, dict] = {}
    for row in results:
        session = session_of(row)
        if not session:
            continue
        sid = story_id(str(row.get("title") or ""))
        if not sid:
            continue
        published = str(row.get("published_at") or "9999")
        title = str(row.get("title") or "")[:180]
        for vote in _votes(row):
            if vote.get("theme_only"):
                gid = (session, "theme", vote["key"])
                slot = stories.setdefault(gid, {
                    "session": session,
                    "kind": "theme",
                    "key": vote["key"],
                    "stories": {},
                })
                slot["stories"].setdefault(sid, {
                    "story_id": sid,
                    "title": title,
                    "published_at": published,
                    "direction": "",
                    "channel": "indirect",
                    "event_class": vote["event_class"],
                })
                continue
            gid = (session, vote["kind"], vote["key"])
            slot = stories.setdefault(gid, {
                "session": session,
                "kind": vote["kind"],
                "key": vote["key"],
                "stories": {},
            })
            prev = slot["stories"].get(sid)
            if prev is not None and str(prev.get("published_at") or "9999") <= published:
                if prev.get("channel") == "indirect" and vote["channel"] == "direct":
                    prev["channel"] = "direct"
                continue
            rets = _rets_for(row, vote["key"], session)
            slot["stories"][sid] = {
                "story_id": sid,
                "title": title,
                "published_at": published,
                "direction": vote["direction"],
                "channel": vote["channel"],
                "event_class": vote["event_class"],
                "rets": rets,
            }

    groups = []
    for slot in stories.values():
        items = list(slot["stories"].values())
        if slot["kind"] == "theme":
            groups.append({
                "session": slot["session"],
                "kind": "theme",
                "key": slot["key"],
                "label": "theme",
                "n_stories": len(items),
                "n_bull": 0,
                "n_bear": 0,
                "n_direct": 0,
                "n_indirect": len(items),
                "net": 0,
                "direction": "",
                "skip_short": True,
                "titles": [it["title"] for it in items[:6]],
            })
            continue
        bulls = [it for it in items if it["direction"] == "up"]
        bears = [it for it in items if it["direction"] == "down"]
        n_bull, n_bear = len(bulls), len(bears)
        if n_bull + n_bear == 0:
            continue
        label = label_of(n_bull, n_bear)
        net = n_bull - n_bear
        if label == "clash":
            direction = "up" if net > 0 else "down" if net < 0 else ""
        elif label == "converge_up":
            direction = "up"
        elif label == "converge_down":
            direction = "down"
        else:
            direction = "up" if n_bull else "down"
        skip = all(
            skips_short_window(str(it.get("event_class") or ""), str(it.get("title") or ""))
            for it in items
        )
        rets: dict[str, Any] = {}
        for it in items:
            got = it.get("rets") or {}
            for _, key in _HORIZONS:
                if rets.get(key) is None and got.get(key) is not None:
                    rets[key] = got.get(key)
        groups.append({
            "session": slot["session"],
            "kind": slot["kind"],
            "key": slot["key"],
            "label": label,
            "n_stories": n_bull + n_bear,
            "n_bull": n_bull,
            "n_bear": n_bear,
            "n_direct": sum(1 for it in items if it["channel"] == "direct"),
            "n_indirect": sum(1 for it in items if it["channel"] == "indirect"),
            "net": net,
            "direction": direction,
            "skip_short": skip,
            "ret_1d": rets.get("ret_1d"),
            "ret_2d": rets.get("ret_2d"),
            "ret_3d": rets.get("ret_3d"),
            "ret_4d": rets.get("ret_4d"),
            "ret_5d": rets.get("ret_5d"),
            "titles": [it["title"] for it in items[:8]],
            "story_ids": [it["story_id"] for it in items],
        })

    names = [g for g in groups if g["kind"] in {"ticker", "sector_etf"}]
    counts = {"singleton": 0, "converge": 0, "clash": 0,
              "converge_up": 0, "converge_down": 0}
    rates = {
        bucket: {hz: {"hits": 0, "n": 0} for hz, _ in _HORIZONS}
        for bucket in ("singleton", "converge", "clash")
    }
    for g in names:
        label = g["label"]
        bucket = "converge" if label.startswith("converge") else label
        counts[bucket] = counts.get(bucket, 0) + 1
        if label in {"converge_up", "converge_down"}:
            counts[label] = counts.get(label, 0) + 1
        direction = g.get("direction") or ""
        if not direction:
            continue
        for hz, key in _HORIZONS:
            if g.get("skip_short"):
                continue
            ret = g.get(key)
            if ret is None:
                continue
            agree = _agree(direction, ret)
            if agree is None:
                continue
            rates[bucket][hz]["n"] += 1
            rates[bucket][hz]["hits"] += int(agree is True)
    for bucket, bag in rates.items():
        for hz, slot in bag.items():
            n = slot["n"]
            slot["hit_rate"] = round(slot["hits"] / n, 4) if n else None

    converge = [g for g in names if str(g["label"]).startswith("converge")]
    converge.sort(
        key=lambda g: (
            -(g["n_bull"] + g["n_bear"]),
            -abs(float(g["ret_1d"])) if g.get("ret_1d") is not None else 0,
        ),
    )
    top = []
    for g in converge[:25]:
        top.append({
            "ticker": g["key"],
            "date": g["session"],
            "label": g["label"],
            "n_bull": g["n_bull"],
            "n_bear": g["n_bear"],
            "n_direct": g["n_direct"],
            "n_indirect": g["n_indirect"],
            "net": g["net"],
            "direction": g["direction"],
            "ret_1d": g.get("ret_1d"),
            "ret_2d": g.get("ret_2d"),
            "ret_5d": g.get("ret_5d"),
            "titles": g.get("titles") or [],
        })
    themes = [g for g in groups if g["kind"] == "theme"]
    return {
        "counts": counts,
        "hit_rates": rates,
        "top_converge": top,
        "n_name_groups": len(names),
        "n_theme_groups": len(themes),
        "theme_note": (
            "Theme groups are router factor/macro keys only. "
            "They are not a second trade. The Elite Sector column is not a key."
        ),
        "amrx_week": amrx_week(names, results),
        "rule": (
            "One entity per session. n_bull / n_bear count impulse up/down "
            "stories after title-normalize. Regime and weather do not vote. "
            "clash = both sides on that entity that session. "
            "converge = two or more stories, one side. "
            "Hit rates grade the entity once: converge uses that side, "
            "clash uses the net side (net 0 is ungraded). "
            "Long-horizon classes stay out of the 0-1d..5d denominator."
        ),
    }


def clash_converge_ids(results: list[dict]) -> set[str]:
    """Article ids that sit in a clash or converge name group. Lane subset."""
    # Rebuild story membership with article ids.
    owners: dict[tuple, set[str]] = {}
    arts: dict[tuple, set[str]] = {}
    for row in results:
        session = session_of(row)
        sid = story_id(str(row.get("title") or ""))
        aid = str(row.get("article_id") or "")
        if not session or not sid or not aid:
            continue
        for vote in _votes(row):
            if vote.get("theme_only"):
                continue
            gid = (session, vote["kind"], vote["key"], sid)
            owners.setdefault((session, vote["kind"], vote["key"]), set()).add(
                (sid, vote["direction"])
            )
            arts.setdefault((session, vote["kind"], vote["key"]), set()).add(aid)
    keep: set[str] = set()
    for gid, pairs in owners.items():
        bulls = {s for s, d in pairs if d == "up"}
        bears = {s for s, d in pairs if d == "down"}
        label = label_of(len(bulls), len(bears))
        if label in {"clash", "converge_up", "converge_down"}:
            keep |= arts.get(gid, set())
    return keep


def amrx_week(groups: list[dict], results: list[dict]) -> dict[str, Any]:
    """Did a second story stack with the lanreotide print on its session / week?"""
    lan = [
        r for r in results
        if str(r.get("ticker_hint") or "").upper() == "AMRX"
        and "lanreotide" in str(r.get("title") or "").lower()
    ]
    if not lan:
        return {"ok": False, "stacked": False, "note": "lanreotide row missing"}
    row = lan[0]
    session = session_of(row)
    sid = story_id(str(row.get("title") or ""))
    lo, hi = AMRX_WEEK
    week = [
        g for g in groups
        if g.get("key") == "AMRX" and lo <= str(g.get("session") or "") <= hi
    ]
    same = next((g for g in groups if g.get("key") == "AMRX" and g.get("session") == session), None)
    others = []
    if same:
        for title, story in zip(same.get("titles") or [], same.get("story_ids") or []):
            if story == sid:
                continue
            others.append(title)
        # titles are capped; story_ids match that cap. Count the rest.
        extra = [
            s for s in (same.get("story_ids") or [])
            if s != sid
        ]
        n_other = len(extra)
    else:
        n_other = 0
    week_rows = []
    for g in sorted(week, key=lambda g: str(g.get("session") or "")):
        week_rows.append({
            "date": g.get("session"),
            "label": g.get("label"),
            "n_bull": g.get("n_bull"),
            "n_bear": g.get("n_bear"),
            "n_stories": g.get("n_stories"),
            "ret_1d": g.get("ret_1d"),
            "titles": g.get("titles") or [],
        })
    stacked = n_other > 0
    if stacked:
        note = (
            f"{n_other} other story(s) voted AMRX on the lanreotide session {session}."
        )
    else:
        note = (
            f"No second story voted AMRX on the lanreotide session {session}. "
            "The print is a singleton on that entity that day."
        )
    return {
        "ok": True,
        "session": session,
        "story_id": sid,
        "stacked_same_session": stacked,
        "n_other_same_session": n_other,
        "other_titles": others,
        "same_session": {
            "label": (same or {}).get("label"),
            "n_bull": (same or {}).get("n_bull"),
            "n_bear": (same or {}).get("n_bear"),
            "n_direct": (same or {}).get("n_direct"),
            "n_indirect": (same or {}).get("n_indirect"),
            "ret_1d": (same or {}).get("ret_1d"),
        },
        "week": week_rows,
        "week_window": f"{lo}..{hi}",
        "note": note,
    }
