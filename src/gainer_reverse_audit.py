"""Reverse-run a session's liquid top gainers through already-printed pipelines.

Same-day Change% only picks the universe (who ripped). Membership is read
from that morning's catalyst targets, dossiers, news actions, News Judge,
stock-book 1d BUY list, and strategy tickets. No LLM is called.

A strategy is **improved** only when the bars in IMPROVE_RULES flip green.
Book% on combo_sh, fill-reality ideal-open, and the general direction call
do not count.

CLI:
  python -m src.gainer_reverse_audit --dates 2026-09-16,2026-09-17,2026-09-18 --write
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import catalyst_daily as cd
from . import gainer_asof as ga
from . import ticker_lookback as tl


def _finite(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
OUT_MD = ROOT / "03_scoreboard" / "STRATEGY_IMPROVE.md"
OUT_JSON = ROOT / "03_scoreboard" / "strategy_improve.json"
DAILY_MD = ROOT / "01_daily" / "strategy_improve.md"
TICKETS = ROOT / "data" / "day_board"
CATALYST = ROOT / "01_daily" / "catalyst"
NEWS = ROOT / "01_daily" / "news"
SLEEVE = ROOT / "data" / "sleeve_merge"

TOP_N = 15
MIN_CHANGE = 5.0
NEWS_NET_FLOOR = 1.0
KEEP_KEYS = (
    "union_hot_n4_h1",
    "combo_sh_5050_shared",
    "combo_sh_macd_5050_shared",
    "combo_ej_5050_shared",
    "union_e_fresh_h3",
    "yday_gainer_h1",
    "union_news_pack_net2_h1",
    "overnight_h1",
    "overnight_mega_h1",
    "flatten_h5",
    "flatten_robust",
)
FAT_SPX_C2C = 0.80
KEEP_OC_SLACK = 0.50
# Official Channel 1 prints when Yahoo SPY is missing (09-16..18).
CHANNEL1_SPX = {
    "2026-09-16": {"prev": 7585.73, "open": 7601.25, "close": 7551.81},
    "2026-09-17": {"prev": 7551.81, "open": 7631.44, "close": 7637.76},
    "2026-09-18": {"prev": 7637.76, "open": 7657.17, "close": 7650.50},
}
RECALL_BAR = 0.25
TOP5_HIT_BAR = 1
ROLLING_SESSIONS = 10
USABLE_DOSSIER_BAR = 2
STUCK_CAPTAINS_DAYS = 3

# Locked bars. A sleeve is "improved" only when these are true on the
# window, not when Book% ticks up.
IMPROVE_RULES = (
    {
        "id": "live_up_not_empty",
        "title": "Live book is in the market on UP mornings",
        "pass": (
            "On a session with morning S > 0, leftover cash ≥ $10k, and "
            "not hard-red: flatten_robust prints at least one live BUY "
            "(n_priced_buys ≥ 1 or a 09:30 ticket). io 3d must be able to "
            "schedule an exit (session calendar extends ≥ 3 sessions past D)."
        ),
        "fail": (
            "S positive and $101k leftover with 0 priced BUYs / "
            "`io 3d cannot settle` is a sit, not a strategy call."
        ),
    },
    {
        "id": "fat_day_keep_oc",
        "title": "KEEP longs are not red from the 09:30 open on fat index days",
        "pass": (
            f"On sessions with SPX close-to-close ≥ +{FAT_SPX_C2C:.2f}%: "
            "equal-weight open→close of that morning's KEEP long list "
            f"(union_hot_n4_h1, else combo_sh longs) ≥ SPX open→close "
            f"− {KEEP_OC_SLACK:.2f} pp. Gap days are scored from the open, "
            "not from the prior close."
        ),
        "fail": (
            "Thursday 09-17: SPX +1.14% close-to-close / +0.08% from the open; "
            "hot4 AZTA/RVTY/IT/FIVN −1.06% from the open."
        ),
    },
    {
        "id": "gainer_recall",
        "title": "Liquid rippers show up on a morning list",
        "pass": (
            f"Rolling {ROLLING_SESSIONS} sessions: of each day's liquid "
            f"top-{TOP_N} (Change% ≥ {MIN_CHANGE:.0f}%, mcap ≥ $100M, "
            f"adv ≥ 500k), ≥ {RECALL_BAR:.0%} are a hit, and ≥ {TOP5_HIT_BAR} "
            "of the day's top-5 is a hit on at least 6 of those sessions. "
            "Hit = in stock-book 1d BUY ∪ flatten/KEEP tickets ∪ news "
            f"|net| ≥ {NEWS_NET_FLOOR:g} ∪ a usable catalyst dossier."
        ),
        "fail": (
            "Same-day Change% is the universe only. A name that ripped "
            "with every camera dark and no list seat is a miss."
        ),
    },
    {
        "id": "catalyst_targets_move",
        "title": "Dossier seats are real company events, not stuck override captains",
        "pass": (
            f"Usable dossiers ≥ {USABLE_DOSSIER_BAR} that morning, the 8 "
            f"targets are not identical for {STUCK_CAPTAINS_DAYS} straight "
            "sessions, and ≥ 1 target is in that day's news |net| ≥ 2 "
            "or that day's liquid top-15 gainers."
        ),
        "fail": (
            "09-16/17/18: same 8 oil/coal override captains (NE RIG SLB "
            "BKR KGS WHD CNR BTU), then OpenClaw+DeepSeek empty STEP1 → 0/8."
        ),
    },
)

NOT_IMPROVED = (
    "Book% on combo_sh_5050 (short grind + one-week hot burst)",
    "Fill-reality ideal 09:30 butterfly",
    "General predict direction-call hit rate",
    "Paper Webull n_would with $0 cash",
)


def _jload(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _ticks(values) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        t = tl._tick(raw)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def load_catalyst(date: str) -> dict:
    data = _jload(CATALYST / f"{date}_dossiers.json")
    rows = [r for r in (data.get("dossiers") or []) if isinstance(r, dict)]
    targets = data.get("targets") or []
    target_ticks = _ticks(
        [(r.get("ticker") if isinstance(r, dict) else r) for r in targets]
        or [r.get("ticker") for r in rows]
    )
    usable = [r for r in rows if cd.usable_dossier(r)]
    return {
        "n_targets": int(data.get("n_targets") or len(target_ticks)),
        "n_ok": int(data.get("n_ok") or len(usable)),
        "targets": target_ticks,
        "target_roles": {
            tl._tick(r.get("ticker")): str(r.get("role") or "")
            for r in targets if isinstance(r, dict) and r.get("ticker")
        },
        "usable": _ticks(r.get("ticker") for r in usable),
        "errors": [str(r.get("error") or "")[:160] for r in rows if r.get("error")],
    }


def load_news_actions(date: str) -> dict[str, dict]:
    data = _jload(NEWS / f"{date}_actions.json")
    rows = data.get("ticker_actions") or []
    if isinstance(rows, dict):
        rows = [{"ticker": k, **(v if isinstance(v, dict) else {})}
                for k, v in rows.items()]
    out: dict[str, dict] = {}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        t = tl._tick(rec.get("ticker"))
        if not t:
            continue
        try:
            net = float(rec.get("net") or 0)
        except (TypeError, ValueError):
            net = 0.0
        out[t] = {
            "net": net,
            "side": str(rec.get("side") or ""),
            "buy": rec.get("buy_score"),
            "sell": rec.get("sell_score"),
        }
    return out


def load_judge_tickers(date: str) -> dict[str, float]:
    data = _jload(NEWS / f"{date}_judge.json")
    raw = data.get("tickers") or {}
    out: dict[str, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            t = tl._tick(k)
            if not t:
                continue
            try:
                out[t] = float(v)
            except (TypeError, ValueError):
                continue
    return out


def load_ticket_sets(date: str) -> dict[str, list[str]]:
    data = _jload(TICKETS / f"{date}_strategy_tickets.json")
    strats = data.get("strategies") or {}
    out: dict[str, list[str]] = {}
    for key in KEEP_KEYS:
        rec = strats.get(key) or {}
        buys = rec.get("buy") or []
        out[key] = _ticks(r.get("ticker") for r in buys if isinstance(r, dict))
        out[f"{key}_sit"] = bool(rec.get("sit"))
        out[f"{key}_buy_n"] = int(rec.get("buy_n") or len(out[key]))
    out["flatten_sit"] = bool((strats.get("flatten_robust") or {}).get("sit"))
    return out


def load_live_card(date: str) -> dict:
    today = _jload(SLEEVE / "today.json")
    if str(today.get("date") or "") == date:
        return today
    # Dated flatten card is markdown; sleeve today.json is the live stamp.
    return {"date": date}


def hit_sets(date: str) -> dict[str, set[str]]:
    catal = load_catalyst(date)
    news = load_news_actions(date)
    judge = load_judge_tickers(date)
    tickets = load_ticket_sets(date)
    book = ga.same_day_buy_set(date)
    keep = set()
    for key in KEEP_KEYS:
        keep.update(tickets.get(key) or [])
    news_hit = {t for t, r in news.items() if abs(float(r.get("net") or 0)) >= NEWS_NET_FLOOR}
    return {
        "stock_book_1d": set(book),
        "keep_tickets": keep,
        "news": news_hit,
        "judge": set(judge),
        "catal_targets": set(catal["targets"]),
        "catal_usable": set(catal["usable"]),
        "any": set(book) | keep | news_hit | set(catal["usable"]),
    }


def classify_gainer(ticker: str, rank: int, hits: dict[str, set[str]],
                    catal: dict) -> dict:
    t = tl._tick(ticker)
    flags = {k: t in (hits.get(k) or set()) for k in (
        "stock_book_1d", "keep_tickets", "news", "judge",
        "catal_targets", "catal_usable", "any",
    )}
    if flags["any"]:
        reason = "captured"
    elif flags["catal_targets"] and not flags["catal_usable"]:
        reason = "targeted_empty"
    elif flags["news"] or flags["judge"]:
        reason = "camera_only"
    elif catal.get("targets") and not flags["catal_targets"]:
        reason = "never_targeted"
    else:
        reason = "never_targeted"
    return {
        "ticker": t,
        "rank": rank,
        "reason": reason,
        **flags,
    }


def index_path(date: str) -> dict:
    """SPX/SPY close-to-close, gap, and open→close. Gap is not 09:30-harvestable."""
    if not date:
        return {"src": None, "gap_pct": None, "oc_pct": None, "c2c_pct": None}
    ch = CHANNEL1_SPX.get(date)
    if ch and ch.get("open") and ch.get("close") and ch.get("prev"):
        o, c, pc = float(ch["open"]), float(ch["close"]), float(ch["prev"])
        return {
            "src": "channel1",
            "gap_pct": round(100.0 * (o / pc - 1.0), 4),
            "oc_pct": round(100.0 * (c / o - 1.0), 4),
            "c2c_pct": round(100.0 * (c / pc - 1.0), 4),
        }
    bar = tl.session_bar("SPY", date) or {}
    o, c = _finite(bar.get("open")), _finite(bar.get("close"))
    # prior close from SPY if present on the bar, else None
    pc = _finite(bar.get("prev_close") or bar.get("prior_close"))
    if pc is None:
        # walk back one session via ticker_lookback if we can
        from . import gainer_capture as gc
        cal = []
        try:
            from . import sleeve_merge as sm
            cal = list(sm.session_calendar(sm.load_payload(), sm.list_books()))
        except Exception:
            cal = []
        prior = gc.prior_session(cal, date) if cal else None
        if prior:
            prev = tl.session_bar("SPY", prior) or {}
            pc = _finite(prev.get("close"))
    if not o or not c or not pc:
        return {"src": None, "gap_pct": None, "oc_pct": None, "c2c_pct": None}
    return {
        "src": "spy",
        "gap_pct": round(100.0 * (o / pc - 1.0), 4),
        "oc_pct": round(100.0 * (c / o - 1.0), 4),
        "c2c_pct": round(100.0 * (c / pc - 1.0), 4),
    }


def close_to_next_open(tickers: list[str], date: str,
                       next_date: str) -> float | None:
    """Equal-weight D close → D+1 open. None when no priced names."""
    rets = []
    for raw in tickers or []:
        t = tl._tick(raw)
        if not t:
            continue
        b0 = tl.session_bar(t, date) or {}
        b1 = tl.session_bar(t, next_date) or {}
        c, o = _finite(b0.get("close")), _finite(b1.get("open"))
        if c and o:
            rets.append(100.0 * (o / c - 1.0))
    if not rets:
        return None
    return round(sum(rets) / len(rets), 4)


def _mean(vals: list[float | None]) -> float | None:
    xs = [v for v in vals if v is not None]
    if not xs:
        return None
    return round(sum(xs) / len(xs), 4)


def score_overnight_find(dates: list[str] | None = None, *,
                         lookback_from: str = "2026-08-14",
                         walk: bool = True) -> dict:
    """Which leak-free D list harvested D+1 open. Calendar, not Change%."""
    from . import gainer_capture as gc
    from . import sleeve_merge as sm

    payload = sm.load_payload()
    books = sm.list_books()
    cal = [d for d in sm.session_calendar(payload, books) if d >= lookback_from]
    focus = set(dates or [])
    rows = []
    if walk:
        for i, d in enumerate(cal):
            if i + 1 >= len(cal):
                break
            prior = cal[i - 1] if i else None
            d1 = cal[i + 1]
            overnight = gc.overnight_scheduled(prior, d, d1)
            mega = gc.overnight_scheduled(
                prior, d, d1, min_mcap_m=gc.OVERNIGHT_MEGA_MCAP_M)
            yday = gc.yesterday_gainers(prior, top_n=25)
            react = gc.earnings_reaction(prior, d)
            rows.append({
                "date": d,
                "next": d1,
                "in_window": d in focus or d1 in focus,
                "overnight": overnight,
                "overnight_n": len(overnight),
                "overnight_gap": close_to_next_open(overnight, d, d1),
                "mega": mega,
                "mega_n": len(mega),
                "mega_gap": close_to_next_open(mega, d, d1),
                "yday": yday,
                "yday_n": len(yday),
                "yday_gap": close_to_next_open(yday, d, d1),
                "earn_react": react,
                "earn_react_n": len(react),
                "earn_react_gap": close_to_next_open(react, d, d1),
            })
    return {
        "lookback_from": lookback_from,
        "n_nights": len(rows),
        "mean_overnight": _mean([r.get("overnight_gap") for r in rows]),
        "mean_mega": _mean([r.get("mega_gap") for r in rows]),
        "mean_yday": _mean([r.get("yday_gap") for r in rows]),
        "mean_earn_react": _mean([r.get("earn_react_gap") for r in rows]),
        "n_mega_nights": sum(1 for r in rows if r.get("mega_n")),
        "n_overnight_nights": sum(1 for r in rows if r.get("overnight_n")),
        "days": rows,
        "rule": (
            "Find the bang *before* 09:30: buy D from the prior Finviz "
            "earnings calendar (AMC today / BMO next). Unfiltered liquid "
            "calendar is a coin flip. Mega-cap ($50B+) is the one-way "
            "slice. Index-like nights are yesterday's liquid winners "
            "(holdup / yday_gainer), not the earnings lottery. "
            "earn_react / e_fresh are after the print."
        ),
    }


def ew_open_close(tickers: list[str], date: str) -> float | None:
    rets = []
    for raw in tickers or []:
        t = tl._tick(raw)
        if not t:
            continue
        bar = tl.session_bar(t, date) or {}
        o, c = _finite(bar.get("open")), _finite(bar.get("close"))
        if o and c:
            rets.append(100.0 * (c / o - 1.0))
    if not rets:
        return None
    return round(sum(rets) / len(rets), 4)


def score_fat_day_keep(days: list[dict]) -> dict:
    """Fat C2C days: KEEP open→close vs index open→close. Gap is reported, not scored."""
    rows = []
    for d in days or []:
        date = d.get("date")
        if not date:
            continue
        path = index_path(date)
        c2c = path.get("c2c_pct")
        if c2c is None or c2c < FAT_SPX_C2C:
            continue
        keep = (d.get("keep") or {}).get("union_hot_n4_h1") or []
        event = (d.get("keep") or {}).get("union_e_fresh_h3") or []
        pack = (d.get("keep") or {}).get("union_news_pack_net2_h1") or []
        keep_oc = ew_open_close(keep, date)
        event_oc = ew_open_close(event, date)
        pack_oc = ew_open_close(pack, date)
        oc = path.get("oc_pct")
        keep_ok = (
            keep_oc is not None and oc is not None
            and keep_oc >= oc - KEEP_OC_SLACK
        )
        rows.append({
            "date": date,
            "c2c_pct": c2c,
            "gap_pct": path.get("gap_pct"),
            "oc_pct": oc,
            "keep": keep,
            "keep_oc": keep_oc,
            "e_fresh_oc": event_oc,
            "news_pack_oc": pack_oc,
            "keep_ok": keep_ok,
            "gap_share": (
                round(path["gap_pct"] / c2c, 3)
                if c2c and path.get("gap_pct") is not None else None
            ),
        })
    if not rows:
        return {
            "pass": None,
            "n_fat": 0,
            "days": [],
            "why": "No fat C2C days (≥ +0.80%) on this window with an index print.",
        }
    n_ok = sum(1 for r in rows if r.get("keep_ok"))
    return {
        "pass": n_ok == len(rows),
        "n_fat": len(rows),
        "n_ok": n_ok,
        "days": rows,
        "why": (
            f"{n_ok}/{len(rows)} fat days: KEEP OC ≥ index OC − {KEEP_OC_SLACK:.2f}pp. "
            "C2C fat days are often the overnight gap; 09:30 longs only get the leftover OC. "
            + "; ".join(
                f"{r['date']} c2c={r['c2c_pct']:+.2f} gap={r['gap_pct']:+.2f} "
                f"idx_oc={r['oc_pct']:+.2f} keep_oc="
                f"{'—' if r['keep_oc'] is None else f'{r['keep_oc']:+.2f}'}"
                for r in rows
            )
        ),
    }


def audit_date(date: str, top_n: int = TOP_N,
               min_change: float = MIN_CHANGE) -> dict:
    df = ga.load_finviz(date)
    gainers = ga.liquid_gainers(df, top_n=top_n, min_change=min_change)
    catal = load_catalyst(date)
    news = load_news_actions(date)
    judge = load_judge_tickers(date)
    tickets = load_ticket_sets(date)
    hits = hit_sets(date)
    rows = []
    for i, g in enumerate(gainers, 1):
        packed = classify_gainer(g["ticker"], i, hits, catal)
        packed.update({
            "change_pct": g.get("change_pct"),
            "sector": g.get("sector") or "",
            "company": g.get("company") or "",
            "news_net": (news.get(g["ticker"]) or {}).get("net"),
            "judge": judge.get(g["ticker"]),
        })
        rows.append(packed)
    n = len(rows)
    n_hit = sum(1 for r in rows if r["any"])
    n_top5 = sum(1 for r in rows[:5] if r["any"])
    reasons = Counter(r["reason"] for r in rows)
    return {
        "date": date,
        "n_gainers": n,
        "n_hit": n_hit,
        "recall": (n_hit / n) if n else None,
        "top5_hits": n_top5,
        "reasons": dict(reasons),
        "gainers": rows,
        "catalyst": {
            "n_targets": catal["n_targets"],
            "n_ok": catal["n_ok"],
            "targets": catal["targets"],
            "target_roles": catal.get("target_roles") or {},
            "usable": catal["usable"],
            "error_sample": (catal.get("errors") or [""])[0],
        },
        "keep": {k: tickets.get(k) for k in KEEP_KEYS},
        "flatten_sit": tickets.get("flatten_sit"),
        "stock_book_n": len(hits["stock_book_1d"]),
        "news_n": len(hits["news"]),
        "coverage": ga.tape_coverage(df),
    }


def score_improve(days: list[dict]) -> dict:
    """Grade the locked bars on the audited window (not a live wire)."""
    if not days:
        return {rule["id"]: {"pass": False, "why": "no sessions"} for rule in IMPROVE_RULES}
    out = {}
    recalls = [d["recall"] for d in days if d.get("recall") is not None]
    top5 = [int(d.get("top5_hits") or 0) for d in days]
    mean_recall = (sum(recalls) / len(recalls)) if recalls else 0.0
    top5_ok_days = sum(1 for n in top5 if n >= TOP5_HIT_BAR)
    out["gainer_recall"] = {
        "pass": mean_recall >= RECALL_BAR and top5_ok_days >= max(1, (len(days) + 1) // 2),
        "mean_recall": round(mean_recall, 3),
        "top5_ok_days": top5_ok_days,
        "n_days": len(days),
        "why": (
            f"mean recall {mean_recall:.0%} vs {RECALL_BAR:.0%} bar; "
            f"top-5 hit on {top5_ok_days}/{len(days)} sessions"
        ),
    }
    ok_dossiers = [d["catalyst"]["n_ok"] for d in days]
    target_sets = [tuple(d["catalyst"]["targets"]) for d in days]
    stuck = (
        len(target_sets) >= STUCK_CAPTAINS_DAYS
        and len(set(target_sets[-STUCK_CAPTAINS_DAYS:])) == 1
        and bool(target_sets[-1])
    )
    overlap = []
    for d in days:
        gset = {r["ticker"] for r in d.get("gainers") or []}
        overlap.append(len(gset & set(d["catalyst"]["targets"])))
    out["catalyst_targets_move"] = {
        "pass": (
            min(ok_dossiers) >= USABLE_DOSSIER_BAR
            and not stuck
            and max(overlap or [0]) >= 1
        ),
        "n_ok": ok_dossiers,
        "stuck_captains": stuck,
        "target_gainer_overlap": overlap,
        "why": (
            f"usable {ok_dossiers}; stuck_captains={stuck}; "
            f"target∩gainer {overlap}"
        ),
    }
    sits = [d.get("flatten_sit") for d in days]
    out["live_up_not_empty"] = {
        "pass": False if any(sits) else None,
        "flatten_sit": sits,
        "why": (
            "flatten_robust sit=True on an audited UP window is a fail "
            "when leftover cash could buy 1 share (see sleeve today.json)."
            if any(sits) else
            "No sit flag on this window — still check leftover cash + tickets."
        ),
    }
    out["fat_day_keep_oc"] = score_fat_day_keep(days)
    return out


def render_markdown(payload: dict) -> str:
    days = payload.get("days") or []
    grades = payload.get("grades") or {}
    lines = [
        "# Strategy improve — gainer reverse-run",
        "",
        f"_Generated {payload.get('generated_at')} — research audit, not a wire._",
        "",
        "Liquid Finviz top-"
        f"{payload.get('top_n')} gainers (Change% ≥ "
        f"{payload.get('min_change'):g}%, mcap ≥ $100M, adv ≥ 500k) "
        "run back through the 09:30 packet that already printed. "
        "Same-day Change% only picks the universe.",
        "",
        "## When is a strategy improved?",
        "",
        "All four bars below must be green on a rolling 10-session window. "
        "Book% / fill-reality ideal / general direction-call do **not** count.",
        "",
    ]
    for rule in IMPROVE_RULES:
        grade = grades.get(rule["id"]) or {}
        mark = {
            True: "PASS",
            False: "FAIL",
            None: "MEASURE",
        }.get(grade.get("pass"), "MEASURE")
        lines += [
            f"### `{rule['id']}` — {rule['title']} · **{mark}**",
            "",
            f"- Pass: {rule['pass']}",
            f"- Fail looks like: {rule['fail']}",
        ]
        if grade.get("why"):
            lines.append(f"- This window: {grade['why']}")
        lines.append("")
    lines += [
        "## What does **not** count as improved",
        "",
    ]
    for item in NOT_IMPROVED:
        lines.append(f"- {item}")
    lines += [
        "",
        "## Why real catalysts miss the book",
        "",
        "Three stacked filters, in order:",
        "",
        "1. **Wrong eight seats.** `catalyst_daily.select_targets` fills "
        "OVERRIDE map-heat captains first. Oil & Gas Drilling / Equipment / "
        "Thermal Coal consumed all 8 slots (NE RIG SLB BKR KGS WHD CNR BTU) "
        "on 09-16, 09-17, and 09-18. News `action_top` and the day's actual "
        "rippers never get a dossier.",
        "2. **Those seats then go empty.** OpenClaw + DeepSeek return empty "
        "on CATALYST STEP1/STEP2 → `n_ok=0`. Usable-dossier boost to news "
        "actions is zero, so the company route has nothing to adjudicate.",
        "3. **The book is a different pile.** News actions stay on the oil "
        "E&P cluster (COP/EOG/RRC…). Judge prints sector ETFs (XLE/IGV), "
        "not SDGR/GNRC. Flatten / hot4 pick healthcare size-book names. "
        "Live flatten then sits (`io 3d cannot settle`) so even the wrong "
        "names are not bought.",
        "",
        "Dossiers also run **after** the stock book in preopen ALL, so a "
        "healthy STEP1 still cannot pick that morning's BUY list.",
        "",
        "## Why long-only books miss highly positive days",
        "",
        "Two stacked reasons, in order:",
        "",
        "1. **The fat print is the overnight gap.** A 09:30 long buys "
        "after the gap. Thursday 09-17 SPX +1.14% close-to-close was "
        "+1.05% before the open and +0.08% from 09:30→16:00. "
        "`union_hot_n4_h1` is hold=1 — it never carries last night's "
        "lot into that gap. `union_hot_n4_holdup` keeps S>0 lots through "
        "the next 09:30 so the *next* gap is in the book. "
        "`union_e_fresh_h3` already holds 3.",
        "2. **KEEP hot4 is leftover tape, not index beta.** On the "
        "09-03 grind fat day (SPY +0.69% from the open) hot4 "
        "GPRO/REAX/CNH/MMED was **−5.28%**. The same morning "
        "`union_e_fresh_h3` (AVGO/CIEN/FIVE…) was **+4.07%** and "
        "`union_news_pack_net2_h1` (AVGO/DELL/HPE) was **+7.37%**. "
        "Event / news longs participated. Hot-score micro names faded.",
        "",
        "Morning S does not call fat grind days (09-03 S=−0.9). "
        "S=+7 on 09-17 was after the gap was already printed.",
        "",
        "## How to find overnight bangers",
        "",
    ]
    ov = payload.get("overnight") or {}
    lines += [
        ov.get("rule") or (
            "Buy D 09:30 names the **prior** Finviz calendar said report "
            "AMC today or BMO next session. Hold 1 sells at the next open."
        ),
        "",
        "Two different nights, two different lists:",
        "",
        "1. **Print night** — AMC today / BMO next from the prior export. "
        "`overnight_mega_h1` keeps mcap ≥ $50B. The full liquid calendar "
        "has more −5% dumps than +5% bangs. "
        "`earn_react` / `e_fresh` fire *after* the print — they miss the bang.",
        "2. **Index-like night** — yesterday's liquid winners "
        "(`yday_gainer_h1` / `union_hot_n4_holdup` when S>0). "
        "Thursday 09-17's +1.05% SPX gap was this kind of night: "
        "scheduled AMC leftover was ALMU/LEN, not mega-cap beta.",
        "",
    ]
    if ov.get("n_nights"):
        def _fmt(v):
            return "—" if v is None else f"{v:+.2f}%"
        lines += [
            f"Lookback {ov.get('lookback_from')} → last closed "
            f"({ov.get('n_nights')} nights; D close → D+1 open EW):",
            "",
            f"- scheduled liquid `overnight`: {_fmt(ov.get('mean_overnight'))} "
            f"on {ov.get('n_overnight_nights')} nights with names",
            f"- scheduled mega `overnight_mega` ($50B+): "
            f"{_fmt(ov.get('mean_mega'))} on {ov.get('n_mega_nights')} nights",
            f"- yesterday's liquid 25: {_fmt(ov.get('mean_yday'))}",
            f"- post-print `earn_react`: {_fmt(ov.get('mean_earn_react'))}",
            "",
        ]
        focus = [r for r in (ov.get("days") or []) if r.get("in_window")]
        if focus:
            lines.append("Audited window nights:")
            lines.append("")
            for r in focus:
                lines.append(
                    f"- {r['date']}→{r['next']}: "
                    f"overnight n={r.get('overnight_n')} "
                    f"gap={_fmt(r.get('overnight_gap'))}; "
                    f"mega n={r.get('mega_n')} "
                    f"gap={_fmt(r.get('mega_gap'))}; "
                    f"yday25 gap={_fmt(r.get('yday_gap'))}; "
                    f"earn_react gap={_fmt(r.get('earn_react_gap'))}"
                    + (
                        " · " + ", ".join(f"`{t}`" for t in (r.get("mega") or [])[:8])
                        if r.get("mega") else ""
                    )
                )
            lines.append("")
    lines += [
        "## Reverse-run",
        "",
    ]
    for day in days:
        rec = day.get("recall")
        rec_s = "—" if rec is None else f"{rec:.0%}"
        lines += [
            f"### {day['date']} · recall {rec_s} "
            f"({day.get('n_hit')}/{day.get('n_gainers')}) · "
            f"top-5 hits {day.get('top5_hits')} · "
            f"dossiers {day['catalyst']['n_ok']}/{day['catalyst']['n_targets']}"
            + (" · flatten **sit**" if day.get("flatten_sit") else ""),
            "",
        ]
        if day["catalyst"].get("targets"):
            lines.append(
                "Catalyst targets: "
                + ", ".join(f"`{t}`" for t in day["catalyst"]["targets"])
            )
            lines.append("")
        if day["catalyst"].get("error_sample"):
            lines.append(f"Dossier error: {day['catalyst']['error_sample']}")
            lines.append("")
        keep = day.get("keep") or {}
        if keep.get("union_hot_n4_h1"):
            lines.append(
                "KEEP hot4: "
                + ", ".join(f"`{t}`" for t in keep["union_hot_n4_h1"])
            )
            lines.append("")
        lines += [
            "| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |",
            "|---:|---|---:|---|---|---|---|---|---|---|",
        ]
        for r in day.get("gainers") or []:
            chg = r.get("change_pct")
            chg_s = "—" if chg is None else f"{chg:+.1f}%"
            yn = lambda v: "Y" if v else ""
            lines.append(
                f"| {r['rank']} | `{r['ticker']}` | {chg_s} | "
                f"{r.get('sector') or '—'} | "
                f"{'Y' if r.get('any') else ''} | {r.get('reason')} | "
                f"{yn(r.get('stock_book_1d'))} | {yn(r.get('keep_tickets'))} | "
                f"{yn(r.get('news'))} | {yn(r.get('catal_usable'))} |"
            )
        lines.append("")
    lines += [
        "## How to extend the reverse-run",
        "",
        "```",
        "python -m src.gainer_reverse_audit --dates YYYY-MM-DD,YYYY-MM-DD --write",
        "```",
        "",
        "Optional: `python -m src.ticker_lookback_run --tickers SDGR,GNRC` "
        "paints the 12 09:30 boxes on one name. "
        "`python -m src.gainer_lookback_action --write` is the full-history "
        "BUY/SELL catch board (stale through 09-08 until restamped).",
        "",
        "Live `flatten_robust`, hard-red sit, and Webull paper stay sit.",
        "",
    ]
    return "\n".join(lines) + "\n"


def run(dates: list[str], top_n: int = TOP_N, min_change: float = MIN_CHANGE,
        write: bool = False) -> dict:
    days = [audit_date(d, top_n=top_n, min_change=min_change) for d in dates]
    payload = {
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "top_n": top_n,
        "min_change": min_change,
        "dates": dates,
        "days": days,
        "grades": score_improve(days),
        "overnight": score_overnight_find(dates),
        "rules": IMPROVE_RULES,
        "not_improved": list(NOT_IMPROVED),
        "live_untouched": "flatten_robust",
    }
    if write:
        OUT_MD.parent.mkdir(parents=True, exist_ok=True)
        OUT_MD.write_text(render_markdown(payload), encoding="utf-8")
        OUT_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        DAILY_MD.write_text(render_markdown(payload), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dates", default="2026-09-16,2026-09-17,2026-09-18")
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--min-change", type=float, default=MIN_CHANGE)
    p.add_argument("--write", action="store_true")
    ns = p.parse_args(argv)
    dates = [d.strip() for d in str(ns.dates).split(",") if d.strip()]
    payload = run(dates, top_n=ns.top_n, min_change=ns.min_change, write=ns.write)
    print(render_markdown(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
