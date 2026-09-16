"""Size today's combo_sh 50/50 + MACD-short tickets for a broker.

Research book: combo_sh_5050_shared + short ∩ macd_up beat the plain
50/50. This module turns that mix into whole-share BUY / SELL tickets
against the account's leftover cash. It does not change flatten_robust.
"""
from __future__ import annotations

import json

from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_combo as fmc
from src import flatten_lookback_action as fla
from src import gainer_capture as gc
from src import sleeve_merge as sm
from src import ticker_lookback as tl
from src.futubull_exec import BrokerSnap

PAPER_COMBO = "combo_sh_macd_5050_shared"
PANEL_PATH = fm.PANEL_PATH


def combo_spec(name: str = PAPER_COMBO) -> dict:
    for spec in fmc.combo_specs():
        if spec["name"] == name:
            return spec
    raise KeyError(f"unknown combo {name}")


def quote_px(ticker: str, date: str, *, cal: list[str] | None = None,
             row: dict | None = None) -> float | None:
    """09:30 open if printed, else last official close. Never same-day Gap."""
    px = fmb._px(ticker, date, "open", None)
    if px is not None:
        return float(px)
    if row:
        for key in ("open", "close"):
            val = fm._finite(row.get(key))
            if val is not None:
                return float(val)
    prior = gc.prior_session(cal or [], date) if cal else None
    if prior:
        px = fmb._px(ticker, prior, "close", None)
        if px is not None:
            return float(px)
    bar = tl.session_bar(ticker, date) or {}
    for key in ("open", "close"):
        val = fm._finite(bar.get(key))
        if val is not None:
            return float(val)
    if prior:
        val = fm._finite((tl.session_bar(ticker, prior) or {}).get("close"))
        if val is not None:
            return float(val)
    return None


def build_look_rows(date: str) -> list[dict]:
    """Leak-free 09:30 rows for one session, including an open morning.

    Same candidate buckets as ``build_panel``. Same-day Change% / Gap
    still never attach. Used so Monday paper tickets are not Friday's list.
    """
    fm._SCAN_CACHE.clear()
    payload = sm.load_payload()
    books = sm.list_books()
    from_date = fm.START
    cal = [d for d in sm.session_calendar(payload, books) if d >= from_date]
    if date not in cal and tl.is_trading_date(date):
        cal = sorted(set(cal) | {date})
    if date not in cal:
        return []
    prior = gc.prior_session(cal, date)
    sess_map, _all = fm._session_map(from_date, date)
    if date not in sess_map:
        sess_map[date] = {
            "date": date,
            "prior": sess_map.get(prior),
            "prior_date": prior,
            "join": {}, "finviz": {}, "ab": {}, "peer": {},
            "universe": {}, "has": {}, "quote_colors": {}, "book": {},
        }
    plan = fla.flatten_day_targets(date)
    try:
        movers = fla.collect_mover_buys(payload, cal[0], date, top_n=15)
    except Exception:
        movers = {"by_date": {}}
    buckets = fm._candidates(date, cal, plan, movers.get("by_date") or {})
    reasons: dict[str, list[str]] = {}
    order: list[str] = []
    for key, names in buckets.items():
        for t in names:
            if not t:
                continue
            reasons.setdefault(t, [])
            if key not in reasons[t]:
                reasons[t].append(key)
            if t not in order:
                order.append(t)
    sess = sess_map.get(date)
    prev_sess = sess_map.get(prior) if prior else None
    prior_export = fm.feature_export_date(cal, date)
    from src import gainer_asof as ga
    prior_df = ga.load_finviz(prior_export) if prior_export else None
    day_rows = []
    for i, t in enumerate(order):
        if sess is None:
            continue
        day_rows.append(fm._attach_row(
            date, t, reasons[t], i, sess, prev_sess, prior_export, prior_df,
        ))
    return day_rows


def resolve_rows(date: str, panel: dict | None = None) -> dict:
    """Today's look rows, else last closed panel (stale)."""
    if panel is None:
        if not PANEL_PATH.is_file():
            return {"date": date, "rows": [], "stale": True,
                    "source": "missing", "want_date": date}
        raw = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
        panel = fm.rehydrate_panel(raw)
    by_date = panel.get("by_date") or {}
    if date in by_date and by_date[date]:
        return {"date": date, "rows": by_date[date], "stale": False,
                "source": "panel", "want_date": date,
                "cal": list(panel.get("session_dates") or [])}
    look_err = ""
    try:
        rows = build_look_rows(date)
        if rows:
            cal = list(panel.get("session_dates") or [])
            if date not in cal:
                cal = sorted(set(cal) | {date})
            return {"date": date, "rows": rows, "stale": False,
                    "source": "look", "want_date": date, "cal": cal}
    except Exception as e:  # noqa: BLE001 — live look is best-effort
        look_err = str(e)[:160]
    last = str(panel.get("to_date") or "")
    cal = list(panel.get("session_dates") or [])
    return {
        "date": last or date,
        "rows": by_date.get(last) or [],
        "stale": True,
        "source": "panel_asof",
        "want_date": date,
        "error": look_err,
        "cal": cal,
    }


def member_recs(spec: dict) -> list[dict]:
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    return fmc.recs_for_spec(spec, rec_by)


def size_combo_tickets(rows: list[dict], recs: list[dict],
                       weights: list[float], *, cash: float,
                       held: set[str] | None = None, date: str,
                       s=None, net: str = "priority",
                       cal: list[str] | None = None,
                       combo: str = PAPER_COMBO,
                       hard_red_mode: str = fmc.HARD_RED_SIT,
                       dip_pct: float | None = None,
                       bars=None) -> tuple[list[dict], list[dict]]:
    """One shared leftover pile. Longs BUY, shorts SELL-to-open.

    Skip names already held. Hard-red S≤−3 sits (no new lots) unless a
    research-only ``hard_red_mode`` is passed. Live Webull / flatten
    callers leave the default sit.
    """
    held = {str(t).upper() for t in (held or set())}
    cash = max(float(cash or 0), 0.0)
    ws = fmc._norm_w(weights)
    offer = sorted(recs, key=lambda r: (fmc._claim_rank(r["name"]), r["name"]))
    order = [r["name"] for r in offer]
    hard_red = s is not None and float(s) <= float(fmb.HARD_RED)
    intents = []
    skips: list[dict] = []
    for rec in recs:
        rec_side = rec.get("side") or "long"
        for r in fm.pick_day(rows, rec):
            t = str(r.get("ticker") or "").upper()
            if not t:
                continue
            if hard_red and fmc.hard_red_skip_new(rec_side, hard_red_mode):
                skips.append({
                    "date": date, "ticker": t, "kind": "hard_red",
                    "reason": (
                        f"hard-red S={s} {hard_red_mode}; "
                        f"no new {rec_side} {rec['name']}"
                    ),
                })
                continue
            try:
                from src import ticket_lesson_filter as tlf
                feat = tlf.prior_features(t, date, r)
                dec = tlf.evaluate(
                    rec_side, feat, extra={"hard_red": hard_red, "new_entry": True})
                if dec.get("action") in ("block", "pause"):
                    skips.append({
                        "date": date, "ticker": t, "kind": "lesson_filter",
                        "lesson_id": dec.get("lesson_id"),
                        "filter_id": dec.get("filter_id"),
                        "features_used": dec.get("features_used"),
                        "reason": dec.get("reason") or (
                            f"{t} {rec_side} BLOCKED by {dec.get('lesson_id')}"
                        ),
                    })
                    continue
            except Exception:
                pass
            if t in held:
                skips.append({
                    "date": date, "ticker": t, "kind": "held",
                    "reason": f"already held — skip {rec['name']}",
                })
                continue
            intents.append({
                "ticker": t, "row": r, "rec": rec,
                "side": rec_side,
                "rank": fmc._claim_rank(rec["name"]),
            })
    chosen = fmc._choose_intents(intents, net=net, s=s)
    remaining_w = {rec["name"]: w for rec, w in zip(recs, ws)}
    by_owner: dict[str, list] = {rec["name"]: [] for rec in recs}
    for it in chosen:
        by_owner[it["rec"]["name"]].append(it)
    tickets: list[dict] = []
    leftover = cash
    for rec in offer:
        names_here = by_owner[rec["name"]]
        if not names_here or leftover <= 0:
            continue
        share = remaining_w[rec["name"]]
        rest = sum(remaining_w[n] for n in order
                   if remaining_w[n] > 0 and (
                       by_owner[n] or n == rec["name"]))
        if rest <= 0:
            continue
        room = leftover * (share / rest)
        side = rec.get("side") or "long"
        if side == "short":
            room = min(room, max(0.0, leftover * 0.5))
        else:
            room = min(room, leftover)
        budget_rows = [it["row"] for it in names_here]
        budgets = fmb.split_budgets(
            budget_rows, room, rec.get("size") or "leftover")
        for row, per in zip(budget_rows, budgets):
            t = str(row.get("ticker") or "").upper()
            px = quote_px(t, date, cal=cal, row=row)
            if px is None or px <= 0:
                skips.append({"date": date, "ticker": t, "kind": "no_price",
                              "reason": "no 09:30 open or prior close"})
                continue
            if (hard_red and side == "long"
                    and hard_red_mode in (fmc.HARD_RED_DIP_SCOOP,
                                          fmc.HARD_RED_SHORT_AND_SCOOP)
                    and dip_pct is not None):
                low = fmb._px(t, date, "low", bars)
                scooped, kind = fmc.dip_limit_px(px, low, dip_pct)
                if scooped is None:
                    skips.append({
                        "date": date, "ticker": t, "kind": kind,
                        "reason": (
                            f"hard-red scoop {dip_pct:g}% below "
                            f"open {px:.4f} — {kind}"
                        ),
                    })
                    continue
                px = scooped
            shares = int(per // px)
            if shares < 1:
                skips.append({
                    "date": date, "ticker": t, "kind": "cash",
                    "reason": f"leftover split {per:.2f} < 1 share @ {px:.2f}",
                })
                continue
            notional = shares * px
            if side == "long":
                if notional > leftover + 1e-6:
                    shares = int(leftover // px)
                    if shares < 1:
                        skips.append({
                            "date": date, "ticker": t, "kind": "cash",
                            "reason": f"cash {leftover:.2f} < 1 share @ {px:.2f}",
                        })
                        continue
                    notional = shares * px
                leftover -= notional
                broker_side = "BUY"
            else:
                leftover += notional
                broker_side = "SELL"
            tickets.append({
                "side": broker_side,
                "ticker": t,
                "shares": shares,
                "px": round(px, 4),
                "notional": round(notional, 2),
                "status": "plan",
                "sleeve": rec["name"],
                "kid_side": side,
                "date": date,
                "clock": "09:30 ET",
                "combo": combo,
                "reason": fmb.why_buy(rec, row),
            })
    return tickets, skips


def leftover_for_snap(snap: BrokerSnap | None) -> float:
    """Spend cash when the book has it; else buying power. Never invent."""
    if snap is None:
        return 0.0
    cash = max(float(snap.cash or 0), 0.0)
    power = max(float(getattr(snap, "buying_power", 0) or 0), 0.0)
    if cash > 1:
        return cash
    if power > 1:
        return power
    return cash


def plan_combo_for_broker(date: str, snap: BrokerSnap,
                          combo: str = PAPER_COMBO,
                          panel: dict | None = None) -> dict:
    spec = combo_spec(combo)
    recs = member_recs(spec)
    looked = resolve_rows(date, panel)
    rows = looked.get("rows") or []
    use_date = str(looked.get("date") or date)
    try:
        s = fmb.morning_s(fmb.load_regime(), date)
    except Exception:
        try:
            s = fmb.morning_s(fmb.load_regime(), use_date)
        except Exception:
            s = None
    held = set((snap.positions or {}) if snap else {})
    cash = leftover_for_snap(snap)
    tickets, skips = size_combo_tickets(
        rows, recs, spec["weights"], cash=cash, held=held,
        date=use_date, s=s, net=spec.get("net") or "priority",
        cal=looked.get("cal"), combo=combo,
    )
    would = []
    for rec in recs:
        rec_side = rec.get("side") or "long"
        for r in fm.pick_day(rows, rec):
            t = str(r.get("ticker") or "").upper()
            if not t:
                continue
            try:
                from src import ticket_lesson_filter as tlf
                feat = tlf.prior_features(t, use_date, r)
                dec = tlf.evaluate(
                    rec_side, feat,
                    extra={"hard_red": s is not None and float(s) <= float(fmb.HARD_RED),
                           "new_entry": True})
                if dec.get("action") in ("block", "pause"):
                    skips.append({
                        "date": use_date, "ticker": t, "kind": "lesson_filter",
                        "lesson_id": dec.get("lesson_id"),
                        "reason": dec.get("reason"),
                    })
                    continue
            except Exception:
                pass
            would.append({
                "ticker": t,
                "sleeve": rec["name"],
                "kid_side": rec_side,
                "clock": "09:30 ET",
            })
    stale = bool(looked.get("stale"))
    why = (
        f"{combo} shared 50/50 · short={spec['members'][0]} "
        f"long={spec['members'][1]} · rows via {looked.get('source')}"
    )
    if stale:
        why += (f" · STALE panel {use_date} (wanted {looked.get('want_date')})"
                " — do not submit unless --allow-stale")
    if s is not None and float(s) <= float(fmb.HARD_RED):
        why += f" · hard-red S={s} sit"
    return {
        "date": use_date,
        "want_date": looked.get("want_date") or date,
        "policy": combo,
        "combo": combo,
        "source": looked.get("source"),
        "stale": stale,
        "score": s,
        "hard_red": s is not None and float(s) <= float(fmb.HARD_RED),
        "why": why,
        "tickets": tickets,
        "skipped": skips,
        "would_buy": {"rows": would},
        "flatten_ok": True,
        "look_error": looked.get("error") or "",
    }
