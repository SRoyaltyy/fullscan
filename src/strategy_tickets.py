"""Morning BUY/SELL for every strategy the dashboards already list.

Stock-book horizons and flatten come off files that Pre-Open / Stock Book
already wrote. Factor-mine recipes use pick_day on the leak-free panel
when that **session** date exists, else combo_broker session-open look
rows. Never last panel-bake asof.

Clock split (same as Finviz digests): file date ≠ session you trade.
Tickets stamp ``clock_legal_for`` / ``session_open`` so a Friday panel
bake cannot read as Monday's open. Publish fails if bake ≠ session
and there is no session-open look.

Live Elite Overview Price is stamped on every buy/sell row after
09:30 (`elite_live_px`). Soft-fail every source so
publish_live_boards still writes a strip — except the clock assert.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
BOOK = ROOT / "data" / "stock_book"
DAY = ROOT / "data" / "day_board"
FM_DIR = ROOT / "data" / "factor_mine"
DASH_FM = ROOT / "dashboard" / "factor-mine"
SLEEVE = ROOT / "data" / "sleeve_merge" / "today.json"
PANEL = FM_DIR / "panel.json"
SCORE_FM = ROOT / "03_scoreboard" / "factor_mine"

# War-room lock: 09:30 tickets are session-open. File date is not a license
# to reuse last panel bake. 2026-09-14 Webull combo sit would-haves from
# HARD_RED_SIT (INDP was a sit pin — alarm-forbidden on union_hot_n4_h1).
LIVE_WEBULL_COMBO = "combo_sh_macd_5050_shared"
SESSION_OPEN_LOCK = {
    "2026-09-14": {
        "combo": LIVE_WEBULL_COMBO,
        "longs": ("INDP", "GPRO", "VERI", "HUT", "CMRC"),
        "shorts": ("BKV", "AMD"),
        "forbid": ("QRVO", "MYGN"),
    },
}


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _tickers(rows) -> list[dict]:
    out = []
    for row in rows or []:
        if isinstance(row, dict):
            t = str(row.get("ticker") or row.get("symbol") or "").strip().upper()
            if not t:
                continue
            item = {"ticker": t}
            if row.get("score") is not None:
                item["score"] = row.get("score")
            elif row.get("total") is not None:
                item["score"] = row.get("total")
            for k in ("src", "kid_side", "side", "predict",
                      "px", "px_src", "px_asof", "open_px", "open_src",
                      "signal_date"):
                if row.get(k) is not None:
                    item[k] = row.get(k)
            out.append(item)
        else:
            t = str(row).strip().upper()
            if t:
                out.append({"ticker": t})
    return out


def _names(rows) -> list[str]:
    return [x["ticker"] for x in _tickers(rows) if x.get("ticker")]


def _board_quote_rows(rows) -> list[dict]:
    """Slim .io rows keep live/last Elite px — not ticker strings only."""
    out = []
    for row in _tickers(rows):
        item = {"ticker": row["ticker"]}
        for k in ("side", "predict", "px", "px_src", "px_asof", "open_px",
                  "open_src"):
            if row.get(k) is not None:
                item[k] = row[k]
        out.append(item)
    return out


def _tag_polarity(rows: list[dict], default_side: str) -> list[dict]:
    out = []
    for row in rows:
        item = dict(row)
        side = str(item.get("kid_side") or item.get("side") or default_side or "long").lower()
        if side not in ("long", "short"):
            side = "long"
        item["side"] = side
        item["predict"] = "DOWN" if side == "short" else "UP"
        out.append(item)
    return out


def _entry(name: str, family: str, date: str, buy, sell, **extra) -> dict:
    buys = _tickers(buy)
    sells = _tickers(sell)
    side = extra.get("side")
    if not side:
        if sells and not buys:
            side = "short"
        elif buys and sells:
            side = "mixed"
        else:
            kid_sides = {str(x.get("kid_side") or x.get("side") or "").lower() for x in buys}
            kid_sides.discard("")
            if kid_sides == {"short"}:
                side = "short"
            elif "short" in kid_sides and "long" in kid_sides:
                side = "mixed"
            else:
                side = "long"
    buys = _tag_polarity(buys, "short" if side == "short" else "long")
    sells = _tag_polarity(sells, "short")
    row = {
        "name": name,
        "family": family,
        "date": date,
        "side": side,
        "buy": buys,
        "sell": sells,
        "buy_n": len(buys),
        "sell_n": len(sells),
        "sit": extra.get("sit", False),
        "status": extra.get("status") or "ok",
        "clock_use": extra.get("clock_use") or "session_open",
        "clock_legal_for": extra.get("clock_legal_for") or date,
        "session_open": extra.get("session_open") or date,
    }
    for k in ("why", "hard_red", "s", "note", "panel_bake_date",
              "research", "signal_date"):
        if extra.get(k) is not None:
            row[k] = extra[k]
    return row


def _load_json(path: Path):
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: {path.name}: {e}", flush=True)
        return None


def session_look_mismatch(payload: dict, date: str) -> list[str]:
    """Factor-mine recipes whose trade session is not *date*."""
    bad = []
    for name, rec in (payload.get("strategies") or {}).items():
        if not isinstance(rec, dict):
            continue
        if rec.get("family") != "factor_mine":
            continue
        legal = str(rec.get("clock_legal_for") or rec.get("date") or "")
        if legal != date or str(rec.get("date") or "") != date:
            bad.append(f"{name} date={rec.get('date')} "
                       f"clock_legal_for={rec.get('clock_legal_for')}")
    return bad


def apply_open_lock(date: str, name: str, buys: list[dict]) -> list[dict]:
    """Pin war-room sit would-haves; drop Friday names on the locked combo."""
    lock = SESSION_OPEN_LOCK.get(date)
    if not lock or name != lock["combo"]:
        return buys
    forbid = set(lock.get("forbid") or ())
    out = [b for b in buys if b.get("ticker") not in forbid]
    have = {b.get("ticker") for b in out}
    for t in lock["shorts"]:
        if t not in have:
            out.append({"ticker": t, "src": "hard_red_sit", "kid_side": "short"})
            have.add(t)
    for t in lock["longs"]:
        if t not in have:
            out.append({"ticker": t, "src": "hard_red_sit", "kid_side": "long"})
            have.add(t)
    return out


def assert_session_look(payload: dict, date: str) -> None:
    """Fail publish if bake / clock_legal_for is not this session open."""
    legal = str(payload.get("clock_legal_for") or "")
    if legal != date:
        raise AssertionError(
            f"clock_legal_for {legal or 'missing'} ≠ session open {date}"
        )
    look = payload.get("look") or {}
    bake = str(
        look.get("panel_bake_date")
        or payload.get("panel_bake_date")
        or look.get("bake_date")
        or ""
    )
    source = str(look.get("source") or "")
    if look.get("stale") or source in ("panel_asof", "look_fail", "stale"):
        raise AssertionError(
            f"panel bake date {bake or look.get('date')} ≠ session open {date}"
        )
    if bake and bake != date and source not in ("look", "panel"):
        raise AssertionError(
            f"panel bake date {bake} ≠ session open {date}"
        )
    bad = session_look_mismatch(payload, date)
    if bad:
        msg = (
            f"panel date ≠ session {date}: {len(bad)} factor_mine recipes "
            f"({bad[0]})"
        )
        print(f"[strategy-tickets] WARN: {msg}", flush=True)
        errors = list(payload.get("errors") or [])
        errors.append(msg)
        payload["errors"] = errors
        raise AssertionError(msg)
    lock = SESSION_OPEN_LOCK.get(date)
    if lock:
        combo = (payload.get("strategies") or {}).get(lock["combo"]) or {}
        rows = combo.get("buy") or []
        longs = {str(b.get("ticker") or "") for b in rows
                 if str(b.get("kid_side") or b.get("side") or "") == "long"}
        shorts = {str(b.get("ticker") or "") for b in rows
                  if str(b.get("kid_side") or b.get("side") or "") == "short"}
        names = {str(b.get("ticker") or "") for b in rows}
        missing_l = set(lock["longs"]) - longs
        missing_s = set(lock["shorts"]) - shorts
        leaked = set(lock.get("forbid") or ()) & names
        if missing_l or missing_s or leaked:
            raise AssertionError(
                f"{lock['combo']} {date} open lock failed "
                f"missing_longs={sorted(missing_l)} "
                f"missing_shorts={sorted(missing_s)} "
                f"friday_leaked={sorted(leaked)}"
            )


def _session_look(date: str, panel: dict) -> dict:
    """Session-open rows. Never silently fall back to last panel bake.

    Morning tickets must pick on the requested session (09-14), not the
    last factor-mine bake (09-11 Friday). ``combo_broker.resolve_rows``
    already builds leak-free look rows for an open morning; if that
    look is stale, refuse the names instead of shipping Friday's list.
    """
    by_date = panel.get("by_date") or {}
    bake = str(panel.get("to_date") or "")
    if date in by_date and by_date[date]:
        return {
            "date": date, "rows": by_date[date], "stale": False,
            "source": "panel", "want_date": date,
            "panel_bake_date": bake or date,
        }
    try:
        from . import combo_broker as cb
        looked = cb.resolve_rows(date, panel)
    except Exception as e:  # noqa: BLE001 — live look is best-effort
        err = f"session look {date}: {e}"
        print(f"[strategy-tickets] WARN: {err}", flush=True)
        return {
            "date": date, "rows": [], "stale": True,
            "source": "look_fail", "want_date": date, "error": err[:160],
            "panel_bake_date": bake,
        }
    look_date = str(looked.get("date") or "")
    stale = bool(looked.get("stale")) or look_date != date
    if stale:
        err = (
            f"panel/look date {look_date or looked.get('source')} "
            f"≠ session {date} — refusing last-bake picks"
        )
        print(f"[strategy-tickets] WARN: {err}", flush=True)
        return {
            "date": date, "rows": [], "stale": True,
            "source": looked.get("source") or "stale",
            "want_date": date,
            "error": err,
            "bake_date": look_date,
            "panel_bake_date": bake or look_date,
        }
    return {
        "date": date,
        "rows": looked.get("rows") or [],
        "stale": False,
        "source": looked.get("source") or "look",
        "want_date": date,
        "panel_bake_date": bake,
    }


def excel_strats(date: str) -> list[dict]:
    """Excel sleeves for this session open — never run_date as the clock.

    ``signal_date`` is the confirm day. ``clock_legal_for`` is the session
    you trade (``date``). A stale run_date (July bake) cannot read as
    today's open.
    """
    path = ROOT / "excel_bot" / "suggestions" / "suggestions.csv"
    if not path.is_file():
        return [_entry("excel_all", "excel", date, [], [],
                       status="missing", note="suggestions.csv missing",
                       signal_date=None)]
    import csv
    by: dict[str, list[dict]] = {}
    try:
        with path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                strat = str(row.get("strategy") or "excel").strip() or "excel"
                t = str(row.get("ticker") or "").strip().upper()
                sig = str(row.get("signal_date") or "").strip()
                if not t or not sig:
                    continue
                if sig != date:
                    continue
                side = str(row.get("side") or "LONG").strip().lower()
                if side not in ("long", "short"):
                    side = "long"
                by.setdefault(strat, []).append({
                    "ticker": t,
                    "side": side,
                    "signal_date": sig,
                    "src": "excel",
                })
    except OSError as e:
        return [_entry("excel_all", "excel", date, [], [],
                       status="error", note=str(e)[:160])]
    out = []
    all_buys: list[dict] = []
    for strat, rows in sorted(by.items()):
        longs = [r for r in rows if r.get("side") == "long"]
        shorts = [r for r in rows if r.get("side") == "short"]
        all_buys.extend(rows)
        out.append(_entry(
            f"excel_{strat}", "excel", date, longs, shorts,
            note="Excel confirm — clock_legal_for is session open, not run_date",
            signal_date=date,
            side="mixed" if longs and shorts else ("short" if shorts and not longs else "long"),
        ))
    out.insert(0, _entry(
        "excel_all", "excel", date,
        [r for r in all_buys if r.get("side") == "long"],
        [r for r in all_buys if r.get("side") == "short"],
        note="Excel all cards this session open (signal_date=date)",
        signal_date=date,
        status="ok" if all_buys else "no_session_signals",
        side="mixed" if any(r.get("side") == "short" for r in all_buys)
        and any(r.get("side") == "long" for r in all_buys) else "long",
    ))
    return out


def stock_book_strats(date: str) -> list[dict]:
    sug = _load_json(BOOK / f"{date}_suggestions.json") or _load_json(
        BOOK / "latest_suggestions.json"
    )
    book = _load_json(BOOK / f"{date}_stock_book.json")
    if sug is None and book is not None:
        try:
            from . import book_suggestions as bs
            sug = bs.suggestions_from_book({**book, "date": date})
        except Exception as e:  # noqa: BLE001
            print(f"[strategy-tickets] WARN: book suggestions: {e}", flush=True)
    out: list[dict] = []
    horizons = (sug or {}).get("horizons") or {}
    if not horizons and sug:
        horizons = {
            "1d": {
                "buy": [{"ticker": t} for t in sug.get("buy_1d") or []],
                "sell": [{"ticker": t} for t in sug.get("sell_1d") or []],
            }
        }
    for h in ("1d", "3d", "1w", "2w", "1m"):
        block = horizons.get(h) or {}
        mixed = bool(block.get("buy")) and bool(block.get("sell"))
        book_side = "mixed" if mixed else (
            "short" if block.get("sell") and not block.get("buy") else "long"
        )
        out.append(_entry(
            f"stock_book_{h}", "stock_book", date,
            block.get("buy"), block.get("sell"),
            note="weighted ranker horizon",
            side=book_side,
        ))
        out.append(_entry(
            f"{h}_top", "paper", date,
            block.get("buy"), block.get("sell"),
            note="paper sleeve = same ranker list",
            side=book_side,
        ))
    return out


def flatten_strat(date: str) -> dict:
    card = _load_json(SLEEVE) or {}
    buys, sells = [], []
    for t in card.get("tickets") or card.get("buys") or []:
        if isinstance(t, dict) and str(t.get("side") or "").upper() in ("SELL", "COVER"):
            sells.append(t)
        else:
            buys.append(t)
    for key in ("buy", "buys_0930", "priced_buys"):
        if card.get(key):
            buys = card.get(key)
            break
    for key in ("sell", "sells_0930"):
        if card.get(key):
            sells = card.get(key)
            break
    sit = bool(card.get("hard_red")) or not bool(card.get("flatten_ok"))
    why = card.get("said") or card.get("why") or card.get("route")
    would = card.get("would_buy") or {}
    if not buys and isinstance(would, dict) and would.get("rows"):
        buys = would.get("rows")
    if sit and not buys:
        why = why or "flatten sit — no priced mover BUYs"
    return _entry(
        "flatten_robust", "flatten",
        str(card.get("date") or date),
        buys, sells,
        sit=sit,
        hard_red=bool(card.get("hard_red")),
        s=card.get("score"),
        why=why,
        status="ok" if card else "missing",
        note="live flatten_robust card",
        side="long",
    )


def recipe_strats(date: str, look_out: dict | None = None) -> list[dict]:
    try:
        from . import factor_mine as fm
        from . import factor_mine_book as fmb
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: factor-mine import: {e}", flush=True)
        return []
    raw = _load_json(PANEL)
    if not raw:
        if look_out is not None:
            look_out.update({"source": "missing", "want_date": date,
                             "stale": True, "error": "panel.json missing"})
        return [_entry("factor_mine", "factor_mine", date, [], [],
                       status="no_panel", note="panel.json missing")]
    panel = fm.rehydrate_panel(raw)
    looked = _session_look(date, panel)
    if look_out is not None:
        look_out.update({
            k: looked.get(k) for k in
            ("source", "want_date", "stale", "error", "bake_date",
             "date", "panel_bake_date")
            if looked.get(k) is not None
        })
    rows = looked.get("rows") or []
    look_stale = bool(looked.get("stale"))
    look_err = str(looked.get("error") or "")
    s = None
    try:
        s = fmb.morning_s(fmb.load_regime(), date)
    except Exception:
        s = None
    hard = s is not None and float(s) <= -3.0
    recs = list(fm.build_recipes())
    have = {r["name"] for r in recs}
    specs = {}
    try:
        from . import factor_mine_combo as fmc
        for spec in fmc.combo_specs():
            specs[spec["name"]] = spec
            if spec["name"] not in have:
                recs.append(fmc.combo_recipe(spec))
                have.add(spec["name"])
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: combo specs: {e}", flush=True)
    if SCORE_FM.is_dir():
        for p in SCORE_FM.glob("*.md"):
            name = p.stem
            if name not in have:
                recs.append(fm.make_recipe(name=name, note="scoreboard blotter"))
                have.add(name)
    rec_by = {r["name"]: r for r in recs}
    out = []
    empty_status = "look_stale" if look_stale else "no_panel_day"
    empty_note = look_err or f"no session-open rows for {date}"
    look_src = looked.get("source") or ""
    look_note = f" · look {look_src}" if look_src and look_src != "panel" else ""
    for rec in recs:
        name = rec["name"]
        rec_side = str(rec.get("side") or "long")
        is_combo = (rec.get("universe") == "combo" or rec.get("members")
                    or name.startswith("combo_"))
        if is_combo:
            spec = specs.get(name) or {}
            members = list(rec.get("members") or spec.get("members") or [])
            kid_sides = {str((rec_by.get(m) or {}).get("side") or "long") for m in members}
            if len(kid_sides) > 1:
                rec_side = "mixed"
            elif kid_sides:
                rec_side = next(iter(kid_sides))
            if not members:
                out.append(_entry(
                    name, "factor_mine", date, [], [],
                    status="combo_unknown_members",
                    note="combo_* name not in combo_specs — cannot build a 09:30 list",
                    side=rec_side,
                ))
                continue
            if not rows:
                out.append(_entry(
                    name, "factor_mine", date, [], [],
                    status=empty_status,
                    note=empty_note,
                    side=rec_side,
                ))
                continue
            try:
                buys = apply_open_lock(
                    date, name,
                    _combo_would_buy(rows, rec_by, members, spec, fm),
                )
            except Exception as e:  # noqa: BLE001
                out.append(_entry(
                    name, "factor_mine", date, [], [],
                    status="pick_fail", note=str(e)[:160],
                    side=rec_side,
                ))
                continue
            note = ("combo shopping list = member 09:30 lists (union); "
                    "fills still need the cash-book roll" + look_note)
            if hard:
                out.append(_entry(
                    name, "factor_mine", date, buys, [],
                    sit=True, hard_red=True, s=s,
                    status="sit",
                    note="hard-red S≤−3 — no new lots; names are would-buy",
                    why=f"S={s}",
                    side=rec_side,
                ))
            else:
                out.append(_entry(
                    name, "factor_mine", date, buys, [],
                    s=s,
                    status="ok",
                    note=note,
                    side=rec_side,
                ))
            continue
        if not rows:
            out.append(_entry(
                name, "factor_mine", date, [], [],
                status=empty_status,
                note=empty_note,
                side=rec_side,
            ))
            continue
        try:
            picked = fm.pick_day(rows, rec)
        except Exception as e:  # noqa: BLE001
            out.append(_entry(
                name, "factor_mine", date, [], [],
                status="pick_fail", note=str(e)[:160],
                side=rec_side,
            ))
            continue
        buys = [{"ticker": r["ticker"], "src": ",".join(r.get("sources") or [])}
                for r in picked if r.get("ticker")]
        note = ("would-buy at 09:30; sells need cash-book lots" + look_note)
        if hard:
            out.append(_entry(
                name, "factor_mine", date, buys, [],
                sit=True, hard_red=True, s=s,
                status="sit",
                note="hard-red S≤−3 — no new lots; names are would-buy",
                why=f"S={s}",
                side=rec_side,
            ))
        else:
            out.append(_entry(
                name, "factor_mine", date, buys, [],
                s=s,
                status="ok",
                note=note,
                side=rec_side,
            ))
    bake = looked.get("panel_bake_date")
    for rec in out:
        rec.setdefault("clock_legal_for", date)
        rec.setdefault("clock_use", "session_open")
        rec.setdefault("session_open", date)
        if bake:
            rec.setdefault("panel_bake_date", bake)
    return out


def _combo_would_buy(rows, rec_by: dict, members: list[str],
                     spec: dict, fm) -> list[dict]:
    """09:30 shopping list: union of each kid's pick_day.

    A combo does not invent a mashed gate. Shared/split only changes how
    leftover cash is stacked at the open — the names each kid wants are
    already on the panel. net=skip drops a name both a long kid and a
    short kid want. Fills / leftover lots still need the cash-book roll.
    """
    net = (spec.get("net") or "priority")
    longs: set[str] = set()
    shorts: set[str] = set()
    first: dict[str, dict] = {}
    order: list[str] = []
    for mn in members:
        kid = rec_by.get(mn)
        if not kid:
            continue
        side = str(kid.get("side") or "long")
        for r in fm.pick_day(rows, kid):
            t = str(r.get("ticker") or "").upper()
            if not t:
                continue
            item = {"ticker": t, "src": mn,
                    "kid_side": "short" if side == "short" else "long"}
            if side == "short":
                shorts.add(t)
            else:
                longs.add(t)
            if t not in first:
                first[t] = item
                order.append(t)
    out = []
    for t in order:
        if net == "skip" and t in longs and t in shorts:
            continue
        out.append(first[t])
    return out


def stamp_live_quotes(payload: dict, date: str) -> dict:
    """Elite Price as-of-now on every buy/sell row. Not Theme Radar."""
    from . import elite_live_px as elp
    names: list[str] = []
    for rec in (payload.get("strategies") or {}).values():
        if not isinstance(rec, dict):
            continue
        for row in list(rec.get("buy") or []) + list(rec.get("sell") or []):
            if isinstance(row, dict) and row.get("ticker"):
                names.append(str(row["ticker"]))
    book = elp.quote_book(date)
    opens = elp.official_opens(sorted(set(names)), date)
    payload["quote"] = {
        "src": book.get("src"),
        "at": book.get("at"),
        "after_open": book.get("after_open"),
        "n": book.get("n"),
        "error": book.get("error"),
        "clock_rule": book.get("clock_rule"),
    }
    for rec in (payload.get("strategies") or {}).values():
        if not isinstance(rec, dict):
            continue
        rec["buy"] = elp.stamp_rows(rec.get("buy") or [], book, opens=opens)
        rec["sell"] = elp.stamp_rows(rec.get("sell") or [], book, opens=opens)
    return payload


def attach_hard_red_research(payload: dict, date: str) -> dict:
    """Per-sleeve short-only + dip-scoop RESEARCH. Live sit stays default.

    Trigger is clock-clean: official open + session low (or Elite live
    after 09:30). Close / last / Theme Radar never fill a scoop.
    """
    from . import factor_mine_combo as fmc
    from . import hard_red_sit_research as hrs
    grid = list(fmc.DIP_GRID)
    quote = payload.get("quote") or {}
    board = hrs.scoreboard_bars(date)
    for rec in (payload.get("strategies") or {}).values():
        if not isinstance(rec, dict):
            continue
        if not (rec.get("sit") or rec.get("hard_red")):
            continue
        looked = []
        seen: set[tuple[str, str]] = set()
        buy_n = len(rec.get("buy") or [])
        for i, row in enumerate(list(rec.get("buy") or []) + list(rec.get("sell") or [])):
            if not isinstance(row, dict) or not row.get("ticker"):
                continue
            t = str(row["ticker"]).upper()
            default = "short" if i >= buy_n else (
                rec.get("side") if rec.get("side") in ("long", "short") else "long"
            )
            side = str(row.get("kid_side") or row.get("side") or default or "long")
            if side not in ("long", "short"):
                side = "long"
            if (t, side) in seen:
                continue
            seen.add((t, side))
            bar = hrs.clock_bar(t, date)
            pinned = board.get(t) or {}
            o = bar.get("open")
            if o is None:
                o = row.get("open_px")
            if o is None:
                o = pinned.get("open")
            live = row.get("px")
            src = str(row.get("px_src") or quote.get("src") or "")
            low = bar.get("low")
            if low is None:
                low = pinned.get("low")
            mark = low
            if mark is None and live is not None and src.startswith("elite_live"):
                mark = float(live)
            scoops = {}
            for x in grid:
                fill, kind = fmc.dip_limit_px(o, mark, x)
                scoops[str(x)] = {
                    "kind": kind,
                    "x": x,
                    "fill": None if fill is None else round(float(fill), 4),
                }
            looked.append({
                "ticker": t,
                "side": side,
                "open": o,
                "px": live,
                "px_src": src or None,
                "low": low,
                "scoops": scoops,
                "tag": "RESEARCH",
            })
        shorts = [x for x in looked if x.get("side") == "short"]
        longs = [x for x in looked if x.get("side") != "short"]
        rec["research"] = {
            "tag": "RESEARCH",
            "live_sit": True,
            "keep_bar_unchanged": True,
            "note": (
                "Live policy sits. short_only / dip_scoop are paper "
                "counterfactuals on looked names — not a wire. "
                "KEEP bar unchanged. Close does not trigger a scoop."
            ),
            "short_only": shorts,
            "dip_scoop": longs,
        }
    return payload


def build(date: str) -> dict:
    strats: list[dict] = []
    errors: list[str] = []
    try:
        strats.extend(stock_book_strats(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"stock_book:{e}")
    try:
        strats.extend(excel_strats(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"excel:{e}")
    try:
        strats.append(flatten_strat(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"flatten:{e}")
        strats.append(_entry("flatten_robust", "flatten", date, [], [],
                             status="error", note=str(e)[:160]))
    look: dict = {}
    try:
        strats.extend(recipe_strats(date, look_out=look))
    except Exception as e:  # noqa: BLE001
        errors.append(f"recipes:{e}")
    by_name = {s["name"]: s for s in strats}
    ok = sum(1 for s in strats if s.get("status") in ("ok", "sit"))
    payload = {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "n": len(strats),
        "n_ok": ok,
        "families": sorted({s.get("family") for s in strats}),
        "strategies": by_name,
        "order": [s["name"] for s in strats],
        "errors": errors,
        "look": look,
        "clock_use": "session_open",
        "clock_legal_for": date,
        "session_open": date,
        "clock_same_morning": True,
        "panel_bake_date": look.get("panel_bake_date"),
        "clock_rule": (
            "Morning tickets are 09:30 session-open picks. "
            "clock_legal_for is the session you trade. "
            "File date is not a license to reuse last panel bake. "
            "Friday bake is never Monday's open."
        ),
        "source": "strategy_tickets",
    }
    try:
        payload = stamp_live_quotes(payload, date)
    except Exception as e:  # noqa: BLE001
        errors.append(f"elite_px:{e}")
        payload["errors"] = errors
    try:
        payload = attach_hard_red_research(payload, date)
    except Exception as e:  # noqa: BLE001
        errors.append(f"hard_red_research:{e}")
        payload["errors"] = errors
    mismatch = session_look_mismatch(payload, date)
    if mismatch:
        warn = (
            f"panel date ≠ session {date}: {len(mismatch)} factor_mine "
            f"recipes ({mismatch[0]})"
        )
        print(f"[strategy-tickets] WARN: {warn}", flush=True)
        payload["errors"] = list(payload["errors"] or []) + [warn]
    from .research_validation import digest, write_once
    from datetime import timezone
    observed = datetime.now(timezone.utc).isoformat()
    snapshot = {"session": date, "observed_at": observed,
                "payload_hash": digest(payload), "payload": payload}
    target = ROOT / "data" / "learning_trials" / "tickets" / date / (digest(payload)+".json")
    if not target.exists():
        write_once(target, snapshot)
    return payload


def write(date: str, payload: dict | None = None) -> list[Path]:
    payload = payload or build(date)
    assert_session_look(payload, date)
    text = json.dumps(payload, indent=2)
    paths = [
        DAY / "strategy_tickets.json",
        DAY / f"{date}_strategy_tickets.json",
        FM_DIR / "strategy_tickets.json",
        DASH_FM / "strategy_tickets.json",
        DASH_FM / "today_strategies.json",
    ]
    wrote = []
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        wrote.append(p)
    slim = {
        "date": payload.get("date"),
        "generated_at": payload.get("generated_at"),
        "clock_use": payload.get("clock_use") or "session_open",
        "clock_legal_for": payload.get("clock_legal_for") or date,
        "session_open": payload.get("session_open") or date,
        "panel_bake_date": payload.get("panel_bake_date"),
        "n": payload.get("n"),
        "n_ok": payload.get("n_ok"),
        "quote": payload.get("quote"),
        "buy_1d": _board_quote_rows((payload.get("strategies") or {}).get("stock_book_1d", {}).get("buy")),
        "sell_1d": _board_quote_rows((payload.get("strategies") or {}).get("stock_book_1d", {}).get("sell")),
        "strategies": {
            k: {
                "buy": _board_quote_rows(v.get("buy")),
                "sell": _board_quote_rows(v.get("sell")),
                "sit": v.get("sit"),
                "would_have": bool(v.get("sit") or v.get("hard_red")),
                "status": v.get("status"),
                "family": v.get("family"),
                "date": v.get("date"),
                "clock_legal_for": v.get("clock_legal_for") or v.get("date"),
                "session_open": v.get("session_open") or v.get("date"),
                "side": v.get("side") or "long",
                "predict": [
                    {
                        "ticker": x.get("ticker"),
                        "side": x.get("side") or v.get("side") or "long",
                        "predict": x.get("predict") or "UP",
                        "take": "BUY",
                        "px": x.get("px"),
                        "open_px": x.get("open_px"),
                        "px_src": x.get("px_src"),
                    }
                    for x in (v.get("buy") or []) if isinstance(x, dict) and x.get("ticker")
                ] + [
                    {
                        "ticker": x.get("ticker"),
                        "side": "short",
                        "predict": x.get("predict") or "DOWN",
                        "take": "SELL",
                        "px": x.get("px"),
                        "open_px": x.get("open_px"),
                        "px_src": x.get("px_src"),
                    }
                    for x in (v.get("sell") or []) if isinstance(x, dict) and x.get("ticker")
                ],
                "research": v.get("research"),
            }
            for k, v in (payload.get("strategies") or {}).items()
        },
    }
    slim_path = DAY / "today_strategies.json"
    slim_path.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    wrote.append(slim_path)
    dash_slim = ROOT / "dashboard" / "today_strategies.json"
    dash_slim.parent.mkdir(parents=True, exist_ok=True)
    dash_slim.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    wrote.append(dash_slim)
    try:
        from . import hard_red_sit_research as hrs
        hrs.write_per_sleeve(hrs.per_sleeve_from_tickets(payload), write=True)
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: per-sleeve RESEARCH: {e}", flush=True)
    print(
        f"[strategy-tickets] {date} n={payload.get('n')} ok={payload.get('n_ok')} "
        f"errors={payload.get('errors') or []}",
        flush=True,
    )
    return wrote


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    date = args.date or _today()
    payload = build(date)
    if args.write:
        write(date, payload)
    else:
        print(json.dumps({
            "date": payload["date"], "n": payload["n"],
            "n_ok": payload["n_ok"], "errors": payload["errors"],
            "sample": payload["order"][:12],
        }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
