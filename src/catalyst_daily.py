"""Bounded daily catalyst dossiers into actions and the stock book."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
OUT_DIR = ROOT / "01_daily" / "catalyst"
NEWS_DIR = ROOT / "01_daily" / "news"
HEAT_DIR = ROOT / "01_daily" / "map_heat"
DATA_CATALYST = ROOT / "data" / "catalyst"
DEFAULT_MAX = 8
MEGA = {
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "GOOGL", "META", "TSLA",
    "AVGO", "CRM", "ORCL", "NFLX", "AMD",
}
SIGNAL_WEIGHT = {
    "strong bullish": 3.0,
    "bullish": 1.8,
    "neutral": 0.0,
    "bearish": -1.8,
    "strong bearish": -3.0,
}


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _norm_ticker(raw: object) -> str:
    t = str(raw or "").strip().upper()
    if not t or t in {"SPY", "QQQ", "DIA", "IWM", "XLE", "XLY", "XLK",
                      "XLF", "XLV", "XLI", "XLB", "XLU", "XLRE", "XLC"}:
        return ""
    if not t.isalpha() or len(t) > 5:
        return ""
    return t


def signal_weight(net_signal: str, conviction: object) -> float:
    key = str(net_signal or "").strip().lower()
    base = SIGNAL_WEIGHT.get(key, 0.0)
    try:
        conv = float(conviction)
    except (TypeError, ValueError):
        conv = 50.0
    scale = max(0.25, min(1.0, conv / 100.0))
    return round(base * scale, 3)


def _add(picked: list[dict], seen: set[str], ticker: str, role: str,
         why: str, max_n: int) -> None:
    t = _norm_ticker(ticker)
    if not t or t in seen or len(picked) >= max_n:
        return
    seen.add(t)
    picked.append({"ticker": t, "role": role, "why": why})


def select_targets(date: str, max_n: int = DEFAULT_MAX,
                   extra: list[str] | None = None) -> list[dict]:
    picked: list[dict] = []
    seen: set[str] = set()
    extra = extra or []
    for t in extra:
        _add(picked, seen, t, "manual", "CLI --tickers", max_n)
    heat = _load_json(HEAT_DIR / f"{date}_map_heat.json")
    research = _load_json(HEAT_DIR / f"{date}_research.json")
    actions = _load_json(NEWS_DIR / f"{date}_actions.json")
    for row in heat.get("overrides") or []:
        industry = row.get("industry") or ""
        for cap in (row.get("spx_leaders") or []) + (row.get("rut_leaders") or []):
            _add(picked, seen, cap.get("ticker") if isinstance(cap, dict) else cap,
                 "override_captain", f"OVERRIDE {industry}", max_n)
    for card in research.get("cards") or []:
        if str(card.get("action") or "") != "OVERRIDE":
            continue
        industry = card.get("industry") or ""
        for cap in card.get("captains") or []:
            _add(picked, seen, cap.get("ticker") if isinstance(cap, dict) else cap,
                 "override_captain", f"OVERRIDE card {industry}", max_n)
    for opp in research.get("opportunities") or []:
        side = str(opp.get("side") or "")
        why = str(opp.get("why") or opp.get("id") or "opportunity")[:120]
        for t in opp.get("tickers") or []:
            _add(picked, seen, t, "opportunity", f"{side} {why}", max_n)
    earns = research.get("earnings") or heat.get("earnings") or []
    for e in earns:
        t = e.get("ticker") if isinstance(e, dict) else e
        t = _norm_ticker(t)
        if t in MEGA:
            sess = e.get("session") if isinstance(e, dict) else ""
            _add(picked, seen, t, "earnings", f"earnings {sess or 'today'}", max_n)
    heat_side: dict[str, str] = {}
    for card in research.get("cards") or []:
        direction = str(card.get("subsector_dir") or "").lower()
        for cap in card.get("captains") or []:
            if not isinstance(cap, dict):
                continue
            t = _norm_ticker(cap.get("ticker"))
            sent = str(cap.get("sent") or "").lower()
            if sent == "neg" or direction == "down":
                heat_side[t] = "sell"
            elif sent == "pos" or direction == "up":
                heat_side[t] = "buy"
    action_rows = actions.get("ticker_actions") or []
    if isinstance(action_rows, dict):
        action_rows = [{"ticker": k, **(v if isinstance(v, dict) else {})}
                       for k, v in action_rows.items()]
    ranked_actions = []
    for rec in action_rows:
        if not isinstance(rec, dict):
            continue
        t = _norm_ticker(rec.get("ticker"))
        if not t:
            continue
        try:
            net = float(rec.get("net") or 0)
        except (TypeError, ValueError):
            net = 0.0
        side = str(rec.get("side") or ("buy" if net > 0 else "sell" if net < 0 else "flat"))
        ranked_actions.append((abs(net), t, side, net))
    ranked_actions.sort(reverse=True)
    for _absn, t, side, net in ranked_actions:
        hs = heat_side.get(t)
        if hs and hs != side and side in ("buy", "sell"):
            _add(picked, seen, t, "conflict",
                 f"actions {side} net={net} vs heat {hs}", max_n)
    for _absn, t, side, net in ranked_actions:
        if _absn < 1.0:
            continue
        _add(picked, seen, t, "action_top",
             f"news_actions {side} net={net}", max_n)
    return picked


def already_good(date: str) -> bool:
    js = OUT_DIR / f"{date}_dossiers.json"
    if not js.exists():
        return False
    data = _load_json(js)
    rows = data.get("dossiers") or []
    ok = [r for r in rows if isinstance(r, dict) and r.get("net_signal") and not r.get("error")]
    return len(ok) >= 2


def load_dossiers(date: str) -> list[dict]:
    data = _load_json(OUT_DIR / f"{date}_dossiers.json")
    rows = data.get("dossiers") or []
    return [r for r in rows if isinstance(r, dict) and r.get("ticker")]


def ticker_boosts(date: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in load_dossiers(date):
        t = _norm_ticker(row.get("ticker"))
        if not t or row.get("error"):
            continue
        w = signal_weight(row.get("net_signal") or "", row.get("conviction"))
        if w:
            out[t] = w
    return out


def apply_to_actions(date: str, dossiers: list[dict] | None = None) -> dict:
    path = NEWS_DIR / f"{date}_actions.json"
    report = _load_json(path)
    if not report:
        return {}
    dossiers = dossiers if dossiers is not None else load_dossiers(date)
    if not dossiers:
        return report
    book: dict[str, dict] = {}
    existing = report.get("ticker_actions") or []
    if isinstance(existing, dict):
        existing = [{"ticker": k, **(v if isinstance(v, dict) else {})}
                    for k, v in existing.items()]
    for rec in existing:
        if isinstance(rec, dict) and rec.get("ticker"):
            book[str(rec["ticker"]).upper()] = rec
    applied = 0
    for row in dossiers:
        t = _norm_ticker(row.get("ticker"))
        if not t or row.get("error"):
            continue
        w = signal_weight(row.get("net_signal") or "", row.get("conviction"))
        if not w:
            continue
        rec = book.setdefault(t, {
            "ticker": t, "buy_score": 0.0, "sell_score": 0.0,
            "events": [], "net": 0.0, "side": "flat",
        })
        if w > 0:
            rec["buy_score"] = round(float(rec.get("buy_score") or 0) + w, 2)
        else:
            rec["sell_score"] = round(float(rec.get("sell_score") or 0) + abs(w), 2)
        rec["net"] = round(float(rec.get("buy_score") or 0) - float(rec.get("sell_score") or 0), 2)
        rec["side"] = "buy" if rec["net"] > 0 else ("sell" if rec["net"] < 0 else "flat")
        rec.setdefault("events", []).append({
            "event": "catalyst", "side": rec["side"], "weight": round(abs(w), 2),
            "bucket": row.get("role") or "catalyst",
            "net_signal": row.get("net_signal"), "conviction": row.get("conviction"),
        })
        rec["catalyst_signal"] = row.get("net_signal")
        rec["catalyst_conviction"] = row.get("conviction")
        rec["catalyst_stack"] = str(row.get("catalyst_stack") or "")[:280]
        applied += 1
    report["ticker_actions"] = sorted(book.values(), key=lambda x: -abs(float(x.get("net") or 0)))
    report["catalyst_tickers"] = applied
    report["catalyst_date"] = date
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    md_path = NEWS_DIR / f"{date}_actions.md"
    if md_path.exists():
        extra = render_actions_appendix(dossiers)
        text = md_path.read_text(encoding="utf-8")
        marker = "## Catalyst dossiers"
        if marker in text:
            text = text.split(marker)[0].rstrip() + "\n\n" + extra
        else:
            text = text.rstrip() + "\n\n" + extra
        md_path.write_text(text, encoding="utf-8")
    print(f"[catalyst_daily] merged {applied} dossiers into {path}")
    return report


def render_actions_appendix(dossiers: list[dict]) -> str:
    lines = ["## Catalyst dossiers", ""]
    if not dossiers:
        lines.append("_none_")
        return "\n".join(lines) + "\n"
    for row in dossiers:
        flag = "ERR" if row.get("error") else str(row.get("net_signal") or "?")
        lines.append(
            f"- **{row.get('ticker')}** ({row.get('role')}) "
            f"{flag} conv={row.get('conviction', '?')} — {(row.get('why') or '')[:120]}"
        )
        stack = str(row.get("catalyst_stack") or "").strip()
        if stack:
            lines.append(f"  _{stack[:280]}_")
    return "\n".join(lines) + "\n"


def render(payload: dict) -> str:
    lines = [
        f"# CATALYST DAILY — {payload.get('date')}", "",
        f"{payload.get('n_ok', 0)}/{payload.get('n_targets', 0)} dossiers "
        f"· max={payload.get('max_n')} · grok={payload.get('grok', True)}", "", "## TARGETS",
    ]
    for t in payload.get("targets") or []:
        lines.append(f"- **{t.get('ticker')}** [{t.get('role')}] {t.get('why')}")
    lines += ["", "## DOSSIERS"]
    for row in payload.get("dossiers") or []:
        if row.get("error"):
            lines.append(f"- **{row.get('ticker')}** ERROR {row.get('error')}")
            continue
        lines.append(
            f"- **{row.get('ticker')}** {row.get('net_signal')} "
            f"conv={row.get('conviction')} +{row.get('n_pos') or 0}/-{row.get('n_neg') or 0} "
            f"[{row.get('role')}]"
        )
        if row.get("catalyst_stack"):
            lines.append(f"  {row['catalyst_stack']}")
        for h in (row.get("top_hits") or [])[:4]:
            lines.append(
                f"  - {h.get('type')} {h.get('taxonomy')} "
                f"{h.get('event_date')}: {str(h.get('headline') or '')[:90]}"
            )
    lines += ["", "CATALYST_DAILY_OK", ""]
    return "\n".join(lines)


def _reuse_saved(ticker: str, date: str) -> dict | None:
    path = DATA_CATALYST / f"{ticker}_{date}.json"
    if not path.exists():
        return None
    data = _load_json(path)
    return data if data.get("net_signal") else None


def _prepare_engine(skip_gemini: bool) -> object:
    from collectors import catalyst_analysis as ca
    from collectors.catalyst_grok_runtime import install as install_grok_search
    if not config.openclaw_enabled():
        raise SystemExit("GROK_ONLY: set OPENCLAW_GATEWAY_URL")
    print("[catalyst_daily] Grok-only — OpenClaw / Grok 4.6 native search")
    install_grok_search(ca)
    if skip_gemini:
        print("[catalyst_daily] note: --gemini/--skip no longer applies; "
              "verdict+catcher are Grok")
    return ca


def _snapshot(ticker: str) -> dict:
    snap = {"profile": {"company_name": ticker, "sector": "", "industry": "",
                        "country": "", "description": ""}, "finviz": {}}
    if not config.DATABASE_URL:
        return snap
    try:
        import psycopg2
        from collectors.catalyst_analysis import build_health_snapshot
        conn = psycopg2.connect(config.DATABASE_URL)
        try:
            return build_health_snapshot(ticker, conn)
        finally:
            conn.close()
    except Exception as e:
        print(f"[catalyst_daily] snapshot {ticker} fallback: {e}")
        return snap


def _summarize(ticker: str, role: str, why: str, result: dict) -> dict:
    grid = result.get("catalyst_grid") or []
    pos = [c for c in grid if c.get("status") == "HIT" and c.get("type") == "positive"]
    neg = [c for c in grid if c.get("status") == "HIT" and c.get("type") == "negative"]
    def _hit(c: dict) -> dict:
        return {
            "type": c.get("type"), "taxonomy": c.get("taxonomy"),
            "event_date": c.get("event_date"),
            "headline": (c.get("headline") or c.get("evidence_excerpt") or "")[:160],
            "weight": c.get("adjusted_weight"), "confidence": c.get("confidence"),
        }
    top = sorted(pos + neg, key=lambda c: abs(float(c.get("adjusted_weight") or 0)), reverse=True)[:6]
    return {
        "ticker": ticker, "role": role, "why": why,
        "net_signal": result.get("net_signal"), "conviction": result.get("conviction"),
        "catalyst_stack": result.get("catalyst_stack") or "",
        "key_assumption": result.get("key_assumption") or "",
        "n_pos": len(pos), "n_neg": len(neg),
        "top_hits": [_hit(c) for c in top], "error": result.get("error"),
    }


def run_dossiers(date: str, targets: list[dict], skip_gemini: bool) -> list[dict]:
    if not targets:
        return []
    config.require_llm()
    print("[catalyst_daily] search=Grok native web/X (no SearXNG)")
    ca = _prepare_engine(skip_gemini)
    ca.CUTOFF_DATE = None
    ca.TODAY = date
    out: list[dict] = []
    for spec in targets:
        ticker = spec["ticker"]
        print(f"[catalyst_daily] → {ticker} ({spec['role']})", flush=True)
        reused = _reuse_saved(ticker, date)
        if reused:
            print(f"[catalyst_daily] reuse data/catalyst/{ticker}_{date}.json")
            out.append(_summarize(ticker, spec["role"], spec["why"], reused))
            continue
        try:
            result = ca.analyze_stock(ticker, _snapshot(ticker), "")
        except Exception as e:
            print(f"[catalyst_daily] FAIL {ticker}: {e}")
            out.append({"ticker": ticker, "role": spec["role"], "why": spec["why"], "error": str(e)[:240]})
            continue
        DATA_CATALYST.mkdir(parents=True, exist_ok=True)
        payload = dict(result)
        payload.update({"ticker": ticker, "generated_at": date, "cutoff_date": None, "role": spec["role"]})
        (DATA_CATALYST / f"{ticker}_{date}.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        out.append(_summarize(ticker, spec["role"], spec["why"], result))
    return out


def write_payload(payload: dict) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    date = payload["date"]
    js = OUT_DIR / f"{date}_dossiers.json"
    md = OUT_DIR / f"{date}_dossiers.md"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md.write_text(render(payload), encoding="utf-8")
    (OUT_DIR / "latest_dossiers.md").write_text(md.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[catalyst_daily] wrote {md}")
    print(f"[catalyst_daily] wrote {js}")
    return md, js


def run(date: str | None = None, max_n: int = DEFAULT_MAX, force: bool = False,
        extra: list[str] | None = None, dry_select: bool = False,
        skip_gemini: bool | None = None) -> dict:
    date = date or _today()
    if skip_gemini is None:
        skip_gemini = False
    if already_good(date) and not force:
        print(f"[catalyst_daily] skip-if-good {date}")
        apply_to_actions(date, load_dossiers(date))
        return _load_json(OUT_DIR / f"{date}_dossiers.json")
    targets = select_targets(date, max_n=max_n, extra=extra)
    print(f"[catalyst_daily] {date}: {len(targets)} targets {[t['ticker'] for t in targets]}")
    if dry_select:
        payload = {
            "date": date, "generated_at": datetime.now(ET).isoformat(),
            "max_n": max_n, "grok": True, "gemini": False, "n_targets": len(targets),
            "n_ok": 0, "targets": targets, "dossiers": [], "dry_select": True,
        }
        write_payload(payload)
        return payload
    dossiers = run_dossiers(date, targets, skip_gemini=skip_gemini)
    n_ok = sum(1 for d in dossiers if d.get("net_signal") and not d.get("error"))
    payload = {
        "date": date, "generated_at": datetime.now(ET).isoformat(),
        "max_n": max_n, "grok": True, "gemini": False, "n_targets": len(targets),
        "n_ok": n_ok, "targets": targets, "dossiers": dossiers,
    }
    write_payload(payload)
    apply_to_actions(date, dossiers)
    if n_ok == 0 and targets:
        print("[catalyst_daily] WARN: zero usable dossiers — actions unchanged")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--max", type=int, default=DEFAULT_MAX)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--tickers", default="")
    ap.add_argument("--dry-select", action="store_true")
    ap.add_argument("--gemini", action="store_true",
                    help="deprecated no-op; verdict/catcher always use Grok")
    args = ap.parse_args()
    extra = [t.strip().upper() for t in (args.tickers or "").split(",") if t.strip()]
    run(date=args.date, max_n=args.max, force=args.force, extra=extra,
        dry_select=args.dry_select, skip_gemini=False)


if __name__ == "__main__":
    main()
