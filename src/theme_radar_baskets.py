"""Theme Radar miss-basket snapshots + mechanism fire notes.

Research only. 09:30-knowable Finviz / AB / join / feature_asof.
Does not touch flatten_robust or invent scrapes.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from . import finviz_style_flags as fsf
from . import overlay_autopsy as oa

# Same floor as ticker_lookback.MIN_ATR_PCT — do not import that module
# (pandas). ATR% = 100 * Elite `Average True Range` / `Price`.
MIN_ATR_PCT = 2.5


def _atr_pct(atr, price) -> float | None:
    a = fsf.finite(atr)
    p = fsf.finite(price)
    if a is None or p is None or p <= 0:
        return None
    return 100.0 * a / p

ROOT = Path(__file__).resolve().parent.parent
JOIN_DIR = ROOT / "data" / "join"
FEAT_DIR = ROOT / "data" / "feature_asof"
AB_DIR = ROOT / "data" / "ab_checklist"

# Theme Radar miss baskets (graded high then lost) + gold contrast hit.
BASKETS: dict[str, tuple[str, ...]] = {
    "optics": ("AAOI", "COHR", "LITE", "GLW"),
    "ai_power": ("GEV", "VRT", "ETN", "PWR", "CAT"),
    "copper": ("FCX", "SCCO", "TECK", "ERO", "HBM"),
    "nuclear": ("CEG", "VST", "OKLO", "SMR", "CCJ"),
    "gold_hit": ("GDX", "GLD", "NEM", "AEM"),
}
ALL_TICKERS = tuple(t for names in BASKETS.values() for t in names)
BASKET_OF = {t: name for name, ts in BASKETS.items() for t in ts}

# Join total_score "high" = top quintile of that session's file (elevate test).
TS_TOP_Q = 0.80


def load_join_map(date: str) -> dict[str, dict]:
    path = JOIN_DIR / f"{date}_ranked.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            veto_raw = str(raw.get("veto") or "").strip().lower()
            out[t] = {
                "total_score": fsf.finite(raw.get("total_score")),
                "score_norm": fsf.finite(raw.get("score_norm")),
                "veto": veto_raw in ("true", "1", "yes"),
                "flags": raw.get("flags") or "",
                "ext": raw.get("ext") or "",
                "earn": raw.get("earn") or "",
            }
    return out


def join_high_cut(join_map: dict[str, dict]) -> float | None:
    xs = [r["total_score"] for r in join_map.values()
          if r.get("total_score") is not None]
    if len(xs) < 20:
        return None
    xs.sort()
    idx = min(len(xs) - 1, max(0, int(len(xs) * TS_TOP_Q)))
    return xs[idx]


def load_feature_asof(date: str) -> dict[str, dict]:
    path = FEAT_DIR / f"{date}_feature_asof.csv"
    if not path.exists():
        return {}
    keep = (
        "join", "ab", "peer", "fade", "alarm", "blue", "join_rank",
        "ab_score", "join_good", "ab_good", "ret_1d", "ret_1w",
    )
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            out[t] = {k: raw.get(k) for k in keep if k in raw}
    return out


def ab_fail_any(date: str, ticker: str) -> dict:
    """Stock-Screener-System analog: any status_* == BAD on prior AB."""
    for name in (f"{date}_ab_checklist_enriched.csv", f"{date}_ab_checklist.csv"):
        p = AB_DIR / name
        if not p.exists():
            continue
        with p.open(newline="", encoding="utf-8", errors="replace") as fh:
            for raw in csv.DictReader(fh):
                t = str(raw.get("Ticker") or "").strip().upper()
                if t != ticker:
                    continue
                bads = [k for k, v in raw.items()
                        if k.startswith("status_")
                        and str(v).strip().upper() == "BAD"]
                n_bad = fsf.finite(raw.get("n_bad"))
                return {
                    "n_bad": int(n_bad) if n_bad is not None else len(bads),
                    "fail_any": bool(bads) or (n_bad is not None and n_bad > 0),
                    "bad_cols": bads[:8],
                    "ab_file": name,
                }
        return {"n_bad": None, "fail_any": False, "bad_cols": [], "ab_file": name}
    return {"n_bad": None, "fail_any": None, "bad_cols": [], "ab_file": ""}


def _atr_from_row(row: dict | None) -> float | None:
    if not row:
        return None
    return _atr_pct(row.get("Average True Range"), row.get("Price"))


def snapshot_one(date: str, ticker: str, cal: list[str],
                 cache: dict) -> dict | None:
    """One name-day, 09:30-knowable only. None if prior Elite missing the name."""
    prior = oa._prior(cal, date)
    if not prior:
        return None
    if prior not in cache:
        cache[prior] = oa.load_finviz_map(prior)
    prior2 = oa._prior(cal, prior)
    if prior2 and prior2 not in cache:
        cache[prior2] = oa.load_finviz_map(prior2)
    yest = (cache.get(prior) or {}).get(ticker)
    if not yest:
        return None
    bits = oa.overlay_bits(date, ticker, cal, cache)
    if date not in cache:
        cache[date] = oa.load_finviz_map(date)
    today = (cache.get(date) or {}).get(ticker)
    jk = ("join", date)
    if jk not in cache:
        cache[jk] = load_join_map(date)
    join = (cache.get(jk) or {}).get(ticker) or {}
    cut = cache.setdefault(("join_cut", date), join_high_cut(cache[jk]))
    fk = ("feat", date)
    if fk not in cache:
        cache[fk] = load_feature_asof(date)
    feat = (cache.get(fk) or {}).get(ticker) or {}
    fail = ab_fail_any(prior, ticker)
    atr = _atr_from_row(yest)
    fpe = bits.get("fpe")
    d_rsi = bits.get("d_rsi")
    d_mcap = bits.get("d_mcap_pct")
    return {
        "date": date,
        "ticker": ticker,
        "basket": BASKET_OF.get(ticker, ""),
        "feature_export": prior,
        "fwd": oa.outcome_1d(today),
        "realized_tape": oa.realized_tape((cache.get(date) or {}).get("SPY")),
        "morning_tape": bits.get("morning_tape"),
        "fpe": fpe,
        "d_rsi": d_rsi,
        "d_mcap_pct": d_mcap,
        "radar_high_fpe": bool(bits.get("radar_high_fpe")),
        "radar_rsi_up": (
            d_rsi is not None and d_rsi >= fsf.D_RSI_UP
        ),
        "radar_mcap_up": (
            d_mcap is not None and d_mcap >= fsf.D_MCAP_PCT
        ),
        "radar_hot": bool(bits.get("radar_hot")),
        "radar_cheap_fpe": bool(bits.get("radar_cheap_fpe")),
        "avoid_veto": bool(bits.get("avoid_veto")),
        "elevate_bump": bool(bits.get("elevate_bump")),
        "mf_flag": bool(bits.get("mf_flag")),
        "canslim_flag": bool(bits.get("canslim_flag")),
        "ab_score": bits.get("ab_score"),
        "s_join": bits.get("s_join"),
        "s_ab": bits.get("s_ab"),
        "total_score": join.get("total_score"),
        "score_norm": join.get("score_norm"),
        "join_high": (
            join.get("total_score") is not None
            and cut is not None
            and join["total_score"] >= cut
        ),
        "join_veto": bool(join.get("veto")),
        "join_flags": join.get("flags") or "",
        "ab_fail_any": fail.get("fail_any"),
        "n_bad": fail.get("n_bad"),
        "ab_file": fail.get("ab_file") or "",
        "atr_pct": atr,
        "atr_below_floor": (
            atr is not None and atr < MIN_ATR_PCT
        ),
        "eps_surprise": fsf.finite(yest.get(fsf.H_EPS_SURP)),
        "rev_surprise": fsf.finite(yest.get("Revenue Surprise")),
        "feat_join": feat.get("join") or "",
        "feat_ab": feat.get("ab") or "",
        "feat_fade": str(feat.get("fade") or "").lower() in ("true", "1"),
        "feat_alarm": str(feat.get("alarm") or "").lower() in ("true", "1"),
        "feat_join_rank": fsf.finite(feat.get("join_rank")),
        "feat_ab_score": fsf.finite(feat.get("ab_score")),
        "feat_present": bool(feat),
    }


def collect_baskets(cal: list[str] | None = None) -> list[dict]:
    cal = cal or oa._export_dates()
    cache: dict = {}
    rows = []
    for d in cal:
        if not oa._prior(cal, d):
            continue
        for t in ALL_TICKERS:
            snap = snapshot_one(d, t, cal, cache)
            if snap:
                rows.append(snap)
    return rows


def score_total_score(cal: list[str] | None = None) -> dict:
    """High join total_score as an elevate — expected to die on up tapes."""
    cal = cal or oa._export_dates()
    cache: dict = {}
    rows = []
    for d in cal:
        prior = oa._prior(cal, d)
        if not prior:
            continue
        if d not in cache:
            cache[d] = oa.load_finviz_map(d)
        today = cache[d]
        spy = oa.realized_tape(today.get("SPY"))
        jmap = load_join_map(d)
        if not jmap:
            continue
        cut = join_high_cut(jmap)
        if cut is None:
            continue
        for t, j in jmap.items():
            ts = j.get("total_score")
            if ts is None:
                continue
            fwd = oa.outcome_1d(today.get(t))
            if fwd is None:
                continue
            rows.append({
                "fwd": fwd,
                "realized_tape": spy,
                "join_high": ts >= cut,
                "join_pos": ts > 0,
            })
    high = oa.score_rule(rows, "join_high", family="elevate")
    pos = oa.score_rule(rows, "join_pos", family="elevate")
    return {
        "n_joined": len(rows),
        "join_high": high,
        "join_pos": pos,
        "cut_note": f"join_high = session total_score ≥ p{int(TS_TOP_Q*100)}",
    }


def _fire_count(rows: list[dict], key: str) -> dict:
    known = [r for r in rows if r.get(key) is not None]
    hits = [r for r in known if r.get(key)]
    return {
        "n": len(known),
        "fired": len(hits),
        "rate": (len(hits) / len(known)) if known else None,
        "thin": len(known) < 8,
    }


def basket_summary(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, tickers in BASKETS.items():
        sub = [r for r in rows if r["ticker"] in tickers]
        fwds = [float(r["fwd"]) for r in sub if r.get("fwd") is not None]
        out[name] = {
            "tickers": list(tickers),
            "n_name_days": len(sub),
            "n_with_fwd": len(fwds),
            "mean_1d": oa._mean(fwds),
            "hit": (sum(1 for x in fwds if x > 0) / len(fwds)) if fwds else None,
            "present": sorted({r["ticker"] for r in sub}),
            "missing_elite": [t for t in tickers if t not in {r["ticker"] for r in sub}],
            "high_fpe": _fire_count(sub, "radar_high_fpe"),
            "rsi_up": _fire_count(sub, "radar_rsi_up"),
            "mcap_up": _fire_count(sub, "radar_mcap_up"),
            "hot": _fire_count(sub, "radar_hot"),
            "avoid": _fire_count(sub, "avoid_veto"),
            "canslim": _fire_count(sub, "canslim_flag"),
            "mf": _fire_count(sub, "mf_flag"),
            "elevate": _fire_count(sub, "elevate_bump"),
            "join_high": _fire_count(sub, "join_high"),
            "join_veto": _fire_count(sub, "join_veto"),
            "ab_fail": _fire_count(sub, "ab_fail_any"),
            "atr_below": _fire_count(sub, "atr_below_floor"),
            "feat_n": sum(1 for r in sub if r.get("feat_present")),
        }
    return out


def mechanism_rows(basket: dict[str, dict], ts: dict) -> list[dict]:
    """One row per external mechanism for the map table."""
    def _fired(key: str) -> str:
        parts = []
        for name, st in basket.items():
            cell = st.get(key) or {}
            n, f = cell.get("n") or 0, cell.get("fired") or 0
            if n == 0:
                parts.append(f"{name}: no Elite")
            elif cell.get("thin") and f == 0:
                parts.append(f"{name}: 0/{n} thin")
            else:
                parts.append(f"{name}: {f}/{n}")
        return "; ".join(parts)

    high = ts.get("join_high") or {}
    up_xs = ((high.get("tapes") or {}).get("up") or {}).get("xs")
    dn_xs = ((high.get("tapes") or {}).get("down") or {}).get("xs")

    def _row(mech: dict, sleeve: str, hold: str, clock: str) -> dict:
        mech["target_sleeve"] = sleeve
        mech["hold_sessions"] = hold
        mech["score_clock"] = clock
        return mech

    return [
        _row({
            "mechanism": "Theme Radar fade vetoes",
            "goal": "Avoid (1d only)",
            "fields": (
                "`Forward P/E` ≥ 35; `d_RSI` = Δ `Relative Strength Index (14)` "
                "(prior − prior-prior); `d_Market Cap` = % Δ `Market Cap` "
                "(same vintage). Knobs in `finviz_style_flags`: "
                f"HIGH_FPE={fsf.HIGH_FPE:g}, D_RSI_UP={fsf.D_RSI_UP:g}, "
                f"D_MCAP_PCT={fsf.D_MCAP_PCT:g}."
            ),
            "basket_fire": (
                f"high-FPE {_fired('high_fpe')}. "
                f"d_RSI↑ {_fired('rsi_up')}. d_mcap↑ {_fired('mcap_up')}. "
                "Surviving both-tape avoid is high-FPE alone; d_RSI / d_mcap "
                "stay veto *candidates*, not rank fuel. Combined `radar_hot` "
                "failed both-tape — do not OR into the live veto. "
                "1d clock only — not flatten_h5 / flatten_robust."
            ),
            "veto_not_fuel": "YES on 1d clock only. Does not auto-apply to flatten_h5 / flatten_robust.",
            "elevate": "NO — fades are not buy-rank fuel.",
        }, "theme_radar_1d", "1", "1d open→close"),
        _row({
            "mechanism": "CANSLIM scanners",
            "goal": "Expand (not Elevate)",
            "fields": (
                "C `EPS Growth Quarter Over Quarter` + `EPS Surprise`; "
                "A `EPS Growth This Year` / `EPS Growth Past 3 Years`; "
                "N `52-Week High` (% below high); S prior `Relative Volume` + "
                "`Average Volume`; L `Performance (Quarter)` + AB "
                "`P01_peer_lead_week`; I `Institutional Ownership` / "
                "`Institutional Transactions`; M weather "
                "`signals.general_direction` / `signals.risk`."
            ),
            "basket_fire": (
                f"{_fired('canslim')}. Fired on copper (FCX/TECK/ERO) — those "
                "names then lost. Panel already failed both-tape as long "
                "(up xs −0.35). Do not bump."
            ),
            "veto_not_fuel": "Do not invert CANSLIM into a fade veto without a new bar.",
            "elevate": "NO — dies on 1d up tapes. Reject until matching hold.",
        }, "theme_radar_1d", "1", "1d open→close"),
        _row({
            "mechanism": "Magic Formula",
            "goal": "Expand (not Elevate)",
            "fields": (
                "`Income` / `Enterprise Value` (else `1/EV/EBITDA`, else "
                "`1/P/E`); ROC = `Return on Invested Capital`. Exclude "
                "Financial/Utilities; `Market Cap` < 100 dropped."
            ),
            "basket_fire": (
                f"{_fired('mf')}. Cheap/MF is the gold-contrast trap, not a "
                "rescue. Panel up xs −0.07 — failed both-tape as long."
            ),
            "veto_not_fuel": "Not a fade. Do not treat cheap as avoid either.",
            "elevate": "NO — do not promote cheap/MF as long.",
        }, "theme_radar_1d", "1", "1d open→close"),
        _row({
            "mechanism": "Stock-Screener-System multi-factor fail-any",
            "goal": "Avoid (shaped)",
            "fields": (
                "AB `status_*` any BAD / `n_bad` > 0 on prior "
                "`{D}_ab_checklist_enriched.csv`. Join `veto_when` already "
                "named: `earn:today`; `ext:extreme` AND weather `risk=off` "
                "(`00_grounding/join_rules.json`)."
            ),
            "basket_fire": (
                f"AB fail-any {_fired('ab_fail')}. ~93% of stock name-days "
                "have some `status_*=BAD` — too wide for a veto. Join "
                f"`veto_when` {_fired('join_veto')} (0 on these baskets). "
                "Enriched AB starts 08-19 — earlier is thin-n. Do not invent "
                "a new screener."
            ),
            "veto_not_fuel": "Fail-any is a veto shape, not a rank add.",
            "elevate": "NO.",
        }, "theme_radar_1d", "1", "1d open→close"),
        _row({
            "mechanism": "AlphaSuite / ATR risk caps",
            "goal": "Avoid / size (if relevant)",
            "fields": (
                "Elite `Average True Range` / `Price` → `atr_pct`. Already "
                f"the liquidity floor `MIN_ATR_PCT={MIN_ATR_PCT}` in "
                "`ticker_lookback` / stock-book. Not a long rank."
            ),
            "basket_fire": (
                f"ATR below floor {_fired('atr_below')}. Only GLD (ETF) "
                "sits under 2.5% — a gold *hit*, so the floor would have "
                "wrongly gated a winner. Optics/AI/copper/nuclear: 0. "
                "Size cap only, not a Theme Radar substitute."
            ),
            "veto_not_fuel": "Size cap only. Do not score ATR% as buy-rank.",
            "elevate": "NO.",
        }, "all flatten_* (size floor)", "n/a", "prior Elite ATR/Price"),
        _row({
            "mechanism": "AlphaSift L1→L2 re-rank",
            "goal": "Expand only",
            "fields": (
                "Existing layers only: `data/join/{D}_ranked.csv` "
                "`total_score` / `score_norm`; book `score_1d`; "
                "`data/feature_asof/{D}_feature_asof.csv` `join_rank` / "
                "`join` / `ab`. No new sift scrape."
            ),
            "basket_fire": (
                f"join top-quintile {_fired('join_high')}. These miss names "
                "were already graded high on join/AB — L2 on the same "
                "layers would have kept them elevated, then they lost."
            ),
            "veto_not_fuel": "Do not feed fade columns into a buy re-rank.",
            "elevate": "NO — existing ranker, already high on miss names; do not bump.",
        }, "theme_radar_1d", "1", "1d open→close"),
        _row({
            "mechanism": "vectorbt sweeps",
            "goal": "Expand only",
            "fields": (
                "Wrap `factor_mine_book` 09:30 `open` + Futubull fees. "
                "Panel: `data/prices/ohlc.parquet` `(date,ticker)`. "
                "No vectorbt default close-to-close fills."
            ),
            "basket_fire": "Harness not written — 0 fires. Expand-only sidecar.",
            "veto_not_fuel": "N/A until a recipe is scored on the both-tape bar.",
            "elevate": "NO — not a bump column.",
        }, "flatten_h1/h3/h5", "1/3/5", "Nd open→exit + Futubull"),
        _row({
            "mechanism": "Zipline cross-section",
            "goal": "Expand only",
            "fields": (
                "Same `data/prices/ohlc.parquet` + PIT book as vectorbt. "
                "No Zipline pipeline in-repo. Do not pull Zipline data."
            ),
            "basket_fire": "No in-repo pipeline — 0 fires. Thin-n / not wired.",
            "veto_not_fuel": "N/A.",
            "elevate": "NO.",
        }, "flatten_h1/h3/h5", "1/3/5", "Nd open→exit"),
        _row({
            "mechanism": "OpenBB SEC / surprise",
            "goal": "Expand (thin-gap only)",
            "fields": (
                "Elite already has `EPS Surprise`, `Revenue Surprise`, "
                "`Earnings Date`. AB `val_B01_eps_surprise` / "
                "`status_B01_eps_surprise`, B02, B17, B18. Do not replace."
            ),
            "basket_fire": (
                "No new OpenBB pull. Surprise headers already on prior Elite "
                "for names Finviz covers. Gap-fill only when the Elite cell "
                "is blank — do not invent 8-K scrapes for OKLO/SMR/GDX."
            ),
            "veto_not_fuel": "Same-day surprise on D is a leak. Prior vintage only.",
            "elevate": "NO — do not bump on a beat.",
        }, "sidecar", "—", "asof < D"),
        _row({
            "mechanism": "qlib / FinRL sidecars",
            "goal": "Expand only",
            "fields": (
                "`sidecars/qlib/`, `sidecars/finrl/`, "
                "`data/sidecars/{name}/{asof}/preds.parquet`. "
                "Join `ticker` + `asof_date` < D (or D iff morning-packet vintage)."
            ),
            "basket_fire": "No sidecar preds on disk — 0 fires. Bar not cleared.",
            "veto_not_fuel": "N/A.",
            "elevate": "NO — offline until the same PIT / fee / audit bar.",
        }, "sidecar → flatten_h*", "matching hold", "Nd open→exit (unscored)"),
    ]


def paper_basket_hits() -> list[dict]:
    hits = []
    want = set(ALL_TICKERS)
    for c in oa.load_paper_cases() + oa.load_gap_cases():
        if c.get("ticker") in want:
            hits.append(c)
    hits.sort(key=lambda r: (r.get("date") or "", r.get("ticker") or ""))
    return hits


OUT_JSON = ROOT / "03_scoreboard" / "theme_radar_baskets.json"


def run() -> dict:
    cal = oa._export_dates()
    rows = collect_baskets(cal)
    ts = score_total_score(cal)
    summary = basket_summary(rows)
    return {
        "n_snapshots": len(rows),
        "sessions": sorted({r["date"] for r in rows}),
        "baskets": summary,
        "total_score": ts,
        "mechanisms": mechanism_rows(summary, ts),
        "paper_or_gap": paper_basket_hits(),
        "snapshots": rows,
    }


def slim_payload(payload: dict) -> dict:
    """Drop bulky snapshots for the scoreboard JSON."""
    snaps = payload.get("snapshots") or []
    # Keep a short per-name last-seen row for the autopsy table.
    last: dict[str, dict] = {}
    for r in snaps:
        last[r["ticker"]] = r
    paper = []
    for c in payload.get("paper_or_gap") or []:
        paper.append({
            "date": c.get("date"),
            "ticker": c.get("ticker"),
            "kind": c.get("kind"),
            "source": c.get("source"),
            "fwd": c.get("fwd"),
            "gap_class": c.get("gap_class"),
        })
    return {
        "n_snapshots": payload.get("n_snapshots"),
        "sessions": payload.get("sessions"),
        "baskets": payload.get("baskets"),
        "total_score": payload.get("total_score"),
        "mechanisms": payload.get("mechanisms"),
        "paper_or_gap": paper,
        "last_seen": last,
    }


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    payload = run()
    if args.write:
        OUT_JSON.write_text(
            json.dumps(slim_payload(payload), indent=2, default=str),
            encoding="utf-8",
        )
        print(f"[theme-radar-baskets] n={payload['n_snapshots']} -> {OUT_JSON}")
    else:
        print(json.dumps({
            "n_snapshots": payload["n_snapshots"],
            "sessions": payload["sessions"],
            "baskets": payload["baskets"],
            "total_score": payload["total_score"],
        }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
