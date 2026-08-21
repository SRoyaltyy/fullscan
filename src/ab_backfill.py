"""Point-in-time daily AB checklist backfill.

Each asof day includes:
  - Part A/B1 score (OHLC always; B1 when Finviz export exists)
  - Peer 5d RS / beat% / peers advancing (price_store × Correlations)
  - Industry 5d median (members from latest export roster)
  - Sector board Dir/Score (nearest _BOARD.md <= asof; coverage from ~late Jul 2026)
  - Forward 1d/3d/1w/2m 🟢🔴 favorability

CLI:
  python -m src.ab_backfill --months 12 --end 2026-08-19
  python -m src.ab_backfill --months 12 --end 2026-08-19 --checkpoint-every 10
  python -m src.ab_backfill --md-only data/ab_backfill/2025-08-19_2026-08-19_universe.parquet

Universe PIT is expensive (~2.5k liquid names × ~252 sessions). Checkpoints
land as ``{start}_{end}_{tag}.resume.parquet`` every N asof days (and on
SIGTERM/SIGINT). A later run with the same window loads that file and skips
completed asof dates. ``--md-only`` rebuilds the BB-style markdown (daily
trail + score/context effectiveness audit) from an existing parquet without
rescoring.
"""
from __future__ import annotations

import argparse
import json
import re
import signal
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import price_store as ps
from . import ab_checklist as ab
from . import ab_context_daily as ctx
from . import event_markers as em

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "ab_backfill"
INS_PANEL = ROOT / "data" / "insider" / "history" / "monthly_panel.parquet"
INS_PANEL_CSV = ROOT / "data" / "insider" / "history" / "monthly_panel.csv"
ET = ZoneInfo(config.TZ)

HORIZONS = {"1d": 1, "3d": 3, "1w": 5, "2m": 42}

# Final universe artifacts only. `*.resume.parquet` is a mid-run checkpoint
# and must not be treated as a completed liquid-universe backfill.
UNIVERSE_FINAL_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_universe\.(parquet|csv)$"
)
RESUME_SUFFIX = ".resume"
DEFAULT_CHECKPOINT_EVERY = 10


class _StopFlag:
    """Set by SIGTERM/SIGINT so the current asof can finish, then we flush."""

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum=None, frame=None) -> None:
        self.requested = True
        print(
            f"[backfill] signal {signum} — checkpoint after this asof, then stop",
            flush=True,
        )


def install_stop_handlers(stop: _StopFlag) -> _StopFlag:
    for name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, stop.request)
        except (ValueError, OSError):
            pass
    return stop


def artifact_stem(start: str, end: str, tag: str) -> str:
    return f"{start}_{end}_{tag}"


def resume_stem(start: str, end: str, tag: str) -> str:
    return f"{artifact_stem(start, end, tag)}{RESUME_SUFFIX}"


def resume_paths(start: str, end: str, tag: str) -> tuple[Path, Path]:
    stem = resume_stem(start, end, tag)
    return OUT_DIR / f"{stem}.parquet", OUT_DIR / f"{stem}.json"


def remaining_days(days: list[str], last_asof: str | None) -> list[str]:
    if not last_asof:
        return list(days)
    return [d for d in days if d > last_asof]


def load_resume_checkpoint(
    start: str, end: str, tag: str,
) -> tuple[pd.DataFrame, dict | None]:
    pq, meta_p = resume_paths(start, end, tag)
    if not pq.exists() or pq.stat().st_size < 1000:
        return pd.DataFrame(), None
    df = pd.read_parquet(pq)
    info: dict = {}
    if meta_p.exists():
        try:
            info = json.loads(meta_p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[backfill] resume json unreadable: {e}", flush=True)
    last = None
    if len(df) and "asof_date" in df.columns:
        last = str(df["asof_date"].max())[:10]
    if not last:
        last = str(info.get("last_asof") or "")[:10] or None
    info["last_asof"] = last
    info["n_rows"] = int(len(df))
    return df, info


def write_resume_checkpoint(
    rows: list[dict],
    start: str,
    end: str,
    tag: str,
    last_asof: str,
    days_done: int,
    days_total: int,
) -> Path:
    """Atomic parquet + json sidecar. Not a finished universe file."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pq, meta_p = resume_paths(start, end, tag)
    df = pd.DataFrame(rows)
    tmp = pq.with_suffix(pq.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(pq)
    meta = {
        "start": start,
        "end": end,
        "tag": tag,
        "last_asof": last_asof,
        "days_done": int(days_done),
        "days_total": int(days_total),
        "n_rows": int(len(df)),
        "status": "in_progress",
        "resume": True,
        "generated": datetime.now(ET).isoformat(),
    }
    meta_p.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(
        f"[backfill] checkpoint {last_asof} ({days_done}/{days_total}) "
        f"rows={len(df):,} → {pq.name}",
        flush=True,
    )
    return pq


def clear_resume_checkpoint(start: str, end: str, tag: str) -> None:
    for p in resume_paths(start, end, tag):
        if p.exists():
            p.unlink()
            print(f"[backfill] removed {p.name}", flush=True)


def _load_exports() -> list[tuple[str, pd.DataFrame]]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    out = []
    for f in files:
        d = f.stem.replace("finviz_", "")
        df = pd.read_csv(f, low_memory=False)
        tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
        df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
        df = df.drop_duplicates("Ticker", keep="first")
        out.append((d, df))
    return out


def _export_asof(exports, asof: str):
    cand = [(d, df) for d, df in exports if d <= asof]
    if not cand:
        return None, None
    return cand[-1]


def _prior_export_row(exports, asof: str, ticker: str):
    cand = [(d, df) for d, df in exports if d < asof]
    if not cand:
        return None, None
    d, df = cand[-1]
    hit = df[df["Ticker"] == ticker]
    if hit.empty:
        return None, d
    return hit.iloc[0], d


def _form4_asof(panel: pd.DataFrame, ticker: str, asof: str) -> dict:
    if panel is None or panel.empty:
        return {"flag": 0, "val": "no_panel"}
    month = (pd.Timestamp(asof).to_period("M") - 1).strftime("%Y-%m")
    p = panel[panel["ticker"].astype(str).str.upper() == ticker.upper()]
    p = p[p["month"] <= month]
    if p.empty:
        return {"flag": 0, "val": f"no_form4_month<={month}"}
    row = p.sort_values("month").iloc[-1]
    net = float(row.get("net_value", np.nan))
    delta = float(row.get("net_delta", np.nan)) if "net_delta" in row.index else np.nan
    flag = 0
    if np.isfinite(delta):
        flag = 1 if delta > 0 else (-1 if delta < 0 else 0)
    elif np.isfinite(net):
        flag = 1 if net > 0 else (-1 if net < 0 else 0)
    return {"flag": flag, "val": f"month={row['month']} net={net} delta={delta}"}


def _empty_b1() -> dict:
    return {"profitable": False, "prior_export_date": None}


def _setup_flag(a: dict, b: dict) -> tuple[int, str]:
    if not a.get("ok"):
        return 0, "no_ohlc"
    prof = bool(b.get("profitable"))
    rsi = a.get("rsi")
    sec = a.get("sections") or {}
    near_floor = bool(sec.get("ok") and (sec.get("rising_lows") or sec.get("flatish")))
    near_low_px = False
    if sec.get("ok") and sec.get("lows"):
        floor = min(sec["lows"])
        px = a.get("price")
        if np.isfinite(px) and floor > 0 and (px / floor - 1.0) <= 0.08:
            near_low_px = True
    pair = a.get("pair") or {}
    body_ok = np.isfinite(pair.get("body_rg", np.nan)) and pair["body_rg"] >= 1.0
    vol_ok = (not np.isfinite(pair.get("vol_rg", np.nan))) or pair["vol_rg"] >= 1.0
    oversold = np.isfinite(rsi) and rsi < 30
    if prof and oversold and (near_floor or near_low_px) and body_ok and vol_ok:
        return 1, "SETUP_GOOD"
    return 0, "SETUP_NO"


def _forward_multi(ohlc, asof: str, entry: float) -> dict:
    out: dict = {}
    for name, bars in HORIZONS.items():
        out[f"mp_{name}"] = np.nan
        out[f"ml_{name}"] = np.nan
        out[f"end_{name}"] = np.nan
        out[f"fav_{name}"] = np.nan
        out[f"up_{name}"] = np.nan
        out[f"bars_{name}"] = 0
    if ohlc is None or ohlc.empty or not np.isfinite(entry) or entry <= 0:
        return out
    df = ohlc.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    fut_all = df[df.index > pd.Timestamp(asof)]
    if fut_all.empty:
        return out
    high_col = "high" if "high" in fut_all.columns else "close"
    low_col = "low" if "low" in fut_all.columns else "close"
    for name, bars in HORIZONS.items():
        fut = fut_all.head(bars)
        n = len(fut)
        if n == 0:
            continue
        hi = float(fut[high_col].astype(float).max())
        lo = float(fut[low_col].astype(float).min())
        end = float(fut["close"].astype(float).iloc[-1])
        mp = hi / entry - 1.0
        ml = lo / entry - 1.0
        er = end / entry - 1.0
        out[f"mp_{name}"] = mp
        out[f"ml_{name}"] = ml
        out[f"end_{name}"] = er
        out[f"bars_{name}"] = n
        out[f"fav_{name}"] = 1.0 if mp > abs(ml) else 0.0
        out[f"up_{name}"] = 1.0 if er > 0 else 0.0
    return out


def _dot(fav) -> str:
    if fav is None or (isinstance(fav, float) and not np.isfinite(fav)):
        return "⚪"
    return "🟢" if float(fav) >= 0.5 else "🔴"


def _sec_dot(direction) -> str:
    if direction == "up":
        return "🟢"
    if direction == "down":
        return "🔴"
    return "⚪"



def _er_chip(kind, color, label, surprise=None, days=None) -> str:
    chip = {"green": "🟢", "red": "🔴", "white": "⚪"}.get(str(color or ""), "⚪")
    if not label and not color:
        return "—"
    extra = ""
    if kind == "E" and surprise is not None and np.isfinite(surprise):
        extra = f"{surprise:+.0f}%"
    elif days is not None and np.isfinite(days):
        extra = f"d{int(days)}"
    return f"{chip}{label or kind}{(' ' + extra) if extra else ''}"


def _score_cell(score) -> str:
    try:
        s = int(score)
    except Exception:
        return "⚪—"
    if s > 0:
        return f"🟢 **{s:+d}**"
    if s < 0:
        return f"🔴 **{s:+d}**"
    return f"⚪ **{s:+d}**"


def _p_chip(val, pos_label: str, neg_label: str) -> str:
    try:
        v = int(val)
    except Exception:
        return "⚪"
    if v > 0:
        return f"🟢{pos_label}"
    if v < 0:
        return f"🔴{neg_label}"
    return "⚪"


def _label_colored(r) -> str:
    parts = [
        _p_chip(r.get("P01"), "LEAD", "LAG"),
        _p_chip(r.get("P02"), "peers↑", "peers↓"),
        _p_chip(r.get("P03"), "ind↑", "ind↓"),
        _p_chip(r.get("P04"), "sec↑", "sec↓"),
    ]
    return " ".join(parts)


def _ensure_price_coverage(start: str, end: str, ticker: str | None, peer_tickers: list[str]) -> None:
    store = ps._load_store()
    need_days = max((pd.Timestamp(end) - pd.Timestamp(start)).days + 150, 400)
    want = set()
    if ticker:
        want.add(ticker.upper())
    want.update(p.upper() for p in peer_tickers)
    # always bootstrap requested names for peer RS
    names = sorted(want) if want else None
    if len(store):
        dmin = pd.to_datetime(store["date"]).min().date().isoformat()
        have = set(store["ticker"].astype(str).str.upper().unique())
        missing = [t for t in (names or []) if t not in have]
        if dmin <= start and not missing:
            print(f"[backfill] price store OK from {dmin}; peers present")
            return
        print(f"[backfill] extending bootstrap days={need_days} missing_peers={missing[:8]}")
    else:
        print(f"[backfill] empty store — bootstrap days={need_days}")
    if names:
        # bootstrap ticker + peers in chunks
        ps.bootstrap(days=need_days, tickers=names[:1], resume=True)
        if len(names) > 1:
            ps.bootstrap(days=need_days, tickers=names[1:], resume=True)
    else:
        ps.bootstrap(days=need_days, tickers=None, resume=True)


def _join_above_sma50(df: pd.DataFrame) -> tuple[pd.Series | None, pd.Series | None]:
    """PIT close >= SMA50 from the price store. (None, None) if store missing."""
    try:
        store = ps._load_store()
    except Exception:
        return None, None
    if store is None or len(store) == 0 or "Ticker" not in df.columns:
        return None, None
    want = set(df["Ticker"].astype(str).str.upper())
    ohlc = store.loc[store["ticker"].astype(str).str.upper().isin(want), ["date", "ticker", "close"]].copy()
    if ohlc.empty:
        return None, None
    ohlc["asof"] = pd.to_datetime(ohlc["date"]).dt.strftime("%Y-%m-%d")
    ohlc["Ticker"] = ohlc["ticker"].astype(str).str.upper()
    ohlc = ohlc.sort_values(["Ticker", "asof"])
    ohlc["sma50"] = ohlc.groupby("Ticker", sort=False)["close"].transform(
        lambda s: s.rolling(50, min_periods=50).mean()
    )
    key = pd.DataFrame(
        {
            "Ticker": df["Ticker"].astype(str).str.upper().to_numpy(),
            "asof": df["asof_date"].astype(str).str[:10].to_numpy(),
        },
        index=df.index,
    )
    m = key.merge(
        ohlc[["Ticker", "asof", "close", "sma50"]],
        on=["Ticker", "asof"],
        how="left",
    )
    m.index = df.index
    px = pd.to_numeric(df["price"], errors="coerce") if "price" in df.columns else pd.Series(np.nan, index=df.index)
    px = px.where(px.notna(), pd.to_numeric(m["close"], errors="coerce"))
    sma = pd.to_numeric(m["sma50"], errors="coerce")
    return px >= sma, px / sma - 1.0


def _pattern_audit(out: pd.DataFrame) -> list[str]:
    """Does AB score / P01–P04 actually line up with forward 🟢?

    🟢 = max upside > |max downside| from that day's close.
    Incomplete horizons dropped (need 1/3/5/20 bars for 1d/3d/1w/2m).
    Cells are hit-rate and percentage-points vs the universe base rate.
    """
    lines = [
        "## Pattern correlation audit",
        "",
        "Test: from that day's close, was max upside > |max downside| (🟢).",
        "Each cell is **hit rate (pp vs base rate)**. Incomplete forward windows are dropped.",
        "",
    ]
    if out is None or out.empty:
        lines.append("_no rows_")
        return lines
    df = out.copy()
    horizons = ["1d", "3d", "1w", "2m"]
    need = {h: int(HORIZONS[h]) for h in horizons}

    def hit(mask, h):
        col, bars = f"fav_{h}", f"bars_{h}"
        sub = df.loc[mask]
        if bars in sub.columns:
            b = pd.to_numeric(sub[bars], errors="coerce").fillna(0)
            sub = sub[b >= need[h]]
        v = pd.to_numeric(sub[col], errors="coerce").dropna()
        if len(v) < 5:
            return None, int(len(v))
        return float((v >= 0.5).mean()), int(len(v))

    bases: dict[str, float | None] = {}
    base_n: dict[str, int] = {}
    for h in horizons:
        r, n = hit(pd.Series(True, index=df.index), h)
        bases[h] = r
        base_n[h] = n

    def cell(mask, h) -> str:
        r, _ = hit(mask, h)
        if r is None:
            return "—"
        b = bases[h]
        if b is None:
            return f"{100 * r:.0f}%"
        return f"{100 * r:.0f}% ({100 * (r - b):+.0f})"

    lines += [
        "### A. Score level (Part A + B1 flags that day)",
        "",
        "| bucket | n | 1d | 3d | 1w | 2m |",
        "|--------|--:|---:|---:|---:|---:|",
    ]
    for (lo, hi), lab in zip(
        [(-99, -1), (0, 2), (3, 5), (6, 99)], ["≤-1", "0-2", "3-5", "≥6"]
    ):
        m = (df["score"] >= lo) & (df["score"] <= hi)
        lines.append(
            f"| {lab} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += [
        "",
        "### B. Context flags vs forward 🟢 (this is the P01–P04 test)",
        "",
        "| flag | n | 1d | 3d | 1w | 2m |",
        "|------|--:|---:|---:|---:|---:|",
    ]
    lab_col = df["context_label"].astype(str)
    for lab in ("LEAD", "LAG", "peers↑", "peers↓", "ind↑", "ind↓", "sec↑", "sec↓"):
        m = lab_col.str.contains(lab, regex=False)
        lines.append(
            f"| {lab} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += [
        "",
        "### C. Enriched score (score + context) buckets",
        "",
        "| enr | n | 1d | 3d | 1w | 2m |",
        "|-----|--:|---:|---:|---:|---:|",
    ]
    for (lo, hi), lab in zip(
        [(-99, -2), (-1, 1), (2, 5), (6, 99)], ["≤-2", "-1..+1", "2-5", "≥6"]
    ):
        m = (df["score_enriched"] >= lo) & (df["score_enriched"] <= hi)
        lines.append(
            f"| {lab} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += [
        "",
        "### D. Score × context — does P01–P04 add anything on top of AB?",
        "",
        "If context is real, `score≥3 ∧ LEAD` should beat `score≥3 ∧ LAG` on 1w/2m.",
        "",
        "| combo | n | 1d | 3d | 1w | 2m |",
        "|-------|--:|---:|---:|---:|---:|",
    ]
    lead = lab_col.str.contains("LEAD", regex=False)
    lag = lab_col.str.contains("LAG", regex=False)
    hi = df["score"] >= 3
    lo = df["score"] <= -1
    for name, m in (
        ("score≥3 ∧ LEAD", hi & lead),
        ("score≥3 ∧ LAG", hi & lag),
        ("score≤-1 ∧ LEAD", lo & lead),
        ("score≤-1 ∧ LAG", lo & lag),
        ("enr≥6", df["score_enriched"] >= 6),
        ("enr≤-2", df["score_enriched"] <= -2),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += ["", "### E. Base rates (all names × sessions with a complete window)", ""]
    bits = []
    for h in horizons:
        r, n = bases[h], base_n[h]
        bits.append(f"{h}:{100 * r:.0f}%(n={n:,})" if r is not None else f"{h}:—")
    lines.append("- " + " · ".join(bits))
    lines.append("")

    lines += [
        "### F. Within-day rank — does AB pick the right names *that session*?",
        "",
        "Pooled lift (A–E) can sit on the market base (~52% 🟢) even if the ranker works. "
        "This is the ranking test: on each asof, top vs bottom quartile of that day's universe.",
        "",
        "| slice | n | 1d | 3d | 1w | 2m |",
        "|-------|--:|---:|---:|---:|---:|",
    ]
    asof_key = df["asof_date"].astype(str).str[:10]

    def qmask(col: str, top: bool, q: float = 0.75):
        rnk = df.groupby(asof_key)[col].rank(pct=True, method="average")
        return (rnk >= q) if top else (rnk <= (1.0 - q))

    for name, m in (
        ("top 25% enr that day", qmask("score_enriched", True)),
        ("bot 25% enr that day", qmask("score_enriched", False)),
        ("top 10% enr that day", qmask("score_enriched", True, 0.90)),
        ("bot 10% enr that day", qmask("score_enriched", False, 0.90)),
        ("top 25% score that day", qmask("score", True)),
        ("bot 25% score that day", qmask("score", False)),
        ("LEAD that day", lead),
        ("LAG that day", lag),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines.append("")
    lines.append(
        "### G. The cut that matters — score>3 + context good, red chips, streaks"
    )
    lines.append("")
    lines.append(
        "Not the median (score ≈ +1). **Score > 3** and **context good** = "
        "LEAD + peers↑ + ind↑. Red circles = 🔴 chips in the context column "
        "(LAG / peers↓ / ind↓ / sec↓). A day with **≤1 red chip** is the "
        "“clean” day. Consecutive = consecutive sessions of that for the same ticker."
    )
    lines.append("")
    p01 = pd.to_numeric(df["P01"], errors="coerce").fillna(0)
    p02 = pd.to_numeric(df["P02"], errors="coerce").fillna(0)
    p03 = pd.to_numeric(df["P03"], errors="coerce").fillna(0)
    p04 = pd.to_numeric(df["P04"], errors="coerce").fillna(0)
    n_red_ctx = (
        (p01 < 0).astype(int)
        + (p02 < 0).astype(int)
        + (p03 < 0).astype(int)
        + (p04 < 0).astype(int)
    )
    hi = df["score"] > 3
    allgreen = (p01 == 1) & (p02 == 1) & (p03 == 1)
    ctx_ok = (p01 == 1) & (p02 == 1) & (p03 != -1)
    clean = n_red_ctx <= 1
    dirty = n_red_ctx >= 3

    def _streak_len(pos_mask: pd.Series) -> pd.Series:
        tmp = pd.DataFrame(
            {
                "Ticker": df["Ticker"].astype(str).to_numpy(),
                "asof": df["asof_date"].astype(str).str[:10].to_numpy(),
                "pos": pos_mask.astype(bool).to_numpy(),
            },
            index=df.index,
        )
        tmp = tmp.sort_values(["Ticker", "asof"])
        t = tmp["Ticker"].to_numpy()
        pos = tmp["pos"].to_numpy()
        prev = np.empty(len(tmp), dtype=object)
        prev[0] = None
        prev[1:] = t[:-1]
        tmp["_rid"] = np.cumsum((t != prev) | (~pos))
        tmp["_p"] = pos.astype(int)
        return tmp.groupby("_rid", sort=False)["_p"].cumsum().reindex(df.index).fillna(0).astype(int)

    streak_clean = _streak_len(clean)
    streak_dirty = _streak_len(dirty)
    streak_setup = _streak_len(hi & allgreen)

    lines += [
        "| slice | n | 1d | 3d | 1w | 2m |",
        "|-------|--:|---:|---:|---:|---:|",
    ]
    for name, m in (
        ("score>3 (context ignored)", hi),
        ("LEAD+peers↑+ind↑", allgreen),
        ("**score>3 ∧ LEAD+peers↑+ind↑**", hi & allgreen),
        ("score>3 ∧ LEAD+peers↑ ∧ not-ind↓", hi & ctx_ok),
        ("score>3 ∧ n_red_ctx=0", hi & (n_red_ctx == 0)),
        ("score>3 ∧ n_red_ctx≤1", hi & clean),
        ("score≤-1 ∧ LAG+peers↓+ind↓ (opposite)", (df["score"] <= -1) & dirty),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += [
        "",
        "Red chips that day (context column only; white does not count):",
        "",
        "| n_red_ctx | n | 1d | 3d | 1w | 2m |",
        "|-----------|--:|---:|---:|---:|---:|",
    ]
    for name, m in (
        ("0 (all green/white)", n_red_ctx == 0),
        ("1", n_red_ctx == 1),
        ("**≤1 (the cut)**", clean),
        ("2", n_red_ctx == 2),
        ("≥3 (LAG+peers↓+ind↓)", dirty),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    lines += [
        "",
        "Consecutive sessions of the same ticker:",
        "",
        "| streak | n | 1d | 3d | 1w | 2m |",
        "|--------|--:|---:|---:|---:|---:|",
    ]
    for name, m in (
        ("clean (≤1 red) = 1", streak_clean == 1),
        ("clean = 2", streak_clean == 2),
        ("clean = 3", streak_clean == 3),
        ("clean = 4", streak_clean == 4),
        ("clean ≥ 5", streak_clean >= 5),
        ("clean ≥ 3", streak_clean >= 3),
        ("score>3∧allgreen streak = 1", streak_setup == 1),
        ("score>3∧allgreen streak = 2", streak_setup == 2),
        ("score>3∧allgreen streak ≥ 3", streak_setup >= 3),
        ("dirty (≥3 red) streak ≥ 3", streak_dirty >= 3),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    sorted_ix = df.sort_values(["Ticker", "asof_date"]).index
    tmp = df.loc[sorted_ix]
    score_ma5 = (
        tmp.groupby("Ticker", sort=False)["score"]
        .transform(lambda s: s.rolling(5, min_periods=5).mean())
        .reindex(df.index)
    )
    nred_ma5 = (
        tmp.assign(_nr=n_red_ctx.reindex(tmp.index))
        .groupby("Ticker", sort=False)["_nr"]
        .transform(lambda s: s.rolling(5, min_periods=5).mean())
        .reindex(df.index)
    )
    rsi = pd.to_numeric(df["rsi"], errors="coerce") if "rsi" in df.columns else pd.Series(np.nan, index=df.index)

    lines += [
        "",
        "### H. SMA50 and 5-day mean score — does 'looks good' help?",
        "",
        "A09_above_sma50 is **already ±1 inside `score`**, and A10 is the 20/50/80 stack. "
        "Score>3 names are ~85% already above SMA50 — that is why the chart looks clean. "
        "This section splits that flag out, and replaces a single day's score with the "
        "**mean score over the past 5 sessions** of the same ticker.",
        "",
        "| slice | n | 1d | 3d | 1w | 2m |",
        "|-------|--:|---:|---:|---:|---:|",
    ]
    for name, m in (
        ("score_ma5 > 3", score_ma5 > 3),
        ("score_ma5 > 3 ∧ nred_ma5 ≤ 1", (score_ma5 > 3) & (nred_ma5 <= 1)),
        ("score_ma5 ≤ -1", score_ma5 <= -1),
        ("nred_ma5 ≤ 1", nred_ma5 <= 1),
        ("nred_ma5 ≥ 2", nred_ma5 >= 2),
        ("nred_ma5 ≥ 2.4 (persistently dirty)", nred_ma5 >= 2.4),
        ("score>3 ∧ RSI≥60 (looks strong)", hi & (rsi >= 60)),
        ("score≤-1 ∧ RSI<40", (df["score"] <= -1) & (rsi < 40)),
    ):
        lines.append(
            f"| {name} | {int(m.sum()):,} | "
            + " | ".join(cell(m, h) for h in horizons)
            + " |"
        )

    above, dist = _join_above_sma50(df)
    if above is not None:
        above_b = above.fillna(False)
        lines += [
            "",
            "SMA50 reconstructed PIT from `data/prices/ohlc.parquet` (same rule as A09):",
            "",
            "| slice | n | 1d | 3d | 1w | 2m |",
            "|-------|--:|---:|---:|---:|---:|",
        ]
        for name, m in (
            ("above SMA50 (any score)", above_b),
            ("below SMA50 (any score)", ~above_b & dist.notna()),
            ("score>3 ∧ above SMA50", hi & above_b),
            ("score>3 ∧ below SMA50", hi & ~above_b & dist.notna()),
            ("score>3 ∧ allgreen ∧ above50", hi & allgreen & above_b),
            ("dist50 < -8% (well below)", dist < -0.08),
            ("dist50 ≥ 8% (extended / looks best)", dist >= 0.08),
            ("score>3 ∧ dist50 ≥ 8%", hi & (dist >= 0.08)),
            ("score_ma5 > 3 ∧ above50", (score_ma5 > 3) & above_b),
            ("score_ma5 ≤ -1 ∧ below50", (score_ma5 <= -1) & ~above_b & dist.notna()),
        ):
            lines.append(
                f"| {name} | {int(m.sum()):,} | "
                + " | ".join(cell(m, h) for h in horizons)
                + " |"
            )

    lines.append("")
    lines.append(
        "Read the (pp) as lift vs that base. Near 0 = no edge. "
        "2m requires 42 bars so the last ~8 weeks of asof are dropped. "
        "B1 fundamentals only exist on/after a Finviz export. "
        "P04 (sec↑/sec↓) is ⚪ until the first sector board. "
        "n_red_ctx≥3 needs P03, so it only exists after the first Finviz export."
    )
    lines.append("")
    return lines


def _universe_daily_trail(out: pd.DataFrame) -> list[str]:
    """BB-style daily trail, one row per session across the liquid universe."""
    if out is None or out.empty:
        return ["## Universe — daily trail (every session)", "", "_no rows_", ""]
    df = out.copy()
    df["_asof"] = df["asof_date"].astype(str).str[:10]
    lines = [
        "## Universe — daily trail (every session)",
        "",
        "Same table as a single-ticker trail, rolled up: **median score / ctx / enr** "
        "and % of names with LEAD / peers↑ / ind↑ that day. "
        "Median score is market weather, not the cut — see audit G for score>3 + context. "
        "1d 3d 1w 2m dots are the universe-wide 🟢 rate that day (🟢 if ≥50%). "
        "Per-ticker rows live in the parquet — markdown cannot hold ~2,500 × N sessions.",
        "",
        "| date | n | score | ctx | enr | LEAD | peers↑ | ind↑ | 1d | 3d | 1w | 2m |",
        "|------|--:|------:|----:|----:|-----:|-------:|-----:|:--:|:--:|:--:|:--:|",
    ]
    for date, day in df.groupby("_asof", sort=True):
        n = len(day)
        sc = day["score"].median()
        cx = day["score_context"].median()
        en = day["score_enriched"].median()
        lead = (day["P01"] == 1).mean()
        p2 = (day["P02"] == 1).mean()
        p3 = (day["P03"] == 1).mean()
        cells = []
        for h in ("1d", "3d", "1w", "2m"):
            sub = day
            bars_col = f"bars_{h}"
            if bars_col in day.columns:
                b = pd.to_numeric(day[bars_col], errors="coerce").fillna(0)
                sub = day[b >= HORIZONS[h]]
            v = pd.to_numeric(sub[f"fav_{h}"], errors="coerce").dropna()
            if len(v) < 5:
                cells.append("⚪")
            else:
                r = float(v.mean())
                cells.append(f"{_dot(r)} {100 * r:.0f}%")
        lines.append(
            f"| {date} | {n:,} | {_score_cell(sc)} | {int(round(cx)):+d} | "
            f"{_score_cell(en)} | {100 * lead:.0f}% | {100 * p2:.0f}% | {100 * p3:.0f}% | "
            + " | ".join(cells)
            + " |"
        )
    lines.append("")
    return lines


def _universe_scoreboard(out: pd.DataFrame) -> list[str]:
    """Latest-session who (the BB trail is one name; universe needs a snapshot)."""
    if out is None or out.empty:
        return ["## Latest session snapshot", "", "_no rows_", ""]
    asof_s = out["asof_date"].astype(str).str[:10]
    latest = asof_s.max()
    day = out.loc[asof_s == latest].copy()
    n = len(day)
    lines = [
        f"## Latest session `{latest}` — who (top / bottom enriched)",
        "",
        f"- names: **{n:,}**",
        f"- score median **{day['score'].median():.0f}** "
        f"(p10={day['score'].quantile(0.1):.0f}, p90={day['score'].quantile(0.9):.0f})",
        f"- ctx median **{day['score_context'].median():.0f}** · "
        f"enr median **{day['score_enriched'].median():.0f}**",
        f"- LEAD {(day['P01'] == 1).mean():.0%} · peers↑ {(day['P02'] == 1).mean():.0%} · "
        f"ind↑ {(day['P03'] == 1).mean():.0%} · sec↑ {(day['P04'] == 1).mean():.0%}",
        "",
        "### Top 25 by enriched score",
        "",
        "| Ticker | score | ctx | enr | label | rsi | sector | 1w |",
        "|--------|------:|----:|----:|-------|----:|--------|:--:|",
    ]
    top = day.sort_values("score_enriched", ascending=False).head(25)
    for _, r in top.iterrows():
        rsi = r.get("rsi")
        rsi_s = f"{float(rsi):.0f}" if rsi is not None and np.isfinite(rsi) else "—"
        lines.append(
            f"| {r['Ticker']} | {_score_cell(r['score'])} | {int(r['score_context']):+d} | "
            f"{_score_cell(r['score_enriched'])} | {r.get('context_label') or ''} | "
            f"{rsi_s} | {r.get('sector') or '—'} | {_dot(r.get('fav_1w'))} |"
        )
    lines += [
        "",
        "### Bottom 15 by enriched score",
        "",
        "| Ticker | score | ctx | enr | label | sector |",
        "|--------|------:|----:|----:|-------|--------|",
    ]
    bot = day.sort_values("score_enriched", ascending=True).head(15)
    for _, r in bot.iterrows():
        lines.append(
            f"| {r['Ticker']} | {_score_cell(r['score'])} | {int(r['score_context']):+d} | "
            f"{_score_cell(r['score_enriched'])} | {r.get('context_label') or ''} | "
            f"{r.get('sector') or '—'} |"
        )
    lines.append("")
    return lines


def write_report(
    out: pd.DataFrame,
    start: str,
    end: str,
    tag: str,
    *,
    ticker: str | None = None,
    first_exp: str | None = None,
    last_exp: str | None = None,
    board_dates: list | None = None,
    sector_name: str | None = None,
    industry_name: str | None = None,
    extra_bullets: list[str] | None = None,
) -> Path:
    """Write the BB-style markdown + json next to the parquet. Returns md path."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = artifact_stem(start, end, tag)
    parquet = OUT_DIR / f"{stem}.parquet"
    board_dates = board_dates or []
    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}** · actual `{out['asof_date'].min() if len(out) else '—'}` → `{out['asof_date'].max() if len(out) else '—'}`",
        f"- Finviz exports: `{first_exp or '—'}` → `{last_exp or '—'}` (B1 only when export ≤ asof)",
        f"- Sector boards on disk: **{len(board_dates)}** "
        f"({board_dates[0] if board_dates else '—'} → {board_dates[-1] if board_dates else '—'}; P04=⚪ before first board)",
        f"- Peers: Correlations map × price_store 5d returns (PIT)",
        f"- Industry: 5d median of industry roster (from latest export membership)",
    ]
    if extra_bullets:
        lines.extend(extra_bullets)
    lines += [
        "",
        "## Legend — scores",
        "",
        "| col | meaning |",
        "|-----|---------|",
        "| score | Sum of AB Part A + B1 feature flags **as of that day** |",
        "| ctx | Sum of context flags P01+P02+P03+P04 that day |",
        "| enr | score + ctx |",
        "| color on score/enr | green if >0, red if <0, white if 0 |",
        "| 1d 3d 1w 2m | forward max-upside vs |max-downside| from that day's close |",
        "",
        "## Legend — P01 to P04 (context flags)",
        "",
        "| Flag | Name | +1 (green) | -1 (red) | Data source |",
        "|------|------|------------|----------|-------------|",
        "| **P01** | Peer lead / lag | stock 5d - peer-median 5d > 0 and beats >=50% peers | lags peers | Correlations peers + price_store OHLC <= asof |",
        "| **P02** | Peers advancing | peer-basket median 5d > 0 | median 5d < 0 | same peer set |",
        "| **P03** | Industry advancing | industry median 5d > 0 | median 5d < 0 | Finviz Industry roster + price_store |",
        "| **P04** | Sector supportive | sector board Dir=up | Dir=down | nearest `01_daily/sectors/<board_date>/_BOARD.md` with board_date <= asof |",
        "",
        "## Legend — Finviz chart E / R markers",
        "",
        "| Marker | Meaning | Green | Red | Source |",
        "|--------|---------|-------|-----|--------|",
        "| **E** | Earnings (chart E) | EPS beat | EPS miss | yfinance earnings dates, PIT <= asof |",
        "| **R** | Analyst action (chart R) | Upgrade | Downgrade | yfinance recommendations |",
        "",
        "Trail shows the most recent E and R on or before that day.",
    ]
    if not ticker:
        lines.append("Universe runs skip E/R prefetch (too slow); those columns are ⚪.")
    lines += [
        "",
        "Label chips: green LEAD / red LAG · peers up/down · ind up/down · sec up/down (white = neutral/no data).",
        "",
    ]

    if len(out) and not ticker:
        lines.extend(_universe_daily_trail(out))
        lines.extend(_universe_scoreboard(out))

    if len(out) and ticker:
        lines += [
            f"## {ticker} — daily trail (every session)",
            "",
            "| date | score | ctx | enr | context | E | R | rs5d | sec | board_date | 1d | 3d | 1w | 2m |",
            "|------|-------|----:|-----|---------|---|---|------|-----|------------|:--:|:--:|:--:|:--:|",
        ]
        for _, r in out.sort_values("asof_date").iterrows():
            rs = r.get("rs_5d")
            bd = r.get("sector_board_date")
            if bd is None or (isinstance(bd, float) and not np.isfinite(bd)):
                bd = "—"
            lines.append(
                f"| {r['asof_date']} | {_score_cell(r['score'])} | {int(r['score_context']):+d} | "
                f"{_score_cell(r['score_enriched'])} | {_label_colored(r)} | "
                f"{_er_chip('E', r.get('last_E_color'), r.get('last_E_label'), r.get('last_E_surprise'), r.get('days_since_E'))} | "
                f"{_er_chip('R', r.get('last_R_color'), r.get('last_R_label'), None, r.get('days_since_R'))} | "
                f"{(f'{float(rs):+.1%}' if rs is not None and np.isfinite(rs) else '—')} | "
                f"{_sec_dot(r.get('sector_dir'))} | {bd} | "
                f"{_dot(r.get('fav_1d'))} | {_dot(r.get('fav_3d'))} | "
                f"{_dot(r.get('fav_1w'))} | {_dot(r.get('fav_2m'))} |"
            )
        lines += [
            "",
            "### Sector source footnote (P04)",
            "",
            f"- Ticker **{ticker}** sector **`{sector_name or '—'}`**, industry **`{industry_name or '—'}`** "
            f"(from latest Finviz export roster).",
            "- Rule: each asof day uses the **latest** `board_date <= asof`. If none, sec is white and P04=0.",
            "",
            "| board_date | file | sector row | Dir | Score |",
            "|------------|------|------------|-----|------:|",
        ]
        used = out.dropna(subset=["sector_board_date"]) if "sector_board_date" in out.columns else out.iloc[0:0]
        if len(used):
            seen = set()
            for _, r in used.sort_values("sector_board_date").iterrows():
                bd = r.get("sector_board_date")
                if not bd or bd in seen:
                    continue
                seen.add(bd)
                sc = r.get("sector_score")
                sc_s = sc if (sc is not None and np.isfinite(sc)) else "—"
                lines.append(
                    f"| {bd} | `01_daily/sectors/{bd}/_BOARD.md` | **{r.get('sector') or sector_name or '—'}** | "
                    f"{r.get('sector_dir') or '—'} | {sc_s} |"
                )
        else:
            lines.append("| — | _(no sector board <= any asof)_ | — | — | — |")
        lines += ["", "All sector board files on disk at run time:"]
        if board_dates:
            for bd in board_dates:
                lines.append(f"- `01_daily/sectors/{bd}/_BOARD.md`")
        else:
            lines.append("- _(none found)_")

    if len(out):
        lines.append("")
        lines.extend(_pattern_audit(out))

    lines += ["", f"- parquet: `{parquet.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{stem}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    (OUT_DIR / f"{stem}.json").write_text(
        json.dumps(
            {
                "start": start,
                "end": end,
                "ticker": ticker,
                "n_rows": int(len(out)),
                "sector_boards": board_dates,
                "export_first": first_exp,
                "export_last": last_exp,
                "generated": datetime.now(ET).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[backfill] wrote {md} ({len(out):,} rows)", flush=True)
    return md


def rebuild_md_from_parquet(path: str) -> None:
    """Rebuild BB-style MD from an existing PIT parquet (no rescoring)."""
    p = Path(path)
    if not p.is_file():
        p = ROOT / path
    if not p.is_file():
        raise SystemExit(f"[backfill] parquet not found: {path}")
    out = pd.read_parquet(p)
    name = p.name.replace(".resume.parquet", ".parquet").replace(".parquet", "")
    m = re.match(r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(.+)$", name)
    if not m:
        raise SystemExit(f"[backfill] cannot parse start/end/tag from {p.name}")
    start, end, tag = m.group(1), m.group(2), m.group(3)
    ticker = None if tag == "universe" else tag
    boards = ctx.load_sector_boards()
    board_dates = [d for d, _ in boards]
    exports = _load_exports()
    exp_dates = [d for d, _ in exports]
    try:
        rel = str(p.resolve().relative_to(ROOT))
    except Exception:
        rel = str(p)
    extra = [f"- rebuilt MD from `{rel}` (no rescoring)"]
    if ".resume." in p.name:
        extra.append("- source is a **resume checkpoint** (PIT still in progress; last asof may be short of `end`)")
    write_report(
        out, start, end, tag,
        ticker=ticker,
        first_exp=exp_dates[0] if exp_dates else None,
        last_exp=exp_dates[-1] if exp_dates else None,
        board_dates=board_dates,
        extra_bullets=extra,
    )
    if tag == "universe" and len(out):
        cut = (pd.Timestamp(end) - pd.DateOffset(months=6)).date().isoformat()
        asof_s = out["asof_date"].astype(str).str[:10]
        six = out.loc[asof_s >= cut].copy()
        if len(six) and str(asof_s.min())[:10] < cut:
            six_pq = OUT_DIR / f"{cut}_{end}_universe.parquet"
            six.to_parquet(six_pq, index=False)
            write_report(
                six, cut, end, "universe",
                ticker=None,
                first_exp=exp_dates[0] if exp_dates else None,
                last_exp=exp_dates[-1] if exp_dates else None,
                board_dates=board_dates,
                extra_bullets=[f"- 6-month slice of `{name}.parquet`"],
            )



def run(
    start: str | None = None,
    end: str | None = None,
    months: int | None = None,
    ticker: str | None = None,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
    skip_events: bool | None = None,
) -> pd.DataFrame:
    exports = _load_exports()
    exp_dates = [d for d, _ in exports]
    first_exp = exp_dates[0] if exp_dates else None
    last_exp = exp_dates[-1] if exp_dates else None

    end = end or (last_exp or datetime.now(ET).date().isoformat())
    if start is None:
        m = 6 if months is None else int(months)
        start = (pd.Timestamp(end) - pd.DateOffset(months=m)).date().isoformat()

    corr_map = ctx.load_corr_map()
    boards = ctx.load_sector_boards()
    board_dates = [d for d, _ in boards]
    print(f"[backfill] requested {start} → {end}")
    print(f"[backfill] exports n={len(exports)} {first_exp}→{last_exp}")
    print(f"[backfill] sector boards n={len(boards)} {board_dates[0] if board_dates else '—'}→{board_dates[-1] if board_dates else '—'}")
    print(f"[backfill] corr map tickers={len(corr_map):,}")
    if skip_events is None:
        skip_events = ticker is None
    events_by_ticker: dict[str, pd.DataFrame] = {}

    peer_list = corr_map.get(ticker.upper(), []) if ticker else []
    # also bootstrap a sample of industry peers for industry median
    sector_name, industry_name = (None, None)
    if ticker:
        sector_name, industry_name = ctx.ticker_sector_industry_from_latest_export(ticker)
    ind_members = ctx.industry_members_from_latest_export()
    ind_tickers = ind_members.get(industry_name or "", [])[:40]

    _ensure_price_coverage(start, end, ticker, peer_list + ind_tickers)

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[backfill] empty price store")

    all_days = sorted(pd.to_datetime(store["date"]).unique())
    days = [d.date().isoformat() for d in all_days if start <= d.date().isoformat() <= end]
    if not days:
        raise SystemExit("[backfill] no sessions in window")

    form4 = None
    if INS_PANEL.exists():
        form4 = pd.read_parquet(INS_PANEL)
    elif INS_PANEL_CSV.exists():
        form4 = pd.read_csv(INS_PANEL_CSV)

    store = store.copy()
    store["date"] = pd.to_datetime(store["date"])
    store["ticker"] = store["ticker"].astype(str).str.upper()
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    if ticker:
        tickers = [ticker.upper()]
    else:
        latest_df = exports[-1][1] if exports else None
        if latest_df is not None:
            tickers = ab._filter_liquid(latest_df)["Ticker"].astype(str).str.upper().tolist()
        else:
            tickers = sorted(groups.keys())

    tag = ticker.upper() if ticker else "universe"
    stop = install_stop_handlers(_StopFlag())
    prior, resume_info = load_resume_checkpoint(start, end, tag)
    last_asof = (resume_info or {}).get("last_asof") if resume_info else None
    days_todo = remaining_days(days, last_asof)
    rows: list[dict] = prior.to_dict("records") if len(prior) else []
    print(f"[backfill] sessions={len(days)} ({days[0]}→{days[-1]}) names={len(tickers)}")
    if last_asof:
        print(
            f"[backfill] RESUME after {last_asof} kept_rows={len(rows):,} "
            f"remaining={len(days_todo)}/{len(days)} "
            f"(file {resume_stem(start, end, tag)}.parquet)"
        )
    else:
        print("[backfill] no resume checkpoint — starting at day 1")
    if ticker:
        print(f"[backfill] {ticker}: sector={sector_name} industry={industry_name} peers={len(peer_list)}")

    # Prefetch E/R only for single-ticker runs. Universe prefetch is ~2.5k
    # yfinance calls and is what blew the 6h cap before day 1 of resume.
    if skip_events:
        print("[backfill] skip E/R yfinance prefetch (--skip-events / universe)")
    else:
        for _t in tickers:
            try:
                ev = em.fetch(_t)
                events_by_ticker[_t] = ev
                if len(ev):
                    em.save(_t, ev)
                nE = int((ev["kind"] == "E").sum()) if len(ev) else 0
                nR = int((ev["kind"] == "R").sum()) if len(ev) else 0
                print(f"[backfill] events {_t}: E={nE} R={nR}")
            except Exception as e:
                print(f"[backfill] events {_t} skip: {e}")
                events_by_ticker[_t] = pd.DataFrame()

    n_ohlc_only = 0
    n_with_export = 0
    done_before = len(days) - len(days_todo)
    for j, asof in enumerate(days_todo, 1):
        i = done_before + j
        exp_date, exp_df = _export_asof(exports, asof)
        exp_idx = exp_df.set_index("Ticker") if exp_df is not None else None

        for t in tickers:
            g = groups.get(t)
            if g is None:
                continue
            ohlc_to = g[g.index <= pd.Timestamp(asof)]
            if ohlc_to.empty or len(ohlc_to) < 25:
                continue

            row = None
            prior_row = None
            prior_d = None
            if exp_idx is not None and t in exp_idx.index:
                row = exp_idx.loc[t]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                prior_row, prior_d = _prior_export_row(exports, exp_date or asof, t)
                n_with_export += 1
                sec_n = str(row.get("Sector")) if "Sector" in row.index else sector_name
                ind_n = str(row.get("Industry")) if "Industry" in row.index else industry_name
            else:
                n_ohlc_only += 1
                sec_n, ind_n = sector_name, industry_name

            a = ab._part_a(ohlc_to)
            if row is not None:
                b = ab._part_b1(row, prior_row, prior_d)
                pb = ab._pass_b1(b)
                mode = "export"
            else:
                b = _empty_b1()
                pb = {k: 0 for k in ab.FEATURE_ORDER if k.startswith("B")}
                mode = "ohlc_only_no_export"

            pa = ab._pass_a(a)
            setup_flag, setup_val = _setup_flag(a, b)
            flags = {**pa, **pb, "A14_profitable_oversold_setup": setup_flag}
            f4 = _form4_asof(form4, t, asof)
            flags["B15_form4_insider"] = f4["flag"]
            score = int(sum(int(v) for v in flags.values()))

            peer = ctx.peer_context_asof(t, asof, groups, corr_map, n=5)
            indc = ctx.industry_context_asof(ind_n, asof, groups, ind_members, n=5)
            secc = ctx.sector_context_asof(sec_n, asof, boards)
            esnap = em.asof_snapshot(events_by_ticker.get(t, pd.DataFrame()), asof)

            p01, p02, p03, p04 = peer["P01"], peer["P02"], indc["P03"], secc["P04"]
            score_ctx = int(p01 + p02 + p03 + p04)
            label = ctx.context_label(p01, p02, p03, p04)

            pair = (a.get("pair") or {}) if a.get("ok") else {}
            entry = a.get("price") if a.get("ok") else np.nan
            fwd = _forward_multi(g, asof, float(entry) if np.isfinite(entry) else np.nan)

            rows.append({
                "Ticker": t,
                "asof_date": asof,
                "score": score,
                "score_context": score_ctx,
                "score_enriched": score + score_ctx,
                "score_mode": mode,
                "context_label": label,
                "rs_5d": peer["rs_5d"],
                "own_5d": peer["own_5d"],
                "peer_med_5d": peer["peer_med_5d"],
                "beat_pct_5d": peer["beat_pct_5d"],
                "n_peers_used": peer["n_peers_used"],
                "P01": p01,
                "P02": p02,
                "ind_med_5d": indc["ind_med_5d"],
                "P03": p03,
                "sector": sec_n,
                "industry": ind_n,
                "sector_dir": secc["sector_dir"],
                "sector_score": secc["sector_score"],
                "sector_board_date": secc["sector_board_date"],
                "P04": p04,
                "last_E_date": esnap.get("last_E_date"),
                "last_E_color": esnap.get("last_E_color"),
                "last_E_label": esnap.get("last_E_label"),
                "last_E_surprise": esnap.get("last_E_surprise"),
                "days_since_E": esnap.get("days_since_E"),
                "flag_E": esnap.get("flag_E", 0),
                "last_R_date": esnap.get("last_R_date"),
                "last_R_color": esnap.get("last_R_color"),
                "last_R_label": esnap.get("last_R_label"),
                "days_since_R": esnap.get("days_since_R"),
                "flag_R": esnap.get("flag_R", 0),
                "rsi": a.get("rsi") if a.get("ok") else np.nan,
                "price": entry,
                "setup_flag": setup_flag,
                "pair_day_a": pair.get("d_a"),
                "pair_day_b": pair.get("d_b"),
                "body_rg_2day": pair.get("body_rg"),
                "export_used": exp_date or "none",
                **fwd,
            })

        if i % 20 == 0 or i == len(days):
            print(f"[backfill] {asof} ({i}/{len(days)}) rows={len(rows):,}", flush=True)
        should_ckpt = bool(checkpoint_every) and (
            j % int(checkpoint_every) == 0 or stop.requested
        )
        if should_ckpt and rows:
            write_resume_checkpoint(
                rows, start, end, tag, asof, i, len(days),
            )
        if stop.requested:
            print(
                "[backfill] stopped with resume checkpoint — "
                "re-run the same --months/--end to continue",
                flush=True,
            )
            return pd.DataFrame(rows)

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = artifact_stem(start, end, tag)
    parquet = OUT_DIR / f"{stem}.parquet"
    out.to_parquet(parquet, index=False)
    if ticker or len(out) < 500_000:
        out.to_csv(OUT_DIR / f"{stem}.csv", index=False)

    write_report(
        out, start, end, tag,
        ticker=ticker,
        first_exp=first_exp,
        last_exp=last_exp,
        board_dates=board_dates,
        sector_name=sector_name,
        industry_name=industry_name,
    )
    print(f"[backfill] wrote {parquet} rows={len(out):,}")

    # Always emit a 6-month slice so the "past 6 months" view exists even when
    # we resumed a 12-month checkpoint (stem 2025-08-19_2026-08-19_universe).
    if tag == "universe" and len(out):
        cut = (pd.Timestamp(end) - pd.DateOffset(months=6)).date().isoformat()
        asof_s = out["asof_date"].astype(str).str[:10]
        six = out.loc[asof_s >= cut].copy()
        if len(six) and str(asof_s.min())[:10] < cut:
            six_pq = OUT_DIR / f"{cut}_{end}_universe.parquet"
            six.to_parquet(six_pq, index=False)
            write_report(
                six, cut, end, "universe",
                ticker=None,
                first_exp=first_exp,
                last_exp=last_exp,
                board_dates=board_dates,
                extra_bullets=[f"- 6-month slice of `{stem}.parquet`"],
            )
            print(f"[backfill] wrote 6m slice {six_pq} rows={len(six):,}")

    clear_resume_checkpoint(start, end, tag)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--months", type=int, default=None)
    ap.add_argument("--ticker", default=None)
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=DEFAULT_CHECKPOINT_EVERY,
        help="Write {start}_{end}_{tag}.resume.parquet every N asof days "
             "(0 = only on SIGTERM). Default 10.",
    )
    ap.add_argument(
        "--skip-events",
        action="store_true",
        default=None,
        help="Skip yfinance E/R prefetch (default for universe).",
    )
    ap.add_argument(
        "--events",
        action="store_true",
        help="Force E/R prefetch even on universe (slow).",
    )
    ap.add_argument(
        "--md-only",
        default=None,
        metavar="PARQUET",
        help="Rebuild BB-style MD + 6m slice from an existing parquet (no rescoring).",
    )
    args = ap.parse_args()
    if args.md_only:
        rebuild_md_from_parquet(args.md_only)
        return
    skip_events = True if args.skip_events else (False if args.events else None)
    run(
        start=args.start,
        end=args.end,
        months=args.months,
        ticker=args.ticker,
        checkpoint_every=args.checkpoint_every,
        skip_events=skip_events,
    )


if __name__ == "__main__":
    main()
