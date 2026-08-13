"""Deep labeling — decompose every stock in the Finviz universe into its
full segment tag bag ("what is it?"), and aggregate each segment's stats
("what kind of market is this?").

Labels are assigned ONLY from columns already present in the Finviz Elite
export, using bin edges from 00_grounding/segments.json (the registry —
retune thresholds there, not here). All labels follow one format,
<family>:<value>, minted by this single module, so keyword scanning on any
downstream artifact is safe.

This is Step A (LABEL) of the label → regime → join design. Whether today
is good for a label is the weather engine's job (src/weather.py). Whether
it made money is the backtest's job. Neither happens here.

Input resolution order:
  1. --csv PATH                              explicit file
  2. newest data/exports/finviz_????-??-??.csv   (weekday Elite archives)
  3. data/finviz/latest.csv                  (fallback)

Outputs:
  data/universe/<date>_membership.csv    one row per ticker, one column per
                                         family, themes pipe-joined
  data/universe/<date>_segment_stats.csv one row per segment_id: n, breadth,
                                         median momentum/RSI/beta/short, etc.
  01_daily/<date>_universe.md            human coverage + distribution report

CLI:
  python -m src.segments                        # newest export
  python -m src.segments --csv data/exports/finviz_2026-08-13.csv
  python -m src.segments --date 2026-08-13      # label as a specific date
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
EXPORTS_DIR = ROOT / "data" / "exports"
FALLBACK_CSV = ROOT / "data" / "finviz" / "latest.csv"
UNIVERSE_DIR = ROOT / "data" / "universe"
DAILY_DIR = ROOT / "01_daily"
REGISTRY_PATH = ROOT / "00_grounding" / "segments.json"
ET = ZoneInfo("America/New_York")

FAMILY_COLS = ["sector", "industry", "size", "index", "geo", "beta", "short",
               "liq", "rvol", "vol", "profit", "lev", "style", "mom", "ext",
               "range", "earn", "earnsurp", "analyst"]


# ---------------------------------------------------------------- loading

def _to_float(v: object) -> float:
    """Finviz Elite export mixes floats, '19.55%', '-', and NaN."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace("%", "").replace(",", "").strip()
    if s in ("", "-", "--", "N/A"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def load_export(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df[df["Ticker"].notna()].copy()
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    num_cols = ["Market Cap", "Price", "Average Volume", "Volume", "Beta",
                "Short Float", "Relative Volume", "Average True Range",
                "Profit Margin", "Total Debt/Equity", "Forward P/E",
                "Sales Year Over Year TTM", "EPS Growth Next Year",
                "Performance (Week)", "Performance (Month)",
                "Performance (Quarter)", "Performance (YTD)",
                "Relative Strength Index (14)",
                "20-Day Simple Moving Average", "50-Day Simple Moving Average",
                "200-Day Simple Moving Average",
                "52-Week High", "52-Week Low", "EPS Surprise",
                "Revenue Surprise", "Analyst Recom", "Target Price",
                "Volatility (Week)", "Volatility (Month)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].map(_to_float)
    # Business descriptions are static text — when the fresh export lacks
    # them (the automated views don't serve Finviz_Description), graft the
    # column from the committed full export by Ticker.
    if "Finviz_Description" not in df.columns and FALLBACK_CSV.exists():
        try:
            desc = pd.read_csv(FALLBACK_CSV, usecols=["Ticker", "Finviz_Description"])
            desc["Ticker"] = desc["Ticker"].astype(str).str.upper().str.strip()
            df = df.merge(desc.drop_duplicates("Ticker"), on="Ticker", how="left")
        except Exception:
            pass
    return df


def resolve_input(csv_arg: str | None) -> tuple[Path, str]:
    """Returns (path, date_str). Date comes from filename when possible."""
    if csv_arg:
        p = Path(csv_arg)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        date_str = m.group(1) if m else datetime.now(ET).date().isoformat()
        return p, date_str
    archives = sorted(EXPORTS_DIR.glob("finviz_????-??-??.csv"))
    if archives:
        p = archives[-1]
        return p, re.search(r"(\d{4}-\d{2}-\d{2})", p.name).group(1)
    if FALLBACK_CSV.exists():
        return FALLBACK_CSV, datetime.now(ET).date().isoformat()
    raise SystemExit("[segments] no Finviz export found "
                     f"(looked in {EXPORTS_DIR} and {FALLBACK_CSV})")


def _load_registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- labeling

def _bin(value: pd.Series, bins: list) -> pd.Series:
    """Vectorized bin assignment; NaN -> 'unknown'."""
    out = pd.Series("unknown", index=value.index)
    for label, lo, hi in bins:
        m = pd.Series(True, index=value.index)
        if lo is not None:
            m &= value >= lo
        if hi is not None:
            m &= value < hi
        out = out.where(~(m & value.notna()), label)
    return out


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df.get(name, pd.Series(np.nan, index=df.index))


def assign_membership(df: pd.DataFrame, registry: dict) -> pd.DataFrame:
    fam = registry["families"]
    m = pd.DataFrame(index=df.index)
    m["Ticker"] = df["Ticker"]

    m["sector"] = df.get("Sector", pd.Series("unknown", index=df.index)).fillna("unknown")
    m["industry"] = df.get("Industry", pd.Series("unknown", index=df.index)).fillna("unknown")

    m["size"] = _bin(_col(df, "Market Cap") * fam["size"].get("scale", 1),
                     fam["size"]["bins"])

    idx_map = fam["index"]["map"]

    def _index_tags(v: object) -> str:
        tags = [idx_map[t.strip()] for t in str(v).split(",")
                if t.strip() in idx_map]
        return "|".join(tags) if tags else fam["index"]["none_value"]

    m["index"] = df.get("Index", pd.Series("-", index=df.index)).map(_index_tags)

    country = df.get("Country", pd.Series("", index=df.index)).fillna("")
    m["geo"] = np.where(country == fam["geo"]["domestic"], "US",
                        "ADR-" + country.where(country != "", "unknown"))

    m["beta"] = _bin(_col(df, "Beta"), fam["beta"]["bins"])
    m["short"] = _bin(_col(df, "Short Float"), fam["short"]["bins"])

    dollar_adv = (_col(df, "Price") * _col(df, "Average Volume")
                  * fam["liq"].get("avg_volume_scale", 1))
    m["liq"] = _bin(dollar_adv, fam["liq"]["bins"])

    m["rvol"] = _bin(_col(df, "Relative Volume"), fam["rvol"]["bins"])

    atr_pct = (_col(df, "Average True Range") / _col(df, "Price") * 100)
    m["vol"] = _bin(atr_pct.replace([np.inf, -np.inf], np.nan),
                    fam["vol"]["bins"])

    m["profit"] = _bin(_col(df, "Profit Margin"), fam["profit"]["bins"])

    de = _col(df, "Total Debt/Equity")
    lev = _bin(de.where(de >= 0), fam["lev"]["bins"])
    m["lev"] = np.where(de < 0, fam["lev"]["negative_value"], lev)

    sales = _col(df, "Sales Year Over Year TTM")
    epsg = _col(df, "EPS Growth Next Year")
    fpe = _col(df, "Forward P/E")
    growth = (sales >= 15) | (epsg >= 20)
    value = (fpe <= 15) & (sales < 10)
    m["style"] = np.where(growth, "growth", np.where(value, "value", "blend"))
    m["style"] = pd.Series(m["style"], index=df.index).where(
        ~(sales.isna() & epsg.isna() & fpe.isna()), "unknown")

    sma50 = _col(df, "50-Day Simple Moving Average")
    sma200 = _col(df, "200-Day Simple Moving Average")
    mom = np.where((sma50 > 0) & (sma200 > 0), "uptrend",
                   np.where((sma50 < 0) & (sma200 < 0), "downtrend", "mixed"))
    m["mom"] = pd.Series(mom, index=df.index).where(
        ~(sma50.isna() | sma200.isna()), "unknown")

    pw = _col(df, "Performance (Week)")
    pm = _col(df, "Performance (Month)")
    rsi = _col(df, "Relative Strength Index (14)")
    extreme = (pw >= 100) | (sma50 >= 40)
    extended = (pw >= 40) | (rsi >= 75)
    washed = (pm <= -25) | (rsi <= 30)
    ext = np.where(extreme, "extreme",
                   np.where(extended, "extended",
                            np.where(washed, "washed", "neutral")))
    m["ext"] = pd.Series(ext, index=df.index).where(
        ~(pw.isna() & pm.isna() & rsi.isna() & sma50.isna()), "unknown")

    # --- fib 52w zones (Finviz stores 52w fields as % distances) ---
    hi_d = _col(df, "52-Week High") / 100.0   # % below high (<= 0)
    lo_d = _col(df, "52-Week Low") / 100.0    # % above low (>= 0)
    price = _col(df, "Price")
    with np.errstate(divide="ignore", invalid="ignore"):
        h52 = price / (1 + hi_d)
        l52 = price / (1 + lo_d)
        x = ((price - l52) / (h52 - l52)).replace([np.inf, -np.inf], np.nan)
    rng = _bin(x, fam["range"]["zones"])
    m["range"] = np.where(hi_d >= 0, fam["range"]["breakout_value"], rng)

    # --- earnings proximity (past dates are NOT "today"; see earnsurp) ---
    ed = pd.to_datetime(df.get("Earnings Date"), errors="coerce",
                        format="mixed")
    today = pd.Timestamp.now(tz="America/New_York").normalize().tz_localize(None)
    delta = (ed.dt.normalize() - today).dt.days
    earn = np.where(delta < -1, "past",
                    np.where(delta <= 0, "today",
                             np.where(delta <= 7, "this_week", "later")))
    m["earn"] = pd.Series(earn, index=df.index).where(ed.notna(), "unknown")

    # --- most recent earnings surprise ---
    m["earnsurp"] = _bin(_col(df, "EPS Surprise"), fam["earnsurp"]["bins"])

    # --- analyst recommendation level ---
    m["analyst"] = _bin(_col(df, "Analyst Recom"), fam["analyst"]["bins"])

    # --- theme keyword overlay (v0) ---
    patterns = fam["themes"]["patterns"]

    def _text_col(name: str) -> pd.Series:
        if name in df.columns:
            return df[name].fillna("").astype(str)
        return pd.Series("", index=df.index)

    blob = (_text_col("Industry") + " "
            + _text_col("Finviz_Description") + " "
            + _text_col("News Title")).str.lower()
    theme_cols = {}
    for name, pat in patterns.items():
        theme_cols[f"theme:{name}"] = blob.str.contains(pat, regex=True, na=False)

    def _themes(i) -> str:
        return "|".join(t for t, hit in theme_cols.items() if hit.iat[i])

    m["themes"] = [_themes(i) for i in range(len(df))]
    m["n_themes"] = m["themes"].map(lambda s: 0 if not s else s.count("|") + 1)

    return m


# ---------------------------------------------------------------- stats

def segment_stats(df: pd.DataFrame, mem: pd.DataFrame) -> pd.DataFrame:
    j = df.set_index("Ticker").join(mem.set_index("Ticker"), rsuffix="_m")
    j["_dollar_adv"] = (j.get("Price", np.nan) * j.get("Average Volume", np.nan)
                        * 1000)
    rows = []

    def _add(family: str, value: str, mask: pd.Series) -> None:
        if not value or value == "unknown" or value != value:
            return
        sub = j[mask]
        n = len(sub)
        if n == 0:
            return
        rows.append({
            "segment_id": f"{family}:{value}",
            "family": family,
            "value": value,
            "n": n,
            "pct_universe": round(n / len(j) * 100, 2),
            "median_perf_week": float(sub["Performance (Week)"].median())
                if "Performance (Week)" in sub else np.nan,
            "median_perf_month": float(sub["Performance (Month)"].median())
                if "Performance (Month)" in sub else np.nan,
            "pct_above_50dma": float((sub["50-Day Simple Moving Average"] > 0).mean() * 100)
                if "50-Day Simple Moving Average" in sub else np.nan,
            "median_rsi": float(sub["Relative Strength Index (14)"].median())
                if "Relative Strength Index (14)" in sub else np.nan,
            "median_beta": float(sub["Beta"].median()) if "Beta" in sub else np.nan,
            "median_short_float": float(sub["Short Float"].median())
                if "Short Float" in sub else np.nan,
            "pct_profitable": float((sub["Profit Margin"] > 0).mean() * 100)
                if "Profit Margin" in sub else np.nan,
            "median_dollar_adv": float(sub["_dollar_adv"].median()),
        })

    for c in FAMILY_COLS:
        if c == "index":
            continue
        for value, _ in j.groupby(c):
            _add(c, str(value), j[c] == value)
    for col, family in (("index", "index"), ("themes", "themes")):
        exploded = j[col].astype(str).str.split("|").explode()
        exploded = exploded[exploded != ""]
        for value in exploded.unique():
            mask = j[col].astype(str).str.split("|").map(lambda xs: value in xs)
            _add(family, str(value), mask)

    return pd.DataFrame(rows).sort_values(["family", "n"],
                                          ascending=[True, False])


# ---------------------------------------------------------------- report

def _fmt_pct(x: float) -> str:
    return f"{x:.1f}%" if x == x else "n/a"


def write_report(date_str: str, mem: pd.DataFrame,
                 stats: pd.DataFrame) -> Path:
    n = len(mem)
    L = [f"# Universe deep labeling — {date_str}", "",
         f"Every one of the **{n:,}** tickers in the Finviz Elite export, "
         f"decomposed into its segment tag bag (`<family>:<value>`). "
         f"Labels answer *what is it?* — whether today favors a label is "
         f"the weather engine's job (`01_daily/weather/`), and whether it "
         f"made money is the backtest's job.", ""]

    L += ["## Coverage (per family)", "",
          "| Family | Labels seen | Unknown |", "|---|---|---|"]
    for c in FAMILY_COLS:
        unk = int((mem[c] == "unknown").sum())
        L.append(f"| {c} | {mem[c].nunique()} | {unk:,} ({unk / n * 100:.1f}%) |")
    L.append("")

    show = [("size", "Size"), ("beta", "Beta"), ("short", "Short float"),
            ("liq", "Liquidity ($ADV)"), ("vol", "Vol regime (ATR%)"),
            ("profit", "Profitability"), ("lev", "Leverage"),
            ("style", "Style"), ("mom", "Momentum trend"), ("ext", "Extension"),
            ("range", "52-week fib zone"), ("earn", "Earnings proximity"),
            ("earnsurp", "Last EPS surprise"), ("analyst", "Analyst stance"),
            ("rvol", "Relative volume"), ("geo", "Geography"),
            ("index", "Index membership"), ("sector", "Sector")]
    for col, title in show:
        vc = (mem[col] if col != "index"
              else mem[col].astype(str).str.split("|").explode()).value_counts()
        L += [f"### {title}", "", "| Label | Names | % |", "|---|---|---|"]
        for v, cnt in vc.items():
            L.append(f"| {v} | {cnt:,} | {cnt / n * 100:.1f}% |")
        L.append("")

    ind = stats[stats["family"] == "industry"].copy()
    if len(ind):
        big = ind[ind["n"] >= 20]
        for title, part in (("Hottest", big.nlargest(12, "median_perf_week")),
                            ("Coldest", big.nsmallest(12, "median_perf_week"))):
            L += [f"### {title} industries (median week perf, n≥20)", "",
                  "| Industry | n | Median wk | Median mo | % above 50DMA |",
                  "|---|---|---|---|---|"]
            for _, r in part.iterrows():
                L.append(f"| {r['value']} | {r['n']:,} | "
                         f"{_fmt_pct(r['median_perf_week'])} | "
                         f"{_fmt_pct(r['median_perf_month'])} | "
                         f"{_fmt_pct(r['pct_above_50dma'])} |")
            L.append("")

    themes = stats[stats["family"] == "themes"]
    if len(themes):
        L += ["### Theme tags", "", "| Theme | Names | Median wk |",
              "|---|---|---|"]
        for _, r in themes.iterrows():
            L.append(f"| {r['value']} | {r['n']:,} | "
                     f"{_fmt_pct(r['median_perf_week'])} |")
        L.append("")

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"{date_str}_universe.md"
    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------- main

def run(csv_path: Path, date_str: str, registry: dict) -> None:
    df = load_export(csv_path)
    mem = assign_membership(df, registry)
    stats = segment_stats(df, mem)

    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    mem_path = UNIVERSE_DIR / f"{date_str}_membership.csv"
    stats_path = UNIVERSE_DIR / f"{date_str}_segment_stats.csv"
    mem.to_csv(mem_path, index=False)
    stats.to_csv(stats_path, index=False)
    rep = write_report(date_str, mem, stats)
    print(f"[segments] {date_str}: {len(mem):,} tickers labeled, "
          f"{len(stats)} segments <- {csv_path.name} -> {mem_path.name}, "
          f"{stats_path.name}, {rep.name}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--date", default=None,
                    help="override output date (default: from filename or today ET)")
    args = ap.parse_args()
    csv_path, date_str = resolve_input(args.csv)
    if args.date:
        date_str = args.date
    run(csv_path, date_str, _load_registry())


if __name__ == "__main__":
    main()
