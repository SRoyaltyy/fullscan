"""Pre-flight integrity checks for every input the stock-book ranker consumes.

Latent failure modes this guards against (all previously silent):
  - a dated file missing so the loader falls back to a STALE file
    (finviz export, membership, ticker checklist)
  - an input present but degraded: row count collapsed vs its own recent
    history, all-zero scores, duplicate tickers
  - LLM layers (news judge, general/sector predicts) absent so their
    weight multiplies zeros with no record of it

Outputs
  data/stock_book/{date}_input_health.json   machine record (consumed by
                                             stock_book renorm + book_learn)
  printed table                              human read in the action log

CLI: python -m src.input_health [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from . import config, scoreboard

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "stock_book"

# status severity order (worst wins when aggregating)
_SEV = {"ok": 0, "degraded": 1, "stale": 2, "missing": 3}

# If a dated file's row count falls below this fraction of its trailing
# median (previous health snapshots), flag degraded.
ROW_COLLAPSE_FRACTION = 0.5
TRAILING_HEALTH_FILES = 10


def _rows_csv(path: Path) -> int:
    try:
        with open(path, encoding="utf-8", errors="ignore") as fh:
            return max(0, sum(1 for _ in fh) - 1)
    except OSError:
        return 0


def _trailing_medians(date: str) -> dict[str, float]:
    """Median row/coverage counts from previous health snapshots."""
    meds: dict[str, list[float]] = {}
    files = sorted(OUT_DIR.glob("????-??-??_input_health.json"))
    files = [p for p in files if p.name[:10] < date][-TRAILING_HEALTH_FILES:]
    for p in files:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for row in data.get("inputs") or []:
            n = row.get("n")
            if isinstance(n, (int, float)) and n > 0:
                meds.setdefault(row["name"], []).append(float(n))
    return {k: float(pd.Series(v).median()) for k, v in meds.items() if v}


def _mk(name: str, family: str, status: str, n: int | None = None,
        detail: str = "") -> dict:
    return {"name": name, "family": family, "status": status, "n": n,
            "detail": detail}


def check(date: str) -> dict:
    """Run all checks. Returns the health dict (also written to disk)."""
    meds = _trailing_medians(date)
    inputs: list[dict] = []

    def collapse(name: str, n: int) -> bool:
        med = meds.get(name)
        return bool(med and n < med * ROW_COLLAPSE_FRACTION)

    # --- Finviz export (liquidity gate + labels + AB proxy) ---
    dated = ROOT / "data" / "exports" / f"finviz_{date}.csv"
    if dated.exists():
        n = _rows_csv(dated)
        st = "degraded" if (n < 5000 or collapse("finviz_export", n)) else "ok"
        inputs.append(_mk("finviz_export", "join", st, n, dated.name))
    else:
        latest = sorted((ROOT / "data" / "exports").glob("finviz_????-??-??.csv"))
        if latest:
            inputs.append(_mk("finviz_export", "join", "stale", _rows_csv(latest[-1]),
                              f"dated missing — ranker falls back to {latest[-1].name}"))
        else:
            inputs.append(_mk("finviz_export", "join", "missing", 0, "no exports at all"))

    # --- membership labels ---
    memb = ROOT / "data" / "universe" / f"{date}_membership.csv"
    if memb.exists():
        n = _rows_csv(memb)
        st = "degraded" if (n < 5000 or collapse("membership", n)) else "ok"
        inputs.append(_mk("membership", "join", st, n))
    else:
        inputs.append(_mk("membership", "join", "missing", 0,
                          "ranker falls back to latest — labels may be stale"))

    # --- join ranked ---
    jp = ROOT / "data" / "join" / f"{date}_ranked.csv"
    if jp.exists():
        try:
            j = pd.read_csv(jp, usecols=lambda c: c in ("Ticker", "score_norm", "total_score"),
                            low_memory=False)
            n = len(j)
            score = pd.to_numeric(
                j.get("score_norm", j.get("total_score")), errors="coerce")
            sd = float(score.std()) if score is not None else 0.0
            if n < 1000 or collapse("join_ranked", n):
                st, det = "degraded", f"rows collapsed (n={n})"
            elif not sd or sd != sd:
                st, det = "degraded", "score variance is zero — rank is meaningless"
            else:
                st, det = "ok", f"std={sd:.3f}"
            inputs.append(_mk("join_ranked", "join", st, n, det))
        except Exception as e:  # noqa: BLE001 — any read failure is the finding
            inputs.append(_mk("join_ranked", "join", "degraded", 0, f"unreadable: {e}"))
    else:
        inputs.append(_mk("join_ranked", "join", "missing", 0, "stock_book aborts without it"))

    # --- news layers ---
    ap = ROOT / "01_daily" / "news" / f"{date}_actions.json"
    if ap.exists():
        try:
            data = json.loads(ap.read_text(encoding="utf-8"))
            ta = data.get("ticker_actions") or data.get("edge_actions") or data.get("actions") or {}
            n = len(ta)
            st = "degraded" if (n == 0 or collapse("news_actions", n)) else "ok"
            inputs.append(_mk("news_actions", "news", st, n))
        except (OSError, json.JSONDecodeError):
            inputs.append(_mk("news_actions", "news", "degraded", 0, "unparseable json"))
    else:
        inputs.append(_mk("news_actions", "news", "missing", 0))

    jd = ROOT / "01_daily" / "news"
    has_judge = (jd / f"{date}_judge.json").exists() or (jd / f"{date}_judge.md").exists()
    inputs.append(_mk("news_judge", "news", "ok" if has_judge else "missing",
                      None, "" if has_judge else "LLM ticker tilts absent"))
    has_digest = (jd / f"{date}_finviz_digest.json").exists()
    inputs.append(_mk("finviz_digest", "news", "ok" if has_digest else "missing", None))

    # --- AB checklist ---
    abe = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv"
    abr = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv"
    src = abe if abe.exists() else (abr if abr.exists() else None)
    if src is not None:
        try:
            a = pd.read_csv(src, low_memory=False)
            tcol = "Ticker" if "Ticker" in a.columns else a.columns[0]
            n = len(a)
            dups = int(a[tcol].astype(str).str.upper().duplicated().sum())
            score_col = next((c for c in ("score_enriched", "score", "checklist_score")
                              if c in a.columns), None)
            nz = int(pd.to_numeric(a[score_col], errors="coerce").fillna(0).ne(0).sum()) \
                if score_col else 0
            det = src.name + (f" · {dups} duplicate tickers" if dups else "")
            if score_col is None or nz == 0:
                st = "degraded"
                det += " · no usable score column" if score_col is None else " · all scores zero"
            elif collapse("ab_checklist", n) or (not abe.exists()):
                st = "degraded"
                det += "" if abe.exists() else " · enrichment missing (raw only)"
            else:
                st = "ok"
            inputs.append(_mk("ab_checklist", "ab", st, n, det))
        except Exception as e:  # noqa: BLE001
            inputs.append(_mk("ab_checklist", "ab", "degraded", 0, f"unreadable: {e}"))
    else:
        inputs.append(_mk("ab_checklist", "ab", "missing", 0,
                          "s_ab=0 for every name — its weight will be renormalized away"))

    # --- peer RS ---
    pp = ROOT / "data" / "peers" / f"{date}_peer_rs.csv"
    if pp.exists():
        n = _rows_csv(pp)
        st = "degraded" if (n == 0 or collapse("peer_rs", n)) else "ok"
        inputs.append(_mk("peer_rs", "peer", st, n))
    else:
        inputs.append(_mk("peer_rs", "peer", "missing", 0))

    # --- ticker checklist (rebound flags) — stale fallback is silent in ranker ---
    cp = ROOT / "data" / "checklist" / f"{date}_checklist.csv"
    if cp.exists():
        inputs.append(_mk("ticker_checklist", "addon", "ok", _rows_csv(cp)))
    else:
        older = sorted((ROOT / "data" / "checklist").glob("*_checklist.csv"))
        hist = ROOT / "data" / "checklist" / "checklist_history.parquet"
        if hist.exists() or older:
            inputs.append(_mk("ticker_checklist", "addon", "stale", None,
                              "no dated file — rebound flags use latest/history (can be stale)"))
        else:
            inputs.append(_mk("ticker_checklist", "addon", "missing", None))

    # --- events freshness ---
    ev = ROOT / "01_daily" / "events" / "latest.json"
    ev_status, ev_det = "missing", ""
    if ev.exists():
        try:
            data = json.loads(ev.read_text(encoding="utf-8"))
            sd = data.get("scan_date") or ""
            delta = abs((datetime.fromisoformat(sd) - datetime.fromisoformat(date)).days) if sd else 99
            n_ev = len(data.get("events") or [])
            if delta > 3:
                ev_status, ev_det = "stale", f"scan_date={sd} ({delta}d old — tilt ignored)"
            elif n_ev == 0:
                ev_status, ev_det = "degraded", "no events"
            else:
                ev_status, ev_det = "ok", f"{n_ev} events, scan_date={sd}"
        except (OSError, json.JSONDecodeError, ValueError):
            ev_status, ev_det = "degraded", "unparseable"
    inputs.append(_mk("events", "addon", ev_status, None, ev_det))

    # --- same-day LLM predicts (scoreboard) ---
    board = scoreboard.load()
    topics = {r.get("topic") for r in board.get("runs", [])
              if r.get("date") == date and r.get("predicted_direction")}
    has_general = "general" in topics
    n_sectors = len([t for t in topics if str(t).startswith("sector:")])
    inputs.append(_mk("general_predict", "general", "ok" if has_general else "missing", None,
                      "" if has_general else "s_general=0 — weight renormalized away"))
    st = "ok" if n_sectors >= 11 else ("degraded" if n_sectors else "missing")
    inputs.append(_mk("sector_predicts", "sector", st, n_sectors, f"{n_sectors}/11"))

    # --- price store staleness (learner only, not the ranker) ---
    meta_p = ROOT / "data" / "prices" / "meta.json"
    if meta_p.exists():
        try:
            last = json.loads(meta_p.read_text(encoding="utf-8")).get("last_date") or ""
            lag = (datetime.fromisoformat(date) - datetime.fromisoformat(last)).days if last else 99
            inputs.append(_mk("price_store", "learn", "ok" if lag <= 5 else "stale",
                              None, f"last_date={last}"))
        except (OSError, json.JSONDecodeError, ValueError):
            inputs.append(_mk("price_store", "learn", "degraded", None, "meta unreadable"))
    else:
        inputs.append(_mk("price_store", "learn", "missing", None))

    # ---- aggregate ----
    fam_status: dict[str, str] = {}
    for row in inputs:
        f = row["family"]
        if f in ("addon", "learn"):
            continue
        cur = fam_status.get(f, "ok")
        if _SEV[row["status"]] > _SEV[cur]:
            fam_status[f] = row["status"]
        else:
            fam_status.setdefault(f, cur)
    # a family is "present" for weight-renorm purposes unless hard-missing
    families_present = {f: st != "missing" for f, st in fam_status.items()}

    core_ok = all(
        fam_status.get(f, "missing") in ("ok", "degraded")
        for f in ("join", "ab", "peer")
    )
    health = {
        "date": date,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "inputs": inputs,
        "family_status": fam_status,
        "families_present": families_present,
        "learn_grade": core_ok and fam_status.get("join") == "ok",
        "worst": max((r["status"] for r in inputs), key=lambda s: _SEV[s]),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{date}_input_health.json"
    out.write_text(json.dumps(health, indent=2), encoding="utf-8")
    return health


def render(health: dict) -> str:
    L = [f"[input-health] {health['date']} — worst status: {health['worst']}"]
    for r in health["inputs"]:
        mark = {"ok": " ", "degraded": "!", "stale": "~", "missing": "X"}[r["status"]]
        n = "" if r.get("n") is None else f" n={r['n']}"
        det = f" — {r['detail']}" if r.get("detail") else ""
        L.append(f"  [{mark}] {r['name']:<18} {r['status']:<9} ({r['family']}){n}{det}")
    L.append(f"  learn_grade={health['learn_grade']} families={health['family_status']}")
    return "\n".join(L)


def load(date: str) -> dict | None:
    p = OUT_DIR / f"{date}_input_health.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    print(render(check(date)))


if __name__ == "__main__":
    main()
