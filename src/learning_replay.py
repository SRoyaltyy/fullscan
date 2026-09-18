"""Daily paired portfolio evaluation of frozen weight and lesson trials.

Only immutable pre-open frames AFTER registration enter a trial. Outcome bars
are loaded separately. Quote replay is unverified until an execution calibration
record is supplied; fixed-bps results remain shadow diagnostics indefinitely.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import pandas as pd

from . import book_learn as bl, factor_mine as fm, factor_mine_book as fmb
from . import lesson_exec as lx, learning_trials as trials
from .execution_clock import fixed_bps, timestamped_quotes
from .research_validation import clock_errors, digest, timestamp, write_once


def evaluate_trial(trial, snapshots, bars, fees, *, quotes=None, calibrated=False):
    """Five equal independent long books; frozen baseline vs candidate policies."""
    snapshots = [s for s in snapshots if not clock_errors(s) and
                 timestamp(s["decision_at"]) > timestamp(trial["frozen_at"])]
    if not snapshots:
        return []
    dates = [s["session"] for s in snapshots]
    paired = []
    calendar = sorted({d for _, d in bars if dates[0] <= d <= dates[-1]})
    coverage_ok = dates == calendar
    for spec in (trial["baseline"], trial["candidate"]):
        books = []
        for horizon, hold in bl.HORIZON_DAYS.items():
            rows, by_date = [], {}
            for snap in snapshots:
                df = pd.DataFrame(snap["rows"])
                if "override" in spec and spec.get("code_hash"):
                    # Only one changed lesson per trial; all other parameters fixed.
                    df, _ = lx.apply_to_frame(df, snap["session"], shadow=True,
                                             override_ids={spec["override"]}, context=snap.get("lesson_context", {}))
                weights = tuple((spec.get("weights") or {}).get(horizon, bl.WEIGHTS[horizon]))
                comp = bl._components_for(df, horizon)
                score, _ = bl._score(df, comp, weights, set(), heat_scale=spec.get("heat_scale", 1.))
                picks = bl._select_buys(df, score, 10)
                selected = []
                for rank, idx in enumerate(picks):
                    ticker = str(df.iloc[idx]["Ticker"]).upper()
                    if (ticker, snap["session"]) not in bars:
                        coverage_ok = False
                    selected.append({"ticker": ticker, "date": snap["session"],
                                     "sources": ["union"], "src_rank": rank,
                                     "ohlc_ret_1": 0, "rsi": None, "e_pol": "neutral"})
                by_date[snap["session"]] = selected
                rows.extend(selected)
            panel = {"rows": rows, "by_date": by_date, "session_dates": dates,
                     "_tape_filled": True, "_ohlc_filled": True}
            rec = fm.make_recipe("shadow_"+horizon, universe="union", hold=hold,
                                 top_n=10, sell="list", rank="src_rank")
            fill = timestamped_quotes(quotes) if quotes is not None else fixed_bps(10)
            b = fmb.simulate_book(panel, rec, bars=bars, fees=fees,
                                  regime={d: {"predict_score": 0} for d in dates},
                                  rules={"capital": 2000}, exec_fill=fill)
            coverage_ok = coverage_ok and b["audit"]["ok"]
            coverage_ok = coverage_ok and not any(s["kind"] in ("no_price", "fill_miss")
                                                  for s in b["skips"])
            books.append(b)
        eq = [sum(b["equity"][i] for b in books) for i in range(len(dates)+1)]
        paired.append([eq[i+1]/eq[i]-1 if eq[i] > 0 else -1 for i in range(len(dates))])
    output = []
    for i, snap in enumerate(snapshots):
        output.append({k: snap[k] for k in ("session", "decision_at", "available_at", "observed_at", "input_hash")})
        output[-1].update(candidate_hash=trial["candidate_hash"],
                         outcome_at=snap["session"]+"T16:02:00"+trials._offset(snap["session"]),
                         baseline_net=paired[0][i], challenger_net=paired[1][i],
                         execution_verified=bool(quotes is not None and calibrated and coverage_ok),
                         evidence_kind="paired shadow portfolio; five equal horizon sleeves",
                         execution_kind="timestamped_quote" if quotes is not None else "10bps sensitivity")
    return output


def run(asof=None):
    root = trials.TRIALS
    now = datetime.now(timezone.utc)
    asof = asof or now.date().isoformat()
    snaps = []
    for path in sorted((root / "inputs").glob("*.json")):
        s = json.loads(path.read_text())
        if s["session"] <= asof and fm.session_has_closed(s["session"]):
            snaps.append(s)
    if not snaps:
        return {"status": "waiting for prospective pre-open snapshots", "observations": 0}
    raw = pd.read_parquet(bl.PRICES)
    raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")
    raw = raw[raw["date"] <= asof]
    bars = {(str(r["ticker"]).upper(), r["date"]): r for r in raw.to_dict("records")}
    quote_path, calibration_path = root / "quotes.json", root / "execution_calibration.json"
    quotes = json.loads(quote_path.read_text()) if quote_path.exists() else None
    calibration = json.loads(calibration_path.read_text()) if calibration_path.exists() else {}
    # Binding prevents reusing validation for a changed feed/policy.
    calibrated = (calibration.get("validated") is True and
                  calibration.get("policy") == "post_open_5s_60s_displayed_size" and
                  bool(calibration.get("broker_comparison_hash")) and
                  calibration.get("engine_hash") == trials.engine_hash() and
                  calibration.get("ranker_parity_validated") is True and
                  bool(calibration.get("ranker_parity_report_hash")))
    fees = json.loads((trials.ROOT / "00_grounding/futubull_fees.json").read_text())
    count = 0
    for path in sorted(root.glob("**/proposals/*.json")):
        trial = json.loads(path.read_text())
        if trial.get("engine_hash") != trials.engine_hash():
            continue
        observations = evaluate_trial(trial, snaps, bars, fees, quotes=quotes, calibrated=calibrated)
        trial_root = path.parent.parent
        for row in observations:
            target = trial_root / "observations" / trial["candidate_hash"] / (row["session"]+".json")
            # First mature grading is immutable; corrected feeds require a new trial.
            if not target.exists():
                write_once(target, row)
                count += 1
    return {"status": "shadow evaluation complete", "observations": count}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    print(json.dumps(run(ap.parse_args().date)))
