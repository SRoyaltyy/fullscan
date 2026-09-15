"""Crash-resume wrapper around excel_stat_mine.

Keeps the original miner module intact. Replaces main() with a streamed
walk that flushes 03_scoreboard/excel_stat_mine_ckpt.json + PARTIAL board
every N tickers / T seconds / SIGTERM. Next run skips processed tickers.
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import signal
import subprocess
import time
from collections import defaultdict

import excel_stat_mine as m

FLUSH_EVERY = 25
FLUSH_SECONDS = 480
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LIVE = {
    "ckpt": "03_scoreboard/excel_stat_mine_ckpt.json",
    "board": "03_scoreboard/EXCEL_STAT_MINE.md",
    "json": "03_scoreboard/excel_stat_mine.json",
    "push": True,
    "state": None,
    "last": 0.0,
    "every": FLUSH_EVERY,
    "secs": FLUSH_SECONDS,
}


def _atomic_json(path, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return path


def load_ckpt(path):
    if not path or not os.path.isfile(path):
        return None
    try:
        raw = json.load(open(path, encoding="utf-8"))
    except Exception as exc:
        print(f"[ckpt] unreadable {path}: {exc}", flush=True)
        return None
    if not isinstance(raw, dict):
        return None
    if raw.get("split_kind") != m.SPLIT_KIND or not raw.get("cutoff"):
        print(
            f"[ckpt] reject stale split_kind={raw.get('split_kind')!r} "
            f"cutoff={raw.get('cutoff')!r} — remine time-split",
            flush=True,
        )
        return None
    print(
        f"[ckpt] resume phase={raw.get('phase')} "
        f"cutoff={raw.get('cutoff')} "
        f"tickers={len(raw.get('processed') or [])} "
        f"sessions={raw.get('n')} rules={len(raw.get('counts') or {})}",
        flush=True,
    )
    return raw


def _ranked_from_counts(counts, n, lab_n, min_n):
    if n <= 0:
        return []
    base = lab_n / n
    out = []
    for rule, pair in counts.items():
        hit, supp = int(pair[0]), int(pair[1])
        if supp < min_n:
            continue
        a, b = hit, supp - hit
        c = lab_n - hit
        d = n - supp - c
        chi, p = m._chi2_p(a, b, c, d)
        conf = hit / supp if supp else 0
        out.append({
            "rule": rule, "n": supp, "label_n": hit, "base": base,
            "conf": conf, "lift": (conf / base) if base else None,
            "chi2": chi, "p": p, "mi": m._mi(a, b, c, d),
        })
    out.sort(key=lambda r: (r["p"], -(r.get("lift") or 0)))
    return out


def _land(msg, *paths):
    script = os.path.join(REPO, "scripts", "safe_git_push.sh")
    if not LIVE.get("push") or not os.path.isfile(script):
        return
    if not os.environ.get("GITHUB_TOKEN") and not os.environ.get("GH_TOKEN"):
        return
    exist = [p for p in paths if p and os.path.isfile(p)]
    if not exist:
        return
    print(f"[ckpt] landing {exist}", flush=True)
    subprocess.run(["bash", script, msg, *exist], check=False)


def _dump(state):
    payload = {
        "phase": state.get("phase"),
        "done": bool(state.get("done")),
        "partial": not bool(state.get("done")),
        "processed": sorted(state.get("processed") or []),
        "n": state.get("n", 0),
        "lab_n": state.get("lab_n", 0),
        "n_hold": state.get("n_hold", 0),
        "hold_lab_n": state.get("hold_lab_n", 0),
        "hold_base": state.get("hold_base", 0),
        "counts": state.get("counts") or {},
        "hold_counts": state.get("hold_counts") or {},
        "pair_counts": state.get("pair_counts") or {},
        "pair_hold_counts": state.get("pair_hold_counts") or {},
        "uni_fdr": state.get("uni_fdr") or [],
        "pairs_fdr": state.get("pairs_fdr") or [],
        "confirmed": state.get("confirmed") or [],
        "pairs": state.get("pairs") or [],
        "pair_n": state.get("pair_n", 0),
        "pair_lab_n": state.get("pair_lab_n", 0),
        "pair_hold_n": state.get("pair_hold_n", 0),
        "pair_hold_lab_n": state.get("pair_hold_lab_n", 0),
        "gate": m.gate_payload(),
        "split_kind": m.SPLIT_KIND,
        "cutoff": state.get("cutoff"),
        "hold_frac": m.HOLD_FRAC,
        "flushed_unix": int(time.time()),
    }
    return _atomic_json(LIVE["ckpt"], payload)


def _board(state):
    n = int(state.get("n") or 0)
    lab_n = int(state.get("lab_n") or 0)
    base = (lab_n / n) if n else 0.0
    rows = _ranked_from_counts(
        state.get("counts") or {}, n, lab_n, max(80, int(0.01 * max(n, 1))))
    status = "DONE" if state.get("done") else "PARTIAL"
    shown = state.get("confirmed") or rows[:60]
    pairs = state.get("pairs") or []
    meta = (
        f"status={status} phase={state.get('phase')} "
        f"TIME-SPLIT cutoff={state.get('cutoff')} "
        f"tickers_done={len(state.get('processed') or [])} "
        f"sessions={n} letters={len(m.ALL_LETTERS)} "
        f"univ=mcap>${m.MCAP_MIN_M:.0f}M & vol>{m.AVGVOL_MIN:.0f}"
    )
    m.write_board(LIVE["board"], meta, shown, pairs, base,
                  float(state.get("hold_base") or 0))
    _atomic_json(LIVE["json"], {
        "status": status, "phase": state.get("phase"),
        "n_tickers_done": len(state.get("processed") or []),
        "n_disc_rows": n, "n_hold_rows": state.get("n_hold", 0),
        "n_rules": len(state.get("counts") or {}),
        "top_singles": shown[:30], "top_pairs": pairs[:20],
        "gate": m.gate_payload(), "partial": status != "DONE",
        "split_kind": m.SPLIT_KIND, "cutoff": state.get("cutoff"),
    })


def flush_progress(state, reason="tick"):
    LIVE["state"] = state
    LIVE["last"] = time.time()
    _dump(state)
    _board(state)
    print(
        f"[ckpt] {reason} phase={state.get('phase')} "
        f"tickers={len(state.get('processed') or [])} "
        f"sessions={state.get('n', 0)} rules={len(state.get('counts') or {})}",
        flush=True,
    )
    _land(
        f"chore: excel stat mine ckpt ({state.get('phase')} {reason})",
        LIVE["ckpt"], LIVE["board"], LIVE["json"],
    )


def _sessions(ticker_days, cutoff, quant, which, skip, label_key="I1_green"):
    skip = set(skip or [])
    hzn = m.label_horizon(label_key)
    for tkr in sorted(ticker_days):
        if tkr in skip:
            continue
        try:
            days = m.normalize_days(ticker_days[tkr])
        except Exception as exc:
            print(f"[skip] {tkr} normalize: {exc}", flush=True)
            continue
        if len(days) < 30:
            continue
        batch = []
        try:
            for t in range(20, len(days)):
                if not days[t]["o"]:
                    continue
                feat = days[t].get("date") or ""
                end = m.label_end_date(days, t, hzn)
                if which == "disc":
                    if not m.disc_label_ok(feat, end, cutoff):
                        continue
                elif not feat or feat < cutoff:
                    continue
                labs = m._labels(days, t)
                if label_key not in labs:
                    continue
                batch.append((m.features_at(days, t, quant), labs))
        except Exception as exc:
            print(f"[skip] {tkr} features: {exc}", flush=True)
            continue
        yield tkr, batch


def _maybe(state, since):
    due = since >= int(LIVE["every"]) or (
        time.time() - float(LIVE["last"] or 0)) >= float(LIVE["secs"])
    if due:
        flush_progress(state, reason=f"+{since}")
        return 0
    return since


def walk_uni(state, ticker_days, cutoff, quant, which, label, key):
    counts = defaultdict(lambda: [0, 0], {k: list(v) for k, v in (state.get(key) or {}).items()})
    processed = set(state.get("processed") or [])
    n_key = "n" if which == "disc" else "n_hold"
    lab_key = "lab_n" if which == "disc" else "hold_lab_n"
    n = int(state.get(n_key) or 0)
    lab_n = int(state.get(lab_key) or 0)
    since = 0
    for tkr, batch in _sessions(ticker_days, cutoff, quant, which, processed, label):
        for feats, lab in batch:
            n += 1
            y = 1 if lab.get(label) else 0
            lab_n += y
            for k in feats:
                slot = counts[k]
                slot[1] += 1
                slot[0] += y
        processed.add(tkr)
        since += 1
        state[key] = dict(counts)
        state["processed"] = sorted(processed)
        state[n_key] = n
        state[lab_key] = lab_n
        if which != "disc" and n:
            state["hold_base"] = lab_n / n
        since = _maybe(state, since)
    if since:
        flush_progress(state, reason="phase-end")
    return counts, n, lab_n


def walk_confirm(state, ticker_days, cutoff, quant, which, rules, label, key):
    want = list(rules)
    counts = defaultdict(lambda: [0, 0], {k: list(v) for k, v in (state.get(key) or {}).items()})
    processed = set(state.get("processed") or [])
    n_key = "n" if which == "disc" else "n_hold"
    lab_key = "lab_n" if which == "disc" else "hold_lab_n"
    n = int(state.get(n_key) or 0)
    lab_n = int(state.get(lab_key) or 0)
    since = 0
    for tkr, batch in _sessions(ticker_days, cutoff, quant, which, processed, label):
        for feats, lab in batch:
            n += 1
            y = 1 if lab.get(label) else 0
            lab_n += y
            for rule in want:
                if "|" in rule:
                    a, b = rule.split("|", 1)
                    ok = a in feats and b in feats
                else:
                    ok = rule in feats
                if not ok:
                    continue
                slot = counts[rule]
                slot[1] += 1
                slot[0] += y
        processed.add(tkr)
        since += 1
        state[key] = dict(counts)
        state["processed"] = sorted(processed)
        state[n_key] = n
        state[lab_key] = lab_n
        if n:
            state["hold_base"] = lab_n / n
        since = _maybe(state, since)
    if since:
        flush_progress(state, reason="phase-end")
    return counts, n, lab_n


def walk_pairs(state, ticker_days, cutoff, quant, which, seeds, label, key):
    seed = list(seeds)
    counts = defaultdict(lambda: [0, 0], {k: list(v) for k, v in (state.get(key) or {}).items()})
    processed = set(state.get("processed") or [])
    if which == "disc":
        n = int(state.get("pair_n") or 0)
        lab_n = int(state.get("pair_lab_n") or 0)
    else:
        n = int(state.get("pair_hold_n") or 0)
        lab_n = int(state.get("pair_hold_lab_n") or 0)
    since = 0
    for tkr, batch in _sessions(ticker_days, cutoff, quant, which, processed, label):
        for feats, lab in batch:
            n += 1
            y = 1 if lab.get(label) else 0
            lab_n += y
            on = [s for s in seed if s in feats]
            if len(on) < 2:
                continue
            for i, a in enumerate(on):
                for b in on[i + 1:]:
                    slot = counts[f"{a}|{b}"]
                    slot[1] += 1
                    slot[0] += y
        processed.add(tkr)
        since += 1
        state[key] = dict(counts)
        state["processed"] = sorted(processed)
        if which == "disc":
            state["pair_n"] = n
            state["pair_lab_n"] = lab_n
        else:
            state["pair_hold_n"] = n
            state["pair_hold_lab_n"] = lab_n
            state["n_hold"] = n
            state["hold_lab_n"] = lab_n
            state["hold_base"] = (lab_n / n) if n else 0
        since = _maybe(state, since)
    if since:
        flush_progress(state, reason="phase-end")
    return counts, n, lab_n


def _on_signal(signum, _frame):
    st = LIVE.get("state")
    if st is not None:
        try:
            flush_progress(st, reason=f"signal-{signum}")
        except Exception as exc:
            print(f"[ckpt] signal flush failed: {exc}", flush=True)
    raise SystemExit(128 + int(signum))


def _on_exit():
    st = LIVE.get("state")
    if st is None or st.get("_exited"):
        return
    st["_exited"] = True
    try:
        flush_progress(st, reason="atexit")
    except Exception as exc:
        print(f"[ckpt] atexit flush failed: {exc}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="")
    ap.add_argument("--finviz", default="")
    ap.add_argument("--split", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--label", default="I1_green")
    ap.add_argument("--out-md", default="03_scoreboard/EXCEL_STAT_MINE.md")
    ap.add_argument("--out-json", default="03_scoreboard/excel_stat_mine.json")
    ap.add_argument("--ckpt", default="03_scoreboard/excel_stat_mine_ckpt.json")
    ap.add_argument("--flush-every", type=int, default=FLUSH_EVERY)
    ap.add_argument("--flush-seconds", type=int, default=FLUSH_SECONDS)
    ap.add_argument("--no-push", action="store_true")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    LIVE["ckpt"] = args.ckpt
    LIVE["board"] = args.out_md
    LIVE["json"] = args.out_json
    LIVE["push"] = not args.no_push
    LIVE["every"] = args.flush_every
    LIVE["secs"] = args.flush_seconds
    LIVE["last"] = time.time()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    atexit.register(_on_exit)

    m.assert_excel_clock_gate()
    print(f"[letters] {len(m.ALL_LETTERS)} columns", flush=True)
    finviz_path = args.finviz or next(
        (p for p in ("excel_bot/data/finviz_with_descriptions.csv",
                     "data/finviz_with_descriptions.csv") if os.path.isfile(p)), "")
    allow = m.load_finviz_filter(finviz_path) if finviz_path else {}
    print(f"[univ] {len(allow)} names mcap>${m.MCAP_MIN_M:.0f}M vol>{m.AVGVOL_MIN:.0f}", flush=True)
    files = m.find_grids(args.grids)
    if args.limit:
        files = files[: args.limit]
    ticker_days = m.load_grids(files, allow if allow else None)
    print(f"[grids] {len(ticker_days)} tickers", flush=True)
    state = None if args.reset else load_ckpt(args.ckpt)
    cutoff = m.time_split_cutoff(ticker_days, locked=(state or {}).get("cutoff"))
    print(f"[split] kind=time cutoff={cutoff} hold_frac={m.HOLD_FRAC} "
          f"(ticker split ignored)", flush=True)
    if not cutoff:
        raise SystemExit("no session dates — cannot time-split")
    quant = m.discovery_quantiles(ticker_days, cutoff)
    print(f"[quant] {len(quant)} letters with discovery numeric bins", flush=True)

    if state is None:
        state = {
            "phase": "disc_uni", "processed": [], "counts": {},
            "n": 0, "lab_n": 0, "split_kind": m.SPLIT_KIND, "cutoff": cutoff,
        }
    else:
        state["split_kind"] = m.SPLIT_KIND
        state["cutoff"] = cutoff
    if state.get("done"):
        print("[ckpt] already done — rewriting board", flush=True)
        LIVE["state"] = state
        flush_progress(state, reason="already-done")
        return
    LIVE["state"] = state
    flush_progress(state, reason="start")

    if state.get("phase") in (None, "disc_uni"):
        state["phase"] = "disc_uni"
        print("[walk] discovery univariate", flush=True)
        counts, n_d, lab_d = walk_uni(
            state, ticker_days, cutoff, quant, "disc", args.label, "counts")
        uni = _ranked_from_counts(
            counts, n_d, lab_d, max(m.MIN_DISC_N, m.MIN_SUPPORT * max(n_d, 1)))
        keep = m.bh_keep(uni, m.FDR_Q)
        uni_fdr = [r for r in uni if r["rule"] in keep and (r.get("lift") or 0) > 1]
        state["uni_fdr"] = uni_fdr
        state["phase"] = "hold_uni"
        state["processed"] = []
        state["hold_counts"] = {}
        state["n_hold"] = 0
        state["hold_lab_n"] = 0
        flush_progress(state, reason="disc-uni-done")
    else:
        uni_fdr = state.get("uni_fdr") or []

    confirmed = state.get("confirmed") or []
    if state.get("phase") == "hold_uni":
        print("[walk] holdout confirm singles", flush=True)
        hold_counts, n_h, lab_h = walk_confirm(
            state, ticker_days, cutoff, quant, "hold",
            [r["rule"] for r in uni_fdr], args.label, "hold_counts")
        base_h = (lab_h / n_h) if n_h else 0
        confirmed = []
        for r in uni_fdr:
            pair = hold_counts.get(r["rule"]) or [0, 0]
            hit, supp = int(pair[0]), int(pair[1])
            if supp < m.MIN_HOLD_N:
                continue
            conf = hit / supp if supp else 0
            lift = (conf / base_h) if base_h else 0
            if lift <= 1:
                continue
            confirmed.append(dict(r, hold_n=supp, hold_conf=conf,
                                  hold_lift=lift, hold_base=base_h))
        confirmed.sort(key=lambda r: (r["p"], -(r.get("hold_lift") or 0)))
        state["confirmed"] = confirmed
        state["n_hold"] = n_h
        state["hold_lab_n"] = lab_h
        state["hold_base"] = base_h
        state["phase"] = "pairs_disc"
        state["processed"] = []
        state["pair_counts"] = {}
        state["pair_n"] = 0
        state["pair_lab_n"] = 0
        flush_progress(state, reason="hold-uni-done")

    pairs_ok = state.get("pairs") or []
    if state.get("phase") == "pairs_disc":
        seeds = [r["rule"] for r in confirmed[:m.PAIR_MAX_SEEDS]]
        print(f"[walk] pairs discovery seeds={len(seeds)}", flush=True)
        pair_counts, n_p, lab_p = walk_pairs(
            state, ticker_days, cutoff, quant, "disc", seeds, args.label, "pair_counts")
        pairs_d = _ranked_from_counts(
            pair_counts, n_p, lab_p, max(m.MIN_HOLD_N, m.MIN_SUPPORT * max(n_p, 1) / 2))
        pair_keep = m.bh_keep(pairs_d, m.FDR_Q)
        pairs_fdr = [r for r in pairs_d if r["rule"] in pair_keep and (r.get("lift") or 0) > 1]
        state["pairs_fdr"] = pairs_fdr
        state["phase"] = "pairs_hold"
        state["processed"] = []
        state["pair_hold_counts"] = {}
        state["pair_hold_n"] = 0
        state["pair_hold_lab_n"] = 0
        flush_progress(state, reason="pairs-disc-done")
    else:
        pairs_fdr = state.get("pairs_fdr") or []

    if state.get("phase") == "pairs_hold":
        print("[walk] pairs holdout", flush=True)
        ph_counts, n_h, lab_h = walk_pairs(
            state, ticker_days, cutoff, quant, "hold",
            [r["rule"] for r in pairs_fdr], args.label, "pair_hold_counts")
        base_h = (lab_h / n_h) if n_h else float(state.get("hold_base") or 0)
        pairs_ok = []
        for r in pairs_fdr:
            pair = ph_counts.get(r["rule"]) or [0, 0]
            hit, supp = int(pair[0]), int(pair[1])
            if supp < m.MIN_HOLD_N // 2:
                continue
            conf = hit / supp if supp else 0
            lift = (conf / base_h) if base_h else 0
            if lift <= 1:
                continue
            pairs_ok.append(dict(r, hold_n=supp, hold_conf=conf,
                                 hold_lift=lift, hold_base=base_h))
        pairs_ok.sort(key=lambda r: (r["p"], -(r.get("hold_lift") or 0)))
        state["pairs"] = pairs_ok
        state["hold_base"] = base_h
        state["phase"] = "done"
        state["done"] = True
        flush_progress(state, reason="done")

    print(f"[hold] {len(confirmed)} singles  [pairs] {len(pairs_ok)}", flush=True)
    print(f"[out] {args.out_md}", flush=True)


def install():
    m.main = main
    m._atomic_json = _atomic_json
    m.load_ckpt = load_ckpt
    m._ranked_from_counts = _ranked_from_counts


if __name__ == "__main__":
    install()
    main()
