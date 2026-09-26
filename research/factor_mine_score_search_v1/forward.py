"""Score the frozen formulas on both windows and both price versions.

Refuses to run until freeze/FREEZE.json is the committed file. Does not
sort the grid again.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v1.protocol import (  # noqa: E402
    BY_ID,
    FORWARD,
    FREEZE,
    REPORT,
    RETURNS,
    SESSIONS,
    TUNE,
    active_inputs,
    compound,
    day_counts,
    load_drop,
    load_inputs,
    mean,
    median,
    prereg_fingerprint,
    window_stats,
)
from research.factor_mine_score_search_v1.walk import (  # noqa: E402
    fee_fns,
    load_tape,
    prepare_days,
    run_formula,
    run_iwm,
    run_random4,
)

SUMMARY = RETURNS / "summary.json"
FREEZE_REL = "research/factor_mine_score_search_v1/freeze/FREEZE.json"


def _show(path: str) -> bytes:
    return subprocess.check_output(["git", "show", f"HEAD:{path}"], cwd=ROOT)


def _pct(value) -> str:
    if value is None:
        return "n/a"
    text = f"{float(value) * 100:.2f}%"
    return "0.00%" if text == "-0.00%" else text


def _num(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _agg(rows: list[dict]) -> dict:
    def col(name: str) -> list[float]:
        return [float(row[name]) for row in rows if row.get(name) is not None]

    compounds = col("compound")
    return {
        "closed_mean": mean(col("closed")),
        "compound_15_mean": mean(col("compound_15")),
        "compound_mean": mean(compounds),
        "days_entered_mean": mean(col("days_entered")),
        "down_mean": mean(col("down")),
        "entries_mean": mean(col("entries")),
        "ex_best_mean": mean(col("ex_best")),
        "flat_mean": mean(col("flat")),
        "n": len(rows),
        "positive_share": (sum(1 for value in compounds if value > 0) / len(compounds)) if compounds else None,
        "too_few": sum(1 for row in rows if row.get("too_few")),
        "up_mean": mean(col("up")),
        "win_rate_mean": mean(col("win_rate")),
    }


def _share(rows: list[dict], names: set[str]) -> dict:
    part = 0.0
    whole = 0.0
    for row in rows:
        pnl = row.get("pnl") or {}
        part += sum(float(value) for ticker, value in pnl.items() if ticker in names)
        whole += sum(float(value) for value in pnl.values())
    return {
        "dropped_pnl": part,
        "share_of_sum": (part / whole) if whole else None,
        "total_pnl": whole,
    }


def _iwm_window(path: dict, window: tuple[str, ...]) -> dict:
    check = [path["returns"][session] for session in window if session in path["returns"]]
    missing = [session for session in window if session in set(path["missing"])]
    up, down, flat = day_counts(check)
    return {
        "compound": compound(check) if check else None,
        "down": down,
        "flat": flat,
        "missing": missing,
        "n_marked": len(check),
        "up": up,
    }


def _score_version(days, ids, futu, flat) -> dict:
    out = {}
    for fid in ids:
        formula = BY_ID[fid]
        by_x = {}
        for top_n in (2, 4, 8):
            book = run_formula(days, formula, top_n, futu)
            side = run_formula(days, formula, top_n, flat)
            entry = {"tune": {}, "forward": {}}
            for key, window in (("tune", TUNE), ("forward", FORWARD)):
                stats = window_stats(book, list(SESSIONS), window)
                stats["compound_15"] = window_stats(side, list(SESSIONS), window)["compound"]
                entry[key] = stats
            by_x[str(top_n)] = entry
        out[fid] = by_x
    return out


def _render(payload: dict) -> str:
    lines = [
        "# factor_mine_score_search_v1",
        "",
        f"Preregistration fingerprint `{payload['fingerprint']}`.",
        "Every session is `designed_after`. The study was created 2026-09-26.",
        f"Luck N = {payload['n_grid']}. The freeze order was committed before this file.",
        "",
        f"Best: `{payload['best']}`.",
        "Top 10: " + ", ".join(f"`{fid}`" for fid in payload["top10"]) + ".",
        "Top 20: " + ", ".join(f"`{fid}`" for fid in payload["top20"]) + ".",
        "",
        "Clean bars are the #362 parquet. Yahoo keeps the 77 names that parquet omits.",
        "A red morning (S missing or S <= -3) does not buy. Exit is the session close.",
        "",
    ]
    labels = (("tune", "Through 2026-09-11"), ("forward", "2026-09-14 through 2026-09-25"))
    header = (
        "| set | version | X | compound | flat 15bp | up | down | flat | "
        "days entered | entries | closed | win | ex-best | positive | too few |"
    )
    rule = "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    for key, label in labels:
        lines += [f"## {label}", ""]
        for version in ("clean", "yahoo"):
            lines += [header, rule]
            block = payload["versions"][version][key]
            for name in ("best", "top10", "top20"):
                for top_n in ("2", "4", "8"):
                    row = block[name][top_n]
                    lines.append(
                        "| {name} | {version} | {x} | {compound} | {flat15} | {up} | {down} | {flat} | {entered} | {entries} | {closed} | {win} | {ex} | {pos} | {few} |".format(
                            name=name,
                            version=version,
                            x=top_n,
                            compound=_pct(row["compound_mean"]),
                            flat15=_pct(row["compound_15_mean"]),
                            up=_num(row["up_mean"]),
                            down=_num(row["down_mean"]),
                            flat=_num(row["flat_mean"]),
                            entered=_num(row["days_entered_mean"]),
                            entries=_num(row["entries_mean"]),
                            closed=_num(row["closed_mean"]),
                            win=_pct(row["win_rate_mean"]),
                            ex=_pct(row["ex_best_mean"]),
                            pos=_pct(row["positive_share"]),
                            few=row["too_few"],
                        )
                    )
            lines.append(
                f"| IWM | {version} |  | {_pct(block['iwm']['compound'])} |  | {block['iwm']['up']} | {block['iwm']['down']} | {block['iwm']['flat']} |  |  |  |  |  |  |  |"
            )
            rnd = block["random4"]
            lines.append(
                f"| RANDOM4 | {version} | 4 | {_pct(rnd['compound_mean'])} |  |  |  |  |  |  |  |  |  |  |  |"
            )
            lines.append("")
            lines.append(
                f"RANDOM4 on {version}, median compound {_pct(rnd['compound_median'])}, n={rnd['n']}."
            )
            if block["iwm"]["missing"]:
                lines.append(
                    f"IWM on {version} has no close on: {', '.join(block['iwm']['missing'])}. "
                    f"Marked sessions: {block['iwm']['n_marked']}."
                )
            gap = block["dropped_77"]
            lines.append(
                f"On {version}, the 77 names absent from the cleaned file are "
                f"{_pct(gap['share_of_sum'])} of summed ticker dollars "
                f"({_num(gap['dropped_pnl'], 2)} / {_num(gap['total_pnl'], 2)}), "
                f"across the frozen top 20 at X=2, 4, and 8."
            )
            lines.append("")
    lines += ["## Inputs in the frozen list", ""]
    lines.append("| input | top 10 | top 20 | top 20 with clean tune mean > 0 | top 20 with clean forward mean > 0 |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    for name, row in payload["recurrence"].items():
        lines.append(f"| `{name}` | {row['top10']} | {row['top20']} | {row['tune_positive']} | {row['forward_positive']} |")
    lines += [
        "",
        "The positive columns are a reading of the frozen top 20. They do not change the order.",
        "The mean is the equal-weight mean of the X=2, X=4, and X=8 Futubull compounds.",
        "",
        "| formula | tune mean clean | forward mean clean | forward mean yahoo | inputs |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for row in payload["formulas"]:
        lines.append(
            f"| `{row['id']}` | {_pct(row['tune_clean'])} | {_pct(row['forward_clean'])} | {_pct(row['forward_yahoo'])} | {row['inputs']} |"
        )
    lines += [
        "",
        "Flat 15bp is the `flat 15bp` column and `compound_15_mean` in `summary.json`. It is not the rank.",
        "A row with `too few` above 0 has at least that many formulas under 30 closed trades.",
        "",
    ]
    return "\n".join(lines)


def _strip(stats: dict) -> dict:
    return {key: stats[key] for key in stats if key != "pnl"}


def main() -> None:
    if prereg_fingerprint() != json.loads(_show(FREEZE_REL))["fingerprint"]:
        raise SystemExit("freeze fingerprint is not this prereg")
    if _show(FREEZE_REL) != FREEZE.read_bytes():
        raise SystemExit("FREEZE.json is not the committed freeze")
    freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    order = list(freeze["order"])
    if order[:20] != list(dict.fromkeys(order[:20])):
        raise SystemExit("freeze order")
    top20 = order[:20]
    top10 = order[:10]
    inputs = load_inputs()
    futu, flat = fee_fns()
    stored = {row["id"]: row for row in freeze["rank"]}
    versions = {}
    raw = {}
    for kind in ("clean", "yahoo"):
        print(f"prepare {kind}", flush=True)
        tape = load_tape(kind, through=None)
        days = prepare_days(inputs, tape, SESSIONS)
        print(f"score {kind}", flush=True)
        scored = _score_version(days, top20, futu, flat)
        raw[kind] = scored
        if kind == "clean":
            for fid in top20:
                compounds = [scored[fid][x]["tune"]["compound"] for x in ("2", "4", "8")]
                got = mean(compounds)
                if abs(got - float(stored[fid]["mean_compound"])) > 1e-8:
                    raise SystemExit(f"clean tune drift {fid} {got} {stored[fid]['mean_compound']}")
        print(f"benchmarks {kind}", flush=True)
        iwm = run_iwm(tape, SESSIONS, futu)
        random_books = run_random4(days, futu)
        banned = set(load_drop()["dropped"])
        version_out = {}
        for key, window in (("tune", TUNE), ("forward", FORWARD)):
            groups = {
                "best": [fid for fid in top20[:1]],
                "top10": top10,
                "top20": top20,
            }
            block = {}
            share_rows = []
            for name, ids in groups.items():
                block[name] = {}
                for top_n in ("2", "4", "8"):
                    stats_rows = []
                    for fid in ids:
                        stats = dict(scored[fid][top_n][key])
                        stats_rows.append(stats)
                        if name == "top20":
                            share_rows.append(stats)
                    block[name][top_n] = _agg(stats_rows)
            block["dropped_77"] = _share(share_rows, banned)
            block["iwm"] = _iwm_window(iwm, window)
            compounds = [window_stats(book, list(SESSIONS), window)["compound"] for book in random_books]
            block["random4"] = {
                "compound_mean": mean(compounds),
                "compound_median": median(compounds),
                "n": len(compounds),
            }
            version_out[key] = block
        versions[kind] = version_out
    keys = ("board_list", "cam", "yday", "rsi", "macd", "flow", "earn", "cap_yday", "cap_macd", "rsi_mode")
    recurrence = {name: {"forward_positive": 0, "top10": 0, "top20": 0, "tune_positive": 0} for name in keys}

    def _mean_x(kind: str, fid: str, window: str) -> float:
        return mean([raw[kind][fid][x][window]["compound"] for x in ("2", "4", "8")])

    formulas = []
    for fid in top20:
        flags = active_inputs(BY_ID[fid])
        tune_mean = _mean_x("clean", fid, "tune")
        forward_clean = _mean_x("clean", fid, "forward")
        forward_yahoo = _mean_x("yahoo", fid, "forward")
        formulas.append({
            "forward_clean": forward_clean,
            "forward_yahoo": forward_yahoo,
            "id": fid,
            "inputs": ", ".join(f"{name}={flags[name]}" for name in flags),
            "tune_clean": tune_mean,
        })
        for name in keys:
            if name not in flags:
                continue
            recurrence[name]["top20"] += 1
            if fid in top10:
                recurrence[name]["top10"] += 1
            if tune_mean > 0:
                recurrence[name]["tune_positive"] += 1
            if forward_clean > 0:
                recurrence[name]["forward_positive"] += 1
    payload = {
        "best": top20[0],
        "fingerprint": prereg_fingerprint(),
        "formulas": formulas,
        "n_grid": 34,
        "recurrence": recurrence,
        "top10": top10,
        "top20": top20,
        "versions": versions,
    }
    # Drop ticker pnl maps before writing.
    for kind in versions.values():
        for block in kind.values():
            for name in ("best", "top10", "top20"):
                for top_n, row in block[name].items():
                    block[name][top_n] = {k: v for k, v in row.items() if k != "pnl"}
    RETURNS.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT.write_text(_render(payload), encoding="utf-8")
    print(f"wrote {REPORT}", flush=True)


if __name__ == "__main__":
    main()
