"""Score the frozen formulas from 2026-09-14 through 2026-09-25.

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

from research.factor_mine_score_search_v2.protocol import (  # noqa: E402
    BY_ID,
    FORWARD,
    FREEZE,
    LUCK_N,
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
from research.factor_mine_score_search_v2.walk import (  # noqa: E402
    board_tickers,
    fee_fns,
    jump_flags,
    load_tape,
    prepare_days,
    run_formula,
    run_iwm,
    run_random4,
)

SUMMARY = RETURNS / "summary.json"
FREEZE_REL = "research/factor_mine_score_search_v2/freeze/FREEZE.json"


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
        "days_traded_mean": mean(col("days_traded")),
        "down_mean": mean(col("down")),
        "entries_mean": mean(col("entries")),
        "ex_best_mean": mean(col("ex_best")),
        "flat_mean": mean(col("flat")),
        "n": len(rows),
        "positive_share": (sum(1 for value in compounds if value > 0) / len(compounds)) if compounds else None,
        "too_few": sum(1 for row in rows if row.get("too_few")),
        "up_mean": mean(col("up")),
        "up_share_mean": mean(col("up_share")),
        "win_rate_mean": mean(col("win_rate")),
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
            entry = {}
            for key, window in (("tune", TUNE), ("forward", FORWARD)):
                stats = window_stats(book, list(SESSIONS), window)
                stats["compound_15"] = window_stats(side, list(SESSIONS), window)["compound"]
                entry[key] = stats
            by_x[str(top_n)] = entry
        out[fid] = by_x
    return out


def _verify_freeze(freeze: dict, days, futu) -> None:
    """The fresh-start cells that set the rank still match the committed freeze."""
    from research.factor_mine_score_search_v2.protocol import RANK_STARTS

    index = {day["session"]: pos for pos, day in enumerate(days)}
    required = {(row[0], int(row[1])) for row in freeze["required"]}
    for fid in freeze["order"]:
        formula = BY_ID[fid]
        for start, top_n in sorted(required):
            if start not in RANK_STARTS and start != "2026-08-20":
                continue
            sub = days[index[start]:]
            book = run_formula(sub, formula, top_n, futu)
            sessions = [day["session"] for day in sub]
            stats = window_stats(book, sessions, tuple(sessions))
            stored = freeze["cells"][fid][f"{start}|{top_n}"]
            for key in ("closed", "win_rate", "up_share"):
                got = stats[key]
                old = stored[key]
                if got is None and old is None:
                    continue
                if got is None or old is None or abs(float(got) - float(old)) > 1e-8:
                    raise SystemExit(f"freeze drift {fid} {start} X={top_n} {key} {got} {old}")


def _render(payload: dict) -> str:
    lines = [
        "# factor_mine_score_search_v2",
        "",
        "Diagnosis of a frozen ranking. The order was fixed through 2026-09-11. This file does not sort again.",
        f"Preregistration fingerprint `{payload['fingerprint']}`.",
        "Every session is `designed_after`. The study was created 2026-09-26.",
        f"Luck N = {payload['n_grid']}. That is v1's 34 formulas plus these 34. They are not added to 9,280.",
        "",
        f"Passers (every required cell has at least 30 closed trades): {payload['n_passers']}.",
        f"Best: `{payload['best']}`.",
        "Top 10: " + ", ".join(f"`{fid}`" for fid in payload["top10"]) + ".",
        "Top 20: " + ", ".join(f"`{fid}`" for fid in payload["top20"]) + ".",
        "",
        "The rank key is the worse of trade win rate and winning-day share, then the worst of those values across the required fresh starts. A flagged formula stays in the top 10 or top 20 when fewer than that many formulas clear every required cell.",
        "",
        "| rank | formula | passer | rank key | worst start | worst X |",
        "| ---: | --- | --- | ---: | --- | ---: |",
    ]
    for pos, row in enumerate(payload["rank_head"], start=1):
        worst = row.get("worst") or {}
        lines.append(
            f"| {pos} | `{row['id']}` | {'yes' if row['eligible'] else 'no'} | {_pct(row.get('rank_key'))} | {worst.get('start', 'n/a')} | {worst.get('x', 'n/a')} |"
        )
    lines += [
        "",
        "The tables below are the continuous $10,000 book from 2026-08-13. It does not reset on 2026-09-14. W/T is up days divided by days with a buy or a sell. Flat 15bp uses the same share counts.",
        "A `too few` count above 0 is how many formulas in that set have fewer than 30 closed trades. Those formulas stay.",
        "",
    ]
    labels = (("tune", "Continuous book through 2026-09-11"), ("forward", "2026-09-14 through 2026-09-25"))
    header = (
        "| set | version | X | compound | flat 15bp | win | W/T | up | down | flat | "
        "days entered | entries | closed | ex-best | positive | too few |"
    )
    rule = "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    for key, label in labels:
        lines += [f"## {label}", ""]
        for version in ("clean", "yahoo"):
            block = payload["versions"][version]
            if block.get("halted"):
                sample = ", ".join(f"{row['ticker']} {row['date']} {row['leg']}" for row in block["jumps"])
                lines.append(
                    f"{version} halted. {block['n']} unexplained jumps on kept names. No compound is written. First jumps: {sample}."
                )
                lines.append("")
                continue
            lines += [header, rule]
            view = block[key]
            for name in ("best", "top10", "top20"):
                for top_n in ("2", "4", "8"):
                    row = view[name][top_n]
                    lines.append(
                        "| {name} | {version} | {x} | {compound} | {flat15} | {win} | {wt} | {up} | {down} | {flat} | {entered} | {entries} | {closed} | {ex} | {pos} | {few} |".format(
                            name=name,
                            version=version,
                            x=top_n,
                            compound=_pct(row["compound_mean"]),
                            flat15=_pct(row["compound_15_mean"]),
                            win=_pct(row["win_rate_mean"]),
                            wt=_pct(row["up_share_mean"]),
                            up=_num(row["up_mean"]),
                            down=_num(row["down_mean"]),
                            flat=_num(row["flat_mean"]),
                            entered=_num(row["days_entered_mean"]),
                            entries=_num(row["entries_mean"]),
                            closed=_num(row["closed_mean"]),
                            ex=_pct(row["ex_best_mean"]),
                            pos=_pct(row["positive_share"]),
                            few=row["too_few"],
                        )
                    )
            lines.append(
                f"| IWM | {version} |  | {_pct(view['iwm']['compound'])} |  |  |  | {view['iwm']['up']} | {view['iwm']['down']} | {view['iwm']['flat']} |  |  |  |  |  |  |"
            )
            rnd = view["random4"]
            lines.append(
                f"| RANDOM4 | {version} | 4 | {_pct(rnd['compound_mean'])} |  |  |  |  |  |  |  |  |  |  |  |  |"
            )
            lines.append("")
            lines.append(f"RANDOM4 on {version}, median compound {_pct(rnd['compound_median'])}, n={rnd['n']}.")
            if view["iwm"]["missing"]:
                lines.append(
                    f"IWM on {version} has no close on: {', '.join(view['iwm']['missing'])}. Marked sessions: {view['iwm']['n_marked']}."
                )
            gap = view["dropped_77"]
            lines.append(
                f"On {version}, the 77 dropped names are {_pct(gap['share_of_sum'])} of summed ticker dollars "
                f"({_num(gap['dropped_pnl'], 2)} / {_num(gap['total_pnl'], 2)}), across the frozen top 20 at X=2, 4, and 8."
            )
            lines.append("")
    lines.append("No formula is added after this freeze. v1's preregistration and pick rule are not edited.")
    lines.append("")
    return "\n".join(lines) + "\n"


def _version_block(scored, top20, top10, iwm, random_books, window_name, window, banned) -> dict:
    groups = {"best": top20[:1], "top10": top10, "top20": top20}
    block = {}
    share_rows = []
    for name, ids in groups.items():
        block[name] = {}
        for top_n in ("2", "4", "8"):
            stats_rows = []
            for fid in ids:
                stats = dict(scored[fid][top_n][window_name])
                stats_rows.append(stats)
                if name == "top20":
                    share_rows.append(stats)
            block[name][top_n] = _agg(stats_rows)
    part = 0.0
    whole = 0.0
    for row in share_rows:
        pnl = row.get("pnl") or {}
        part += sum(float(value) for ticker, value in pnl.items() if ticker in banned)
        whole += sum(float(value) for value in pnl.values())
    compounds = [window_stats(book, list(SESSIONS), window)["compound"] for book in random_books]
    return {
        "best": block["best"],
        "dropped_77": {
            "dropped_pnl": part,
            "share_of_sum": (part / whole) if whole else None,
            "total_pnl": whole,
        },
        "iwm": _iwm_window(iwm, window),
        "random4": {
            "compound_mean": mean(compounds),
            "compound_median": median(compounds),
            "n": len(compounds),
        },
        "top10": block["top10"],
        "top20": block["top20"],
    }


def main() -> None:
    if prereg_fingerprint() != json.loads(_show(FREEZE_REL))["fingerprint"]:
        raise SystemExit("freeze fingerprint is not this prereg")
    if _show(FREEZE_REL) != FREEZE.read_bytes():
        raise SystemExit("FREEZE.json is not the committed freeze")
    freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    order = list(freeze["order"])
    top20 = list(freeze["top20"])
    top10 = list(freeze["top10"])
    if top20 != order[:20] or top10 != order[:10] or freeze["best"] != order[0]:
        raise SystemExit("freeze order")
    inputs = load_inputs()
    futu, flat = fee_fns()
    names = board_tickers(inputs, FORWARD[-1])
    dropped = set(load_drop()["dropped"])
    print("verify freeze on the tune tape", flush=True)
    tune_tape = load_tape("clean", through=TUNE[-1])
    tune_days = prepare_days(inputs, tune_tape, TUNE)
    _verify_freeze(freeze, tune_days, futu)
    versions = {}
    for kind in ("clean", "yahoo"):
        print(f"jump check {kind}", flush=True)
        tape = load_tape(kind, through=FORWARD[-1])
        flags = jump_flags(tape, names, FORWARD[-1], dropped)
        if flags:
            versions[kind] = {"halted": True, "jumps": flags[:12], "n": len(flags)}
            print(f"{kind} halted {len(flags)}", flush=True)
            if kind == "clean":
                raise SystemExit("clean forward tape halted; no returns written")
            continue
        print(f"score {kind}", flush=True)
        days = prepare_days(inputs, tape, SESSIONS)
        scored = _score_version(days, top20, futu, flat)
        print(f"benchmarks {kind}", flush=True)
        iwm = run_iwm(tape, SESSIONS, futu)
        random_books = run_random4(days, futu)
        banned = set(dropped)
        versions[kind] = {
            "forward": _version_block(scored, top20, top10, iwm, random_books, "forward", FORWARD, banned),
            "halted": False,
            "tune": _version_block(scored, top20, top10, iwm, random_books, "tune", TUNE, banned),
        }
    by_id = {row["id"]: row for row in freeze["rank"]}
    rank_head = []
    for fid in top20:
        row = by_id[fid]
        rank_head.append({
            "eligible": row["eligible"],
            "id": fid,
            "inputs": ", ".join(f"{name}={value}" for name, value in active_inputs(BY_ID[fid]).items()),
            "rank_key": row["rank_key"],
            "worst": row["worst"],
        })
    payload = {
        "best": order[0],
        "fingerprint": prereg_fingerprint(),
        "n_grid": LUCK_N,
        "n_passers": len(freeze["passers"]),
        "rank_head": rank_head,
        "top10": top10,
        "top20": top20,
        "versions": versions,
    }
    RETURNS.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT.write_text(_render(payload), encoding="utf-8")
    print(f"wrote {REPORT}", flush=True)


if __name__ == "__main__":
    main()
