"""Run Part 1, then Part 2 train, then Part 2 test.

Part 2 train refuses any bar after 2023-12-31. The test command
checks part2_frozen.json and does not run without that hash.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from research.longhist.engine import (
    attach_baselines,
    audit_panel,
    best_ticker,
    build_panel,
    fee_model,
    freeze_hash,
    iwm_book,
    load_tapes,
    luck_test,
    part1_rules,
    part2_rules,
    random4,
    rule_body,
    score_books,
    sessions_between,
    simulate,
)

ROOT = Path("research/longhist")
OUT = ROOT / "results"
FROZEN = ROOT / "part2_frozen.json"


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return "inf"
        return value
    if hasattr(value, "item"):
        return clean(value.item())
    return value


def dump(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(clean(payload), indent=2) + "\n")


def write_daily(path: Path, books, stage: str) -> None:
    rows = []
    for book in books:
        for day, equity, ret, cash, npos in book.daily:
            rows.append({
                "stage": stage,
                "rule": book.rule,
                "date": day,
                "equity": equity,
                "daily_return": ret,
                "cash": cash,
                "positions": npos,
            })
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("stage", pa.string()),
        ("rule", pa.string()),
        ("date", pa.string()),
        ("equity", pa.float64()),
        ("daily_return", pa.float64()),
        ("cash", pa.float64()),
        ("positions", pa.int64()),
    ]))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")


def write_trades(path: Path, books, stage: str) -> None:
    rows = []
    for book in books:
        for trade in book.trades:
            rows.append({
                "stage": stage,
                "rule": book.rule,
                "ticker": trade.ticker,
                "entry_date": trade.entry_date,
                "exit_date": trade.exit_date,
                "shares": trade.shares,
                "entry_px": trade.entry_px,
                "exit_px": trade.exit_px,
                "buy_fee": trade.buy_fee,
                "sell_fee": trade.sell_fee,
                "pnl": trade.pnl,
                "ret": trade.ret,
                "alarm": False,
            })
    if not rows:
        rows.append({
            "stage": stage, "rule": "", "ticker": "", "entry_date": "",
            "exit_date": "", "shares": 0, "entry_px": 0.0, "exit_px": 0.0,
            "buy_fee": 0.0, "sell_fee": 0.0, "pnl": 0.0, "ret": 0.0,
            "alarm": False,
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")


def money(value) -> str:
    if value is None:
        return ""
    return f"{value:,.2f}"


def pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * value:.2f}%"


def render(title: str, rows: list[dict], note: str) -> str:
    lines = [f"# {title}", "", note, ""]
    lines.append("| rule | 6.1 mean>0 Holm | years | best removed | >=30/yr | study | trades | buy fills | Futubull return | removed return | closed P&L | removed closed P&L |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            "| {rule} | {a} | {b} | {c} | {d} | {label} | {trades} | {buys} | {ret} | {rret} | {pnl} | {rpnl} |".format(
                rule=row["rule"],
                a="pass" if row["pass_6_1_mean"] else "fail",
                b="pass" if row["pass_years"] else "fail",
                c="pass" if row["pass_6_3_best_removed"] else "fail",
                d="pass" if row["pass_6_4_rate"] else "fail",
                label=row["label"],
                trades=row["closed_trades"],
                buys=row["buy_fills"],
                ret=pct(row["return_on_10000"]),
                rret=pct(row["removed_return_on_10000"]),
                pnl=money(row["closed_pnl"]),
                rpnl=money(row["removed_closed_pnl"]),
            )
        )
    lines.append("")
    iwm = rows[0].get("iwm") or {}
    if iwm:
        lines.append(
            f"RANDOM4 mean ending-equity return: {pct(rows[0].get('random4_mean_return'))}."
        )
        lines.append("")
        if iwm.get("ok"):
            lines.append(
                f"IWM {iwm['entry_date']} → {iwm['exit_date']}: "
                f"Futubull return {pct(iwm['return_on_10000'])}, "
                f"15bp return {pct(iwm['return_15bp'])}."
            )
        else:
            lines.append(f"IWM: {iwm.get('reason')}.")
        lines.append("")
    lines.append("## Per rule")
    lines.append("")
    for row in rows:
        lines.append(f"### {row['rule']}")
        lines.append("")
        lines.append(
            f"Cluster mean {row['cluster_mean']:.6f}, t {row['t']}, "
            f"p {row['p']}, Holm p {row['p_holm']:.6g}, "
            f"entry days {row['cluster_n']}."
        )
        lines.append("")
        lines.append(
            f"Fires/year {row['fires_per_year']:.2f}. "
            f"Buys by year {json.dumps(row['buy_fills_by_year'])}."
        )
        lines.append("")
        lines.append(
            f"Ending equity {money(row['ending_equity'])}. "
            f"15bp closed P&L on the same shares {money(row['pnl_15bp_same_shares'])}. "
            f"Win rate {pct(row['win_rate'])}. "
            f"Best stock {row['best_ticker']}. "
            f"RANDOM4 beaten {pct(row.get('random4_draws_beaten'))}."
        )
        lines.append("")
        lines.append(f"Year closed P&L {json.dumps({k: round(v, 2) for k, v in row['year_pnl'].items()})}.")
        lines.append("")
        if "p_rule" in row:
            lines.append(
                f"Luck test 1-share mean {row.get('real_1share_mean')}, "
                f"p_rule {row.get('p_rule')}."
            )
            lines.append("")
    if rows and rows[0].get("p_best") is not None:
        lines.append(f"Best-of-N luck p: {rows[0]['p_best']}.")
        lines.append("")
    return "\n".join(lines)


def run_slice(rules, sessions, tapes, fees, family: str):
    print(f"building panel {family} sessions {len(sessions)}", flush=True)
    panel = build_panel(tapes, sessions)
    audit_panel(tapes, panel, sessions)
    books = []
    reruns = []
    for rule in rules:
        book = simulate(panel, tapes, sessions, rule, fees)
        banned = best_ticker(book.trades)
        rerun = simulate(panel, tapes, sessions, rule, fees, banned=banned)
        books.append(book)
        reruns.append(rerun)
        print(
            f"{rule.name} equity {book.final_equity:.2f} "
            f"trades {len(book.trades)} buys {len(book.buy_dates)} ban {banned}",
            flush=True,
        )
    rows = score_books(books, reruns, len(sessions), family)
    print("RANDOM4", flush=True)
    equities = random4(panel, tapes, sessions, fees)
    iwm = iwm_book(tapes, sessions, fees)
    attach_baselines(rows, equities, iwm)
    return panel, books, rows, equities


def merge_luck(rows, luck) -> None:
    by = {item["rule"]: item for item in luck["rules"]}
    for row in rows:
        item = by[row["rule"]]
        row["real_1share_mean"] = item["real_1share_mean"]
        row["pooled_picks"] = item["pooled_picks"]
        row["p_rule"] = item["p_rule"]
        row["p_best"] = luck["p_best"]
        row["real_best_1share_mean"] = luck["real_best_1share_mean"]


def part1() -> None:
    fees = fee_model()
    sessions = sessions_between("2019-01-01", "2026-08-12")
    dump(OUT / "sessions_part1.json", {"sessions": sessions, "n": len(sessions)})
    print("loading bars through 2026-08-12", flush=True)
    tapes = load_tapes("2026-08-12")
    rules = part1_rules()
    panel, books, rows, _eq = run_slice(rules, sessions, tapes, fees, "part1")
    write_daily(OUT / "daily_returns_part1.parquet", books, "part1")
    write_trades(OUT / "trades_part1.parquet", books, "part1")
    dump(OUT / "part1.json", {"rows": rows, "luck": None})
    (OUT / "part1.md").write_text(render(
        "Part 1",
        rows,
        "Window 2019-01-01 through 2026-08-12. Year pass is 4 of 2019–2024. "
        "Futubull return is ending equity over $10,000, including open marks. "
        "Removed return is the rerun with the best closed-P&L ticker ineligible. "
        "Luck test follows.",
    ))
    print("luck part1", flush=True)
    luck = luck_test(books, panel, tapes, sessions, fees)
    merge_luck(rows, luck)
    dump(OUT / "part1.json", {"n_sessions": len(sessions), "rows": rows, "luck": luck})
    (OUT / "part1.md").write_text(render(
        "Part 1",
        rows,
        "Window 2019-01-01 through 2026-08-12. Year pass is 4 of 2019–2024. "
        "Futubull return is ending equity over $10,000, including open marks. "
        "Removed return is the rerun with the best closed-P&L ticker ineligible.",
    ))
    print("part1 done", flush=True)


def part2_train() -> None:
    fees = fee_model()
    sessions = sessions_between("2019-01-01", "2023-12-31")
    if any(day >= "2024-01-01" for day in sessions):
        raise RuntimeError("train session list contains the test window")
    print("loading bars through 2023-12-31", flush=True)
    tapes = load_tapes("2023-12-31")
    newest = max(int(tape.dates[-1]) for tape in tapes.values())
    if newest >= 20240101:
        raise RuntimeError(f"train tapes include {newest}")
    rules = part2_rules()
    panel, books, rows, _eq = run_slice(rules, sessions, tapes, fees, "part2_train")
    write_daily(OUT / "daily_returns_part2_train.parquet", books, "part2_train")
    write_trades(OUT / "trades_part2_train.parquet", books, "part2_train")
    passed = [row for row in rows if row["study_pass"]]
    def sort_key(row):
        tstat = row["t"]
        tval = tstat if isinstance(tstat, float) and math.isfinite(tstat) else 1e300
        return (-row["closed_pnl"], -tval, row["rule"])
    passed.sort(key=sort_key)
    kept = passed[:5]
    by_name = {rule.name: rule for rule in rules}
    bodies = [rule_body(by_name[row["rule"]]) for row in kept]
    payload = {
        "sha256": freeze_hash(bodies),
        "train_end": "2023-12-31",
        "test_not_scored": True,
        "n_grid": 40,
        "n_passed_train": len(passed),
        "winners": bodies,
        "selection": [
            {"name": row["rule"], "closed_pnl": row["closed_pnl"], "t": row["t"]}
            for row in kept
        ],
    }
    dump(FROZEN, payload)
    if freeze_hash(payload["winners"]) != payload["sha256"]:
        raise RuntimeError("frozen hash mismatch before write-back")
    print("luck part2 train", flush=True)
    luck = luck_test(books, panel, tapes, sessions, fees)
    merge_luck(rows, luck)
    dump(OUT / "part2_train.json", {
        "n_sessions": len(sessions),
        "rows": rows,
        "luck": luck,
        "frozen_sha256": payload["sha256"],
        "winners": [row["rule"] for row in kept],
    })
    (OUT / "part2_train.md").write_text(render(
        "Part 2 train",
        rows,
        "Train only, 2019-01-01 through 2023-12-31. No bar after that date was loaded. "
        "A winner needs the train four: Holm N=40, 4 of 2019–2023, best stock removed, "
        f"and the fire rate. Frozen {len(kept)} of {len(passed)} that passed. "
        f"Hash {payload['sha256']}. The test window is not scored in this file.",
    ))
    print("part2 train done", payload["sha256"], flush=True)


def part2_test() -> None:
    if not FROZEN.exists():
        raise RuntimeError("part2_frozen.json is missing")
    payload = json.loads(FROZEN.read_text())
    digest = freeze_hash(payload["winners"])
    if digest != payload["sha256"]:
        raise RuntimeError(f"frozen hash {payload['sha256']} != {digest}")
    fees = fee_model()
    sessions = sessions_between("2024-01-01", "2026-08-12")
    print("loading bars for the test slice", flush=True)
    tapes = load_tapes("2026-08-12")
    rules = part2_rules()
    panel, books, rows, _eq = run_slice(rules, sessions, tapes, fees, "part2_test")
    frozen = {body["name"] for body in payload["winners"]}
    for row in rows:
        row["frozen_winner"] = row["rule"] in frozen
        row["called_winner"] = bool(row["frozen_winner"] and row["study_pass"])
    write_daily(OUT / "daily_returns_part2_test.parquet", books, "part2_test")
    write_trades(OUT / "trades_part2_test.parquet", books, "part2_test")
    print("luck part2 test", flush=True)
    luck = luck_test(books, panel, tapes, sessions, fees)
    merge_luck(rows, luck)
    dump(OUT / "part2_test.json", {
        "n_sessions": len(sessions),
        "frozen_sha256": digest,
        "rows": rows,
        "luck": luck,
    })
    (OUT / "part2_test.md").write_text(render(
        "Part 2 test",
        rows,
        "Test window 2024-01-01 through 2026-08-12, scored once. "
        "Year column is the addendum: 2024 closed P&L > 0 and 2025-01-01..2026-08-12 "
        "closed P&L > 0. Holm N=40 on every rule. "
        f"Frozen hash {digest}. Only a frozen name that also passes the four is called a winner.",
    ))
    # One parquet of every daily series tried.
    tables = []
    for name in (
        "daily_returns_part1.parquet",
        "daily_returns_part2_train.parquet",
        "daily_returns_part2_test.parquet",
    ):
        path = OUT / name
        if path.exists():
            tables.append(pq.read_table(path))
    if tables:
        pq.write_table(pa.concat_tables(tables), OUT / "daily_returns.parquet", compression="zstd")
    print("part2 test done", flush=True)


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "part1":
        part1()
    elif cmd == "part2-train":
        part2_train()
    elif cmd == "part2-test":
        part2_test()
    else:
        raise SystemExit("usage: part1 | part2-train | part2-test")


if __name__ == "__main__":
    main()
