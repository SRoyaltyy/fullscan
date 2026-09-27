"""Score the locked price-only fullscan.

Reads the audit marks, the freeze-commit callables, and the long-history
bar cache. It does not edit those inputs. Output is new files under
research/pricefull/results/.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT = ROOT / "research" / "pricefull" / "results"
AUDIT = ROOT / "research" / "audit" / "INPUT_PROVENANCE_336.md"
PREREG = ROOT / "research" / "pricefull" / "PREREG.md"
FREEZE = "3fe544103b94d9795f2b182211e9a8d90d99b95e"
CARDS = "abe8d79facd84c94b61f3158b245f2c50262f1bd"
ENGINE = "76aef1278a53cda755071b7bd970a3f886fcf2cc"
FINGERPRINT = "0dc51aeced8323f5466145e2311acd66a89319548ae5705db06c235b81200f3e"
LISTED_SHA = "62609d9165bcbd5e7327516ea4da03f05297698d67aab660f207ddf8426195d5"
TICKERS_SHA = "40e5b09c5460d71d07947c62c1123d9cd43ec03cab6abcbbb3527acbe074e653"

from research.pricefull.append_only import write_once
from research.pricefull.inclusion import load_audit
from research.pricefull.luck import luck_test
from research.pricefull.rules import (
    CAPITAL,
    empty_ledger,
    holm,
    rerun_empty,
    summarize,
    ticker_pnl,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def _check_fingerprint() -> None:
    raw = PREREG.read_bytes()
    if b"\r" in raw:
        raise SystemExit("PREREG line endings changed")
    text = raw.decode("utf-8")
    marker = "<!-- BEGIN COVERED -->\n"
    body = text[text.find(marker) + len(marker):]
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    if digest != FINGERPRINT:
        raise SystemExit(f"fingerprint {digest} does not match the locked preregistration")


def _freeze_callables() -> dict:
    """Import the freeze tree as ``freeze_src``, not the live ``src`` package."""
    import importlib
    import types

    dest = Path("/tmp/pricefull_freeze")
    if not (dest / "src" / "stock_book.py").is_file():
        dest.mkdir(parents=True, exist_ok=True)
        archive = subprocess.check_output(["git", "archive", FREEZE, "src"], cwd=ROOT)
        subprocess.run(["tar", "-x", "-C", str(dest)], input=archive, check=True)
    pkg = sys.modules.get("freeze_src")
    if pkg is None:
        pkg = types.ModuleType("freeze_src")
        pkg.__path__ = [str(dest / "src")]
        pkg.__package__ = "freeze_src"
        sys.modules["freeze_src"] = pkg
    stock = importlib.import_module("freeze_src.stock_book")
    mine = importlib.import_module("freeze_src.factor_mine")
    book = importlib.import_module("freeze_src.factor_mine_book")
    WEIGHTS = stock.WEIGHTS
    SIGNAL_FAMILIES = stock.SIGNAL_FAMILIES
    effective_weights = stock.effective_weights
    build_recipes = mine.build_recipes
    pick_day = mine.pick_day
    rank_key = mine.rank_key
    split_budgets = book.split_budgets
    vector = tuple(WEIGHTS["1d"])
    if vector != (0.12, 0.10, 0.08, 0.25, 0.25, 0.20):
        raise SystemExit(f"freeze 1d weights {vector} are not the locked vector")
    present = {name: False for name in SIGNAL_FAMILIES}
    scaled, absent = effective_weights(dict(WEIGHTS), present)
    recipes = build_recipes()
    hot = next(row for row in recipes if row.get("name") == "union_hot_n4_h1")
    if int(hot["top_n"]) != 4 or int(hot["hold"]) != 1:
        raise SystemExit("union_hot_n4_h1 body is not top_n 4 and hold 1")
    empty_pick = pick_day([], {"rank": "hot_score", "top_n": 4})
    if empty_pick != []:
        raise SystemExit("pick_day on an empty list did not stay empty")
    budgets = split_budgets([], CAPITAL, "leftover")
    if budgets != []:
        raise SystemExit("split_budgets on nobody did not stay empty")
    # rank_key is imported so the freeze callable is the one this run loaded.
    _ = rank_key
    return {
        "freeze": FREEZE,
        "weights_1d": list(vector),
        "signal_families": list(SIGNAL_FAMILIES),
        "effective_weights_when_none_present": list(scaled["1d"]),
        "absent_families": list(absent),
        "neither_survives": "no score",
        "union_hot_n4_h1": {
            "top_n": hot["top_n"],
            "hold": hot["hold"],
            "rank": hot["rank"],
            "size": hot["size"],
            "sell_on_the_recipe": hot["sell"],
            "side": hot["side"],
            "s_boost": hot["s_boost"],
            "day_cap": hot["day_cap"],
            "take_pct": hot["take_pct"],
            "stop_pct": hot["stop_pct"],
        },
        "shared_order_body": {
            "side": "long",
            "top_n": 4,
            "hold": 1,
            "size": "leftover",
            "sell": "time",
            "s_boost": "none",
            "stop": None,
            "take": None,
            "day_cap": 1,
            "capital": CAPITAL,
        },
        "pick_day_empty": empty_pick,
        "split_budgets_empty": budgets,
    }


def _excel() -> dict:
    names = _git("ls-tree", "-r", "--name-only", CARDS, "excel_bot/strategies").splitlines()
    paths = sorted(path for path in names if path.endswith("/card.json"))
    cards = []
    for path in paths:
        raw = subprocess.check_output(["git", "show", f"{CARDS}:{path}"], cwd=ROOT)
        body = json.loads(raw)
        cohort = str((body.get("spec") or {}).get("cohort_filter"))
        cards.append({"path": path, "name": body.get("name"), "cohort_filter": cohort})
    included = [card for card in cards if card["cohort_filter"] == "ALL"]
    engine_ok = subprocess.call(
        ["git", "cat-file", "-e", f"{ENGINE}:excel_bot/engine/daily_run.py"],
        cwd=ROOT,
    ) == 0
    return {
        "cards_commit": CARDS,
        "engine_commit": ENGINE,
        "engine_file_present": engine_ok,
        "cards": cards,
        "n_cards": len(cards),
        "n_cohort_all": len(included),
        "excel_set": "empty",
        "second_gate_reached": False,
        "counts_as_variant": False,
    }


def _survivorship() -> dict:
    path = ROOT / "research" / "longhist" / "tickers.json"
    listed = ROOT / "research" / "longhist" / "listed_common.txt"
    digest = _sha(path)
    listed_sha = _sha(listed)
    if digest != TICKERS_SHA:
        raise SystemExit(f"tickers.json hash {digest} is not the locked manifest")
    if listed_sha != LISTED_SHA:
        raise SystemExit(f"listed_common.txt hash {listed_sha} is not the locked list")
    payload = json.loads(path.read_text(encoding="utf-8"))
    reasons: dict[str, int] = {}
    for row in payload["symbols"]:
        if row.get("charts"):
            continue
        key = str(row.get("reason"))
        reasons[key] = reasons.get(key, 0) + 1
    return {
        "tickers_sha256": digest,
        "listed_sha256": listed_sha,
        "n_listed": payload["n_listed"],
        "n_delisted_candidates": payload["n_delisted_candidates"],
        "n_yahoo_charts": payload["n_yahoo_charts"],
        "n_yahoo_does_not_chart": payload["n_yahoo_does_not_chart"],
        "n_delisted_served": payload["n_delisted_served"],
        "n_delisted_absent": payload["n_delisted_absent"],
        "no_chart_reasons": reasons,
        "note": (
            "618 delisted candidates do not chart on Yahoo. "
            "The study is not CRSP. The sign of the net bias is not known. "
            "Thresholds are not changed by this count."
        ),
    }


def _dump(obj: dict) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _ledger_bytes(header: dict, daily: list[dict]) -> bytes:
    lines = [json.dumps(header, sort_keys=True, separators=(",", ":"))]
    lines.extend(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in daily)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _variant_rows(names: list[str], ledgers: dict, sessions: list[str]) -> list[dict]:
    tests_p = []
    for name in names:
        # summarize computes p again; holm needs the same p the summary uses.
        from research.pricefull.rules import cluster_test
        tests_p.append(cluster_test([])["p"])
    adjusted = holm(tests_p)
    rows = []
    for name, adj in zip(names, adjusted):
        best = rerun_empty([])
        top3 = rerun_empty([])
        ranked = ticker_pnl(ledgers[name]["trades"])
        best_names = [ranked[0][0]] if ranked else []
        top_names = [ticker for ticker, _pnl in ranked[:3]]
        # No closed trade, so the removed set is empty and the rerun is the
        # original book. The pass still requires the rerun sum to be > 0.
        best = rerun_empty(best_names)
        top3 = rerun_empty(top_names)
        rows.append(summarize(name, ledgers[name], sessions, adj, best, top3))
    return rows


def _money(value) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}"


def _pct(value) -> str:
    if value is None:
        return "—"
    return f"{100.0 * value:.2f}%"


def _plain(value) -> str:
    if value is None:
        return "not defined"
    return str(value)


def _report(scores: dict) -> str:
    lines = [
        "# Price-only fullscan scores",
        "",
        "Window 2019-01-01 through 2026-08-12. Two variants. Holm N and luck-test N are 2.",
        "Every section 4 id failed `REBUILD_MATCH=exact` on `research/audit/INPUT_PROVENANCE_336.md`, so both variants buy nobody.",
        "This is a hindsight study. It is not a live strategy.",
        "",
        "## Result",
        "",
        "| variant | trades | trades per session | Futubull P&L | return | 15bp P&L | study |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in scores["variants"]:
        lines.append(
            f"| `{row['rule']}` | {row['closed_trades']} | {row['trades_per_session']:.4f} | "
            f"{_money(row['closed_pnl'])} | {_pct(row['return_on_10000'])} | "
            f"{_money(row['pnl_15bp_same_shares'])} | {row['label']} |"
        )
    lines += [
        "",
        "| variant | 9.1 mean after Holm | 9.2 years 2019–2024 | 9.3 best removed | 9.4 fires per year | 9.5 P&L from 2025-01-01 | keep bar |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in scores["variants"]:
        lines.append(
            f"| `{row['rule']}` | fail (p={row['p']}, Holm={row['p_holm']}, no positive mean) | "
            f"fail ({row['positive_years_2019_2024']} of 6) | "
            f"fail (rerun P&L {_money(row['pnl_without_best'])}) | "
            f"fail (rate {row['fires_per_year']:.2f}) | "
            f"fail (P&L {_money(row['pnl_2025_01_01_through_2026_08_12'])}) | "
            f"{'pass' if row['ironclad_keep_bar'] else 'fail'} |"
        )
    lines += [
        "",
        "## Per variant",
        "",
    ]
    for row in scores["variants"]:
        lines.append(f"### `{row['rule']}`")
        lines.append("")
        lines.append(
            f"Trades (closed) {row['closed_trades']}. Buy fills {row['buy_fills']}. "
            f"Trades per session {row['trades_per_session']:.4f}. "
            f"Buy fills per session {row['buy_fills_per_session']:.4f}."
        )
        lines.append("")
        lines.append("| year | wins | losses | flat | closed P&L | buy fills |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        years = row["wins_losses_by_exit_year"]
        fills = row["buy_fills_by_year"]
        for year in sorted(years):
            cell = years[year]
            lines.append(
                f"| {year} | {cell['wins']} | {cell['losses']} | {cell['flat']} | "
                f"{_money(cell['pnl'])} | {fills.get(year, 0)} |"
            )
        lines.append("")
        lines.append(
            f"Return without the best stock: {_pct(row['return_without_best'])} "
            f"(P&L {_money(row['pnl_without_best'])}; removed {row['removed_best_ticker'] or 'nobody'})."
        )
        lines.append(
            f"Return without the top 3: {_pct(row['return_without_top3'])} "
            f"(P&L {_money(row['pnl_without_top3'])}; removed {row['removed_top3'] or 'nobody'})."
        )
        share = row["best_share_of_closed_pnl"]
        share_text = "not defined (closed P&L is not strictly positive, and there is no best stock)" if share is None else _pct(share)
        lines.append(f"Best stock's share of closed P&L: {share_text}.")
        lines.append("")
    base = scores["baselines"]
    iwm = base["iwm"]
    lines += [
        "## Baselines",
        "",
        f"RANDOM4, seed 20260813, 1000 draws, 4 names, hold 1, Futubull, $10,000. "
        f"Mean ending-equity return {_pct(base['random4_mean_return'])}.",
        "",
        "| variant | draws whose ending equity this book beats |",
        "| --- | ---: |",
    ]
    for row in scores["variants"]:
        lines.append(f"| `{row['rule']}` | {_pct(row['random4_draws_beaten'])} |")
    lines += [
        "",
        f"IWM buy-and-hold, one round trip, {iwm.get('entry_date')} open to {iwm.get('exit_date')} close. "
        f"Futubull P&L {_money(iwm.get('pnl'))} ({_pct(iwm.get('return_on_10000'))}). "
        f"Flat 15bp P&L {_money(iwm.get('pnl_15bp'))} ({_pct(iwm.get('return_15bp'))}).",
        "",
        "## Luck test",
        "",
        f"Seed {scores['luck']['seed']}. Reshuffles {scores['luck']['reshuffles']}. "
        f"Permutation calls {scores['luck']['permutation_calls']}.",
        "",
    ]
    for rule in scores["luck"]["rules"]:
        lines.append(
            f"`{rule['rule']}` pooled 1-share picks {rule['pooled_picks']}. "
            f"Real 1-share mean {_plain(rule['real_1share_mean'])}. "
            f"p_rule {_plain(rule['p_rule'])}. {rule['p_rule_reason']}."
        )
    lines.append("")
    lines.append(
        f"p_best {_plain(scores['luck']['p_best'])}. {scores['luck']['p_best_reason']}."
    )
    lines += [
        "",
        "## What was dropped",
        "",
        "The audit file is on this commit. No section 4 id has `REBUILD_MATCH` equal to `exact`. "
        "A different spelling in that table (hot score, prior-day gainers, VIX, rates) is not the id. "
        "`A06_volume_red_green_2day` and `A14_profitable_oversold_setup` are on a REBUILD_MATCH row and the cell is not `exact`. "
        "A14 would stay out under section 3 even if that cell were exact.",
        "",
        "With ab and peer both absent, `effective_weights` at the freeze returns the original 1d vector because no family survives. "
        "Section 5.2 then gives the variant no score. `pricefull_w1d` does not fall through to `hot_score`. "
        "`pricefull_hot4` does not use the full proxy as a backup list.",
        "",
        "The Excel set is empty: zero cards at the locked commit have `cohort_filter` `ALL`. "
        "That set is not a variant and does not add to N. The second gate is not reached.",
        "",
        "## Prices and survivorship",
        "",
        "Fills would use the long-history Yahoo chart v8 `indicators.quote` cache "
        "(split-adjusted, not dividend-adjusted). `adj_close` is on the file and is not read. "
        "Both books have zero fills, so no trade interval contains a split ex-date. "
        "The long-history jump check classifies Yahoo split events against the cached open and the prior close. "
        "Those event payloads are not in this repository, and this run does not download a new tape or rewrite a stored bar.",
        "",
    ]
    surv = scores["survivorship"]
    reasons = surv["no_chart_reasons"]
    lines.append(
        f"Delisted candidates {surv['n_delisted_candidates']}. "
        f"Yahoo does not chart {surv['n_yahoo_does_not_chart']}: "
        f"HTTP 404 {reasons.get('no_bar_http_404', 0)}, "
        f"HTTP 400 {reasons.get('no_bar_http_400', 0)}, "
        f"HTTP 200 with no bar on or before 2026-08-12 {reasons.get('no_bar_http_200', 0)}. "
        "The study is not CRSP. The sign of the net bias is not known."
    )
    lines.append("")
    lines.append(f"Sessions {scores['n_sessions']}, from {scores['first_session']} through {scores['last_session']}.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    _check_fingerprint()
    decision = load_audit(AUDIT)
    if any(row["buys"] for row in decision["variants"].values()):
        blocker = {
            "blocker": "an id is exact and this run has no reader for a surviving family",
            "decision": decision,
        }
        write_once(OUT / "BLOCKER.json", _dump(blocker))
        raise SystemExit("exact id present; refusing to invent a feature path")
    freeze = _freeze_callables()
    excel = _excel()
    if excel["n_cohort_all"] != 0 or excel["n_cards"] != 7:
        raise SystemExit("excel card set is not the locked seven with zero ALL cohorts")
    surv = _survivorship()

    from research.longhist.engine import (
        attach_baselines,
        audit_panel,
        build_panel,
        fee_model,
        iwm_book,
        load_tapes,
        random4,
        sessions_between,
    )

    sessions = sessions_between("2019-01-01", "2026-08-12")
    session_bytes = (json.dumps(sessions) + "\n").encode("utf-8")
    # The session list is written before the first score.
    write_once(OUT / "sessions.json", session_bytes)
    saved = json.loads((OUT / "sessions.json").read_text(encoding="utf-8"))
    if saved != sessions:
        raise SystemExit("saved session list does not match XNYS")

    names = ["pricefull_w1d", "pricefull_hot4"]
    ledgers = {name: empty_ledger(name, sessions) for name in names}
    rows = _variant_rows(names, ledgers, sessions)
    header_common = {
        "sessions_sha256": hashlib.sha256(session_bytes).hexdigest(),
        "audit_sha256": _sha(AUDIT),
        "buys": 0,
        "reason": "no section 4 id is REBUILD_MATCH=exact",
    }
    for name in names:
        header = {"type": "header", "rule": name, **header_common}
        write_once(OUT / f"{name}_ledger.jsonl", _ledger_bytes(header, ledgers[name]["daily"]))

    print(f"sessions {len(sessions)} {sessions[0]} {sessions[-1]}", flush=True)
    fees = fee_model()
    print("loading tapes", flush=True)
    tapes = load_tapes("2026-08-12")
    print(f"tapes {len(tapes)}", flush=True)
    panel = build_panel(tapes, sessions)
    audit_panel(tapes, panel, sessions)
    print("random4", flush=True)
    equities = random4(panel, tapes, sessions, fees)
    iwm = iwm_book(tapes, sessions, fees)
    # attach_baselines writes onto dicts that already have ending_equity.
    for row in rows:
        row["ending_equity"] = CAPITAL
    attach_baselines(rows, equities, iwm)
    print("luck", flush=True)
    luck_books = [
        {"rule": name, "hold": 1, "buy_fills": ledgers[name]["buy_fills"]}
        for name in names
    ]
    luck = luck_test(panel, tapes, sessions, fees, luck_books)
    if luck["permutation_calls"] != luck["expected_permutation_calls"]:
        raise SystemExit("luck-test generator call count does not match")

    mean_ret = float(sum(eq / CAPITAL - 1.0 for eq in equities) / len(equities))
    scores = {
        "window": ["2019-01-01", "2026-08-12"],
        "n_sessions": len(sessions),
        "first_session": sessions[0],
        "last_session": sessions[-1],
        "head": _git("rev-parse", "HEAD"),
        "audit_sha256": header_common["audit_sha256"],
        "prereg_fingerprint": FINGERPRINT,
        "inclusion": {
            "exact_ids": decision["exact_ids"],
            "dropped_ids": decision["dropped_ids"],
            "variants": decision["variants"],
            "hot_score_usable": decision["hot_score_usable"],
            "ab_survives": decision["ab_survives"],
            "peer_survives": decision["peer_survives"],
            "found_marks": [
                {"id": row["id"], "marks": row["marks"], "lines": row["lines"]}
                for row in decision["ids"]
                if row["marks"]
            ],
        },
        "freeze": freeze,
        "excel": excel,
        "survivorship": surv,
        "variants": rows,
        "baselines": {
            "random4_draws": len(equities),
            "random4_seed": 20260813,
            "random4_mean_return": mean_ret,
            "iwm": iwm,
        },
        "luck": luck,
        "price_source": {
            "cache": "research/longhist/bars/ohlcv",
            "fields_read": ["open", "high", "low", "close", "volume"],
            "adj_close_read": False,
            "jump_check": (
                "longhist mechanics_check classifies Yahoo split events. "
                "Event JSON is not in the repo. Zero fills, so no trade spans an ex-date."
            ),
        },
    }
    write_once(OUT / "scores.json", _dump(scores))
    write_once(OUT / "SCORES.md", _report(scores).encode("utf-8"))
    print("wrote", OUT, flush=True)


if __name__ == "__main__":
    main()
