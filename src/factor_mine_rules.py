"""Append-only Factor Mine rules.

A locked recipe name keeps one definition hash. A later run that finds
the same name with a different hash fails and does not rescore. Days
before a recipe's creation date are ``designed_after`` and stay out of
the real total. A locked ledger day keeps its picks, fills, and P&L.
The first backfill of a new recipe name is the exception.
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_freeze as fmf

ROOT = Path(__file__).resolve().parent.parent
FEE_PATH = ROOT / "00_grounding" / "futubull_fees.json"
LEDGER_DIR = ROOT / "data" / "factor_mine" / "ledgers"
DESIGNED_CSV = ROOT / "03_scoreboard" / "factor_mine_designed_after.csv"
DESIGNED_JSON = ROOT / "03_scoreboard" / "factor_mine_designed_after.json"
DESIGNED_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_DESIGNED_AFTER.md"
RULES_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_RULES.md"
SAME_BAR = "stop_first"
FILL_SIDES = ("BUY", "SELL", "SHORT", "COVER")
BOOK_START = "2026-08-13"

# Hashed definition. Prose (note, explain) is not a rule.
_BODY_KEYS = (
    "universe", "hold", "side", "top_n", "require", "forbid", "rank",
    "exit_when", "size", "sell", "s_boost", "day_cap", "take_pct",
    "stop_pct", "members", "weights", "net", "pool", "hold_mix",
)


class RuleDrift(SystemExit):
    """Same recipe name, different definition. Do not rescore."""


class AppendDrift(SystemExit):
    """A locked day's picks, fills, or P&L would change."""


def _canonical(obj) -> bytes:
    return fmf.canonical_bytes(obj)


def _sha(obj) -> str:
    return hashlib.sha256(_canonical(obj)).hexdigest()


def _round(value, digits: int):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits)


def fee_model_sha(path: Path | None = None) -> str:
    """Hash the priced schedule. Comment keys are not part of the model."""
    src = Path(path or FEE_PATH)
    doc = json.loads(src.read_text(encoding="utf-8"))
    priced = {
        key: value for key, value in doc.items()
        if not str(key).startswith("_")
    }
    return _sha(priced)


def recipe_body(rec: dict, *, fee_sha: str | None = None) -> dict:
    """Full definition that the fingerprint covers."""
    rec = rec or {}
    name = str(rec.get("name") or "")
    body = {
        "name": name,
        "same_bar": SAME_BAR,
        "fee_model_sha256": fee_sha or fee_model_sha(),
        "created_on": fm.recipe_created_on(name, rec),
    }
    for key in _BODY_KEYS:
        if key == "hold":
            body[key] = int(rec.get("hold") or 1)
        elif key == "top_n":
            body[key] = int(rec.get("top_n") or 0)
        elif key == "day_cap":
            body[key] = float(rec.get("day_cap") if rec.get("day_cap") is not None else 1.0)
        elif key in ("require", "forbid", "exit_when"):
            body[key] = dict(rec.get(key) or {})
        elif key == "members":
            body[key] = [str(m) for m in (rec.get("members") or []) if m]
        elif key == "weights":
            body[key] = [float(w) for w in (rec.get("weights") or [])]
        elif key == "hold_mix":
            body[key] = bool(rec.get("hold_mix"))
        elif key in ("take_pct", "stop_pct"):
            body[key] = _round(rec.get(key), 6)
        else:
            body[key] = rec.get(key)
    return body


def recipe_fingerprint(rec: dict, *, fee_sha: str | None = None) -> str:
    """Hash of gates, weights, hold, exits, sizing, and the fee model."""
    return _sha(recipe_body(rec, fee_sha=fee_sha))


def lock_recipe_rules(recipes: list[dict], *, write: bool = True,
                      locked_on: str | None = None) -> dict:
    """Store each recipe hash the first time. A changed hash fails loudly.

    Does not rescore. Changing the rules means a new name.
    """
    man = fmf.load_manifest()
    rules = man.setdefault("recipe_rules", {})
    fee_sha = fee_model_sha()
    stamp = str(locked_on or "2026-09-25")[:10]
    changed = False
    for rec in recipes or []:
        if not isinstance(rec, dict) or not rec.get("name"):
            continue
        name = str(rec["name"])
        digest = recipe_fingerprint(rec, fee_sha=fee_sha)
        have = rules.get(name) or {}
        prev = have.get("sha256")
        if prev and prev != digest:
            raise RuleDrift(
                f"recipe {name} definition changed {prev} -> {digest}. "
                f"Not rescoring. Changing the rules means a new name."
            )
        if not prev:
            rules[name] = {
                "sha256": digest,
                "locked_on": stamp,
                "same_bar": SAME_BAR,
                "created_on": fm.recipe_created_on(name, rec),
            }
            changed = True
    if write and changed:
        fmf.save_manifest(man)
    return man


def is_designed_after(name: str, session: str, rec: dict | None = None) -> bool:
    """True for a session before this recipe existed."""
    created = fm.recipe_created_on(name, rec)
    return str(session)[:10] < created


def first_real_day(name: str, locked_dates: list[str],
                   rec: dict | None = None) -> str | None:
    """First locked session on or after creation. Earlier days are designed_after."""
    created = fm.recipe_created_on(name, rec)
    days = sorted(str(d)[:10] for d in locked_dates if d and str(d)[:10] >= created)
    return days[0] if days else None


def session_label(name: str, session: str, rec: dict | None = None) -> str:
    if is_designed_after(name, session, rec):
        return "designed_after"
    return "real"


def compound_pct(means: list[float]) -> float | None:
    """Chain session percents. Empty input is not a zero total."""
    if not means:
        return None
    acc = 1.0
    for mean in means:
        acc *= 1.0 + float(mean) / 100.0
    return round((acc - 1.0) * 100.0, 3)


def split_means(name: str, rows: list[dict], rec: dict | None = None) -> dict:
    """Real compound excludes designed_after days."""
    real: list[float] = []
    designed: list[float] = []
    created = fm.recipe_created_on(name, rec)
    dates = []
    for row in rows or []:
        date = str(row.get("date") or "")[:10]
        if not date:
            continue
        dates.append(date)
        mean = row.get("mean")
        if mean is None:
            continue
        if is_designed_after(name, date, rec):
            designed.append(float(mean))
        else:
            real.append(float(mean))
    return {
        "name": name,
        "created_on": created,
        "first_real_day": first_real_day(name, dates, rec),
        "n_real": len(real),
        "n_designed_after": len(designed),
        "real_compound_pct": compound_pct(real),
        "designed_after_compound_pct": compound_pct(designed),
    }


def _fills(trades: list | None, date: str) -> list[tuple]:
    out = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("date") or "")[:10] != date:
            continue
        side = trade.get("side")
        if side not in FILL_SIDES:
            continue
        shares = trade.get("shares")
        try:
            shares_n = int(shares) if shares is not None and float(shares) == int(float(shares)) else shares
        except (TypeError, ValueError):
            shares_n = shares
        out.append((
            side,
            str(trade.get("ticker") or ""),
            shares_n,
            _round(trade.get("price"), 4),
        ))
    return sorted(out, key=lambda item: tuple("" if part is None else str(part) for part in item))


def _pnl(row: dict) -> tuple:
    return (_round(row.get("equity"), 2), _round(row.get("mean"), 4))


def _name_list(values) -> list:
    if not values:
        return []
    out = []
    for item in values:
        if isinstance(item, dict):
            out.append(str(item.get("ticker") or ""))
        else:
            out.append(str(item))
    return out


def assert_scoreboard_append(prior: dict | None, proposed: dict | None) -> None:
    """Fail when a locked scoreboard day changes. A new recipe may land once."""
    prior = prior or {}
    proposed = proposed or {}
    prior_daily = prior.get("daily") or {}
    new_daily = proposed.get("daily") or {}
    if not prior_daily:
        return
    if "daily" not in proposed:
        raise AppendDrift(
            "scoreboard publish drops locked daily rows. "
            "Not rescoring a locked day."
        )
    prior_books = prior.get("books") or {}
    new_books = proposed.get("books") or {}
    for name, rows in prior_daily.items():
        if name not in new_daily:
            raise AppendDrift(
                f"ledger drop {name}: locked recipe left the board. "
                f"A new name may backfill once; an old name stays."
            )
        new_by_date = {
            str(row.get("date") or "")[:10]: row
            for row in (new_daily.get(name) or [])
            if isinstance(row, dict) and row.get("date")
        }
        old_trades = (prior_books.get(name) or {}).get("trades")
        new_trades = (new_books.get(name) or {}).get("trades")
        for row in rows or []:
            if not isinstance(row, dict) or not row.get("date"):
                continue
            date = str(row["date"])[:10]
            got = new_by_date.get(date)
            if got is None:
                raise AppendDrift(
                    f"ledger drop {name} {date}: locked day removed"
                )
            if _name_list(row.get("bought")) != _name_list(got.get("bought")):
                raise AppendDrift(
                    f"ledger rewrite {name} {date} picks bought "
                    f"{_name_list(row.get('bought'))} -> {_name_list(got.get('bought'))}"
                )
            if _name_list(row.get("sold")) != _name_list(got.get("sold")):
                raise AppendDrift(
                    f"ledger rewrite {name} {date} picks sold "
                    f"{_name_list(row.get('sold'))} -> {_name_list(got.get('sold'))}"
                )
            if _pnl(row) != _pnl(got):
                raise AppendDrift(
                    f"ledger rewrite {name} {date} pnl {_pnl(row)} -> {_pnl(got)}"
                )
            old_fills = _fills(old_trades, date)
            new_fills = _fills(new_trades, date)
            if old_fills != new_fills:
                raise AppendDrift(
                    f"ledger rewrite {name} {date} fills {old_fills} -> {new_fills}"
                )


def _slot(doc: dict, name: str) -> dict:
    rec = (doc.get("recipes") or {}).get(name) or {}
    if isinstance(rec.get("primary"), dict):
        rec = rec["primary"]
    return rec if isinstance(rec, dict) else {}


def _ledger_signature(slot: dict) -> dict:
    daily = slot.get("daily") if isinstance(slot.get("daily"), dict) else slot
    buys = slot.get("buys")
    if buys is None:
        buys = slot.get("bought")
    sells = slot.get("sells")
    if sells is None:
        sells = slot.get("sold")
    trades = slot.get("trades")
    if trades is None:
        trades = slot.get("fills")
    return {
        "buys": _order_list(buys),
        "sells": _order_list(sells),
        "fills": _order_list(trades),
        "equity": _round(daily.get("equity"), 2),
        "mean": _round(daily.get("mean"), 4),
    }


def _order_list(values) -> list:
    """Picks and fills, stable for a string ticker or a trade dict."""
    out = []
    for item in values or []:
        if isinstance(item, str):
            out.append(item)
            continue
        if not isinstance(item, dict):
            out.append(str(item))
            continue
        side = item.get("side")
        if side and side not in FILL_SIDES and side not in ("BUY", "SELL", "SHORT", "COVER"):
            continue
        shares = item.get("shares")
        try:
            shares_n = int(shares) if shares is not None and float(shares) == int(float(shares)) else shares
        except (TypeError, ValueError):
            shares_n = shares
        out.append((
            side or "",
            str(item.get("ticker") or ""),
            shares_n,
            _round(item.get("price"), 4),
        ))
    return out


def assert_ledger_append(prior: dict | None, proposed: dict | None) -> None:
    """Fail when an existing recipe row on a locked day changes.

    A recipe name that is not in ``prior`` may be written once.
    """
    prior = prior or {}
    proposed = proposed or {}
    old_names = set((prior.get("recipes") or {}))
    new_names = set((proposed.get("recipes") or {}))
    if not old_names:
        return
    for name in sorted(old_names - new_names):
        raise AppendDrift(
            f"ledger drop {name}: locked recipe row removed. "
            f"The first backfill of a new recipe is the only add."
        )
    for name in sorted(old_names):
        old_sig = _ledger_signature(_slot(prior, name))
        new_sig = _ledger_signature(_slot(proposed, name))
        if old_sig != new_sig:
            raise AppendDrift(
                f"ledger rewrite {name} {prior.get('date') or proposed.get('date')} "
                f"{old_sig} -> {new_sig}"
            )


def write_day_ledger(date: str, doc: dict, *, path: Path | None = None) -> Path:
    """Write one locked day. An existing row that differs is refused."""
    dest = Path(path or (LEDGER_DIR / f"{date}.json"))
    payload = dict(doc or {})
    payload["date"] = str(date)[:10]
    if dest.is_file():
        prior = json.loads(dest.read_text(encoding="utf-8"))
        assert_ledger_append(prior, payload)
        raw = _canonical(payload)
        if dest.read_bytes() == raw:
            return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(_canonical(payload))
    return dest


def same_bar_counts(recipes: list[dict]) -> list[dict]:
    """Trades a same-bar take-and-stop can touch.

    A recipe with no take-profit and no stop has zero. A stop with no
    take-profit cannot double-touch, so that count is zero too. This
    reads the definition only. It does not rescore.
    """
    rows = []
    for rec in recipes or []:
        if not isinstance(rec, dict) or not rec.get("name"):
            continue
        take = rec.get("take_pct")
        stop = rec.get("stop_pct")
        both = take is not None and stop is not None
        rows.append({
            "name": rec["name"],
            "take_pct": take,
            "stop_pct": stop,
            "same_bar_trades": 0 if not both else None,
        })
    return rows


def _index_recipes(recipes: list[dict]) -> dict[str, dict]:
    return {
        str(rec.get("name")): rec
        for rec in recipes or []
        if isinstance(rec, dict) and rec.get("name")
    }


def publish_rule_pages(payload: dict | None = None) -> dict:
    """Write the designed_after scoreboard beside the live board.

    Real compounds never include a designed_after day. The dashboard
    HTML is left as it is.
    """
    payload = payload or {}
    if not payload.get("daily") or not payload.get("recipes"):
        if fm.OUT_JSON.is_file():
            payload = fm.load_scoreboard()
    recipes = list(payload.get("daily") and payload.get("recipes") or [])
    by_name = _index_recipes(payload.get("recipes") or [])
    daily = payload.get("daily") or {}
    summaries = []
    csv_rows = []
    for name in sorted(daily):
        rec = by_name.get(name) or {"name": name}
        rows = list(daily.get(name) or [])
        summaries.append(split_means(name, rows, rec))
        created = fm.recipe_created_on(name, rec)
        for row in rows:
            date = str(row.get("date") or "")[:10]
            if not date:
                continue
            label = session_label(name, date, rec)
            csv_rows.append({
                "recipe": name,
                "created_on": created,
                "date": date,
                "label": label,
                "mean": "" if row.get("mean") is None else row.get("mean"),
                "in_real_total": "false" if label == "designed_after" else "true",
            })
    counts = same_bar_counts(list(by_name.values()) or recipes)
    both_levels = [
        row["name"] for row in counts
        if row["take_pct"] is not None and row["stop_pct"] is not None
    ]
    stop_only = [
        row["name"] for row in counts
        if row["stop_pct"] is not None and row["take_pct"] is None
    ]
    designed = [row for row in summaries if row["n_designed_after"]]
    doc = {
        "same_bar": SAME_BAR,
        "book_start": BOOK_START,
        "n_recipes": len(summaries),
        "n_with_designed_after": len(designed),
        "same_bar_trades_affected": 0 if not both_levels else None,
        "stop_only_recipes": stop_only,
        "recipes_with_both_take_and_stop": both_levels,
        "recipes": summaries,
        "dashboard_html": "unchanged",
    }
    DESIGNED_CSV.parent.mkdir(parents=True, exist_ok=True)
    with DESIGNED_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "recipe", "created_on", "date", "label", "mean", "in_real_total",
        ])
        writer.writeheader()
        writer.writerows(csv_rows)
    DESIGNED_JSON.write_text(
        json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    DESIGNED_MD.write_text(_designed_md(doc), encoding="utf-8")
    RULES_MD.write_text(_rules_md(doc), encoding="utf-8")
    return doc


def _fmt(value) -> str:
    if value is None:
        return "—"
    return f"{float(value):.3f}"


def _designed_md(doc: dict) -> str:
    lines = [
        "# Factor Mine designed_after",
        "",
        "A day before the recipe existed is `designed_after`. "
        "It is labeled with the creation date and kept out of the real total. "
        "The first real test day is the first locked session on or after creation.",
        "",
        f"Recipes on the board: **{doc.get('n_recipes')}**. "
        f"Recipes with at least one designed_after day: "
        f"**{doc.get('n_with_designed_after')}**.",
        "",
        "`union_hot_n4_holdup` days before 2026-09-21 are designed_after. "
        "Anything created after 2026-08-13 uses that creation date the same way.",
        "",
        "The compounds chain the published session percents on the labeled days. "
        "They do not restart the cash book on the creation date, and they do not rescore.",
        "",
        "The dashboard HTML was not regenerated. This page and "
        "`03_scoreboard/factor_mine_designed_after.csv` are the separate scoreboard.",
        "",
        "| recipe | created_on | first real day | designed_after days | real days | designed_after compound | real compound |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for row in doc.get("recipes") or []:
        if not row.get("n_designed_after"):
            continue
        lines.append(
            f"| `{row['name']}` | {row['created_on']} | {row['first_real_day'] or '—'} | "
            f"{row['n_designed_after']} | {row['n_real']} | "
            f"{_fmt(row['designed_after_compound_pct'])} | {_fmt(row['real_compound_pct'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def _rules_md(doc: dict) -> str:
    stop_only = ", ".join(f"`{name}`" for name in (doc.get("stop_only_recipes") or [])) or "none"
    return "\n".join([
        "# Factor Mine rule lock",
        "",
        "Each recipe's gates, weights, hold, take-profit and exit, sizing, "
        "and fee model are hashed into `data/factor_mine/freeze_manifest.json` "
        "the first time the name is locked. The same name with a different hash "
        "fails the job. It does not rescore. Changing the rules means a new name.",
        "",
        f"Same-bar exit, written into every fingerprint: when one daily bar "
        f"touches both take-profit and stop, assume **{SAME_BAR}**.",
        "",
        f"Trades that rule can change: **{doc.get('same_bar_trades_affected')}**. "
        f"Recipes with neither a take-profit nor a stop are 0. "
        f"Recipes with a stop and no take-profit cannot double-touch, so they are 0 too"
        f"{(': ' + stop_only) if stop_only != 'none' else ''}. "
        "This count is the definition, not a rescore.",
        "",
        "A job that would change picks, fills, or P&L on a locked day fails. "
        "The first backfill of a recipe with no locked history is the exception, "
        "and those pre-creation days stay `designed_after`.",
        "",
        "See `03_scoreboard/FACTOR_MINE_DESIGNED_AFTER.md` for the split totals.",
        "",
        "`data/day_board/<date>_strategy_tickets.json` stays the send-time file. "
        "After that session's 09:30 ET, a different body does not replace it.",
        "",
    ])
