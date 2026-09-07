"""Harden leftover A–JL KEEPs: collapse twins, re-run the ship bar.

Raw full-scale mine printed 126 KEEP rows. Most are eq1/ge1/gt0 twins of
the same letter, or a fill and a value that fire the same days. This beat
keeps one survivor per letter × side × hold (hold1/hold2 first), re-applies
the ship bar on the saved blocks, and ranks a shortboard.

Research only. Live flatten_robust is not imported or changed.

  python engine/harden_unmined.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import SHIP  # noqa: E402
from harden_hyst_open import splice_md  # noqa: E402
from mine_unmined import (  # noqa: E402
    BEAT, HALF_CUT, LETTER_PLAIN, Q3_CUT, fmt_blk, letter_of,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
SWEEP_JSON = os.path.join(RESEARCH, "unmined_sweep.json")
OUT_MD = os.path.join(RESEARCH, "UNMINED_HARDEN.md")
OUT_JSON = os.path.join(RESEARCH, "unmined_harden.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
SWEEP_MD = os.path.join(RESEARCH, "UNMINED_SWEEP.md")
MARKER = "## Leftover KEEP harden (unique letters)"
FEATURED = ("T", "BA", "AH", "CZ", "EH", "IB")


def vs_pp(r):
    h, b = r.get("holdout") or {}, r.get("baseline") or {}
    if not h or not b:
        return None
    return h["avg_net"] - b["avg_net"]


def rejudge(r):
    """Re-run the ship bar on saved blocks. Does not trust the raw KEEP."""
    reasons = []
    d, h = r.get("discovery"), r.get("holdout")
    early, late = r.get("early"), r.get("late")
    up, dn = r.get("spy_up"), r.get("spy_dn")
    q12, q3 = r.get("q12"), r.get("q3")
    b = r.get("baseline")
    if not d or d.get("n", 0) < 80:
        return "THIN", ["thin_disc"]
    if d["n"] < SHIP["disc_n"]:
        reasons.append("thin_disc")
    if not h or h.get("n", 0) < SHIP["hold_n"]:
        reasons.append("thin_hold")
    if d.get("t", 0) < SHIP["disc_t"]:
        reasons.append("disc_t")
    if h and h.get("t", 0) < SHIP["hold_t"]:
        reasons.append("hold_t")
    if d.get("avg_net", 0) <= 0:
        reasons.append("disc_sign")
    if h and h.get("avg_net", 0) <= 0:
        reasons.append("hold_sign")
    if r.get("n_tickers", 0) < SHIP["n_tickers"]:
        reasons.append("ticker_bar")
    if r.get("n_dates", 0) < SHIP["n_dates"]:
        reasons.append("date_bar")
    lot = r.get("lottery_day_frac")
    if lot is None or lot > SHIP["max_trade_frac"]:
        reasons.append("lottery_day")
    if not early or not late or early.get("n", 0) < 40 or late.get("n", 0) < 40:
        reasons.append("tape_thin")
    elif late.get("avg_net", 0) <= 0 or (
            (early.get("avg_net", 0) > 0) != (late.get("avg_net", 0) > 0)):
        reasons.append("tape_split")
    if not up or not dn or up.get("n", 0) < 40 or dn.get("n", 0) < 40:
        reasons.append("spy_thin")
    elif up.get("avg_net", 0) <= 0 or dn.get("avg_net", 0) <= 0:
        reasons.append("spy_regime")
    if not q3 or q3.get("n", 0) < 40:
        reasons.append("q3_missing")
    elif q3.get("avg_net", 0) <= 0:
        reasons.append("q3_sign")
    if q12 and q3 and q12.get("n", 0) >= 40 and q3.get("n", 0) >= 40:
        if (q12.get("avg_net", 0) > 0) != (q3.get("avg_net", 0) > 0):
            reasons.append("q3_split")
    if b and d and ((d["avg_net"] < b.get("avg_net", 0) + BEAT) or
                    (h and h["avg_net"] < b.get("avg_net", 0) + BEAT)):
        reasons.append("no_edge_vs_uncond")
    if r.get("n_tickers", 0) < 50 or d["n"] < 80:
        return "THIN", reasons or ["thin"]
    if reasons:
        return "KILL", reasons
    return "KEEP", []


def pick_survivor(rows):
    """One row: best holdout t, then vs-everyone, value/flag over fill."""
    def key(r):
        h = r.get("holdout") or {}
        fam = 0 if r.get("family") in ("val_open", "val_close", "flag") else 1
        return ((h.get("t") or -9), (vs_pp(r) or -9), -fam)
    return max(rows, key=key)


def collapse(keepers):
    """letter × side × hold → survivor + killed twins."""
    groups = defaultdict(list)
    for r in keepers:
        groups[(letter_of(r["def"]), r["side"], r["exit"])].append(r)
    survivors, killed = [], []
    for key, rows in groups.items():
        win = pick_survivor(rows)
        twins = [x for x in rows if x["def"] != win["def"]
                 or x["exit"] != win["exit"]]
        # same-day twins: identical disc+hold n and avg
        dn, hn = (win.get("discovery") or {}).get("n"), (win.get("holdout") or {}).get("n")
        da, ha = (win.get("discovery") or {}).get("avg_net"), (win.get("holdout") or {}).get("avg_net")
        for x in twins:
            xd, xh = x.get("discovery") or {}, x.get("holdout") or {}
            same = (xd.get("n") == dn and xh.get("n") == hn
                    and abs((xd.get("avg_net") or 0) - (da or 0)) < 1e-6
                    and abs((xh.get("avg_net") or 0) - (ha or 0)) < 1e-6)
            killed.append({
                "def": x["def"], "letter": key[0], "side": key[1],
                "exit": key[2], "family": x.get("family"),
                "keep": "KILL",
                "fail_reasons": ["twin_same_days" if same else "twin_weaker"],
                "twin_of": win["def"],
                "discovery": xd, "holdout": xh,
            })
        survivors.append(win)
    return survivors, killed


def harden(survivors):
    """Re-run ship bar; hold1/2 first; hold5 is not a sleeve survivor."""
    out = []
    for r in survivors:
        verdict, reasons = rejudge(r)
        rec = dict(r)
        rec["letter"] = letter_of(r["def"])
        rec["plain"] = LETTER_PLAIN.get(rec["letter"], rec["letter"])
        rec["vs_uncond_pp"] = vs_pp(r)
        rec["fail_reasons"] = list(reasons)
        rec["raw_keep"] = r.get("keep") or r.get("verdict")
        if rec["exit"] == "hold5" and verdict == "KEEP":
            verdict = "KILL"
            rec["fail_reasons"] = rec["fail_reasons"] + ["hold5_not_primary"]
        rec["keep"] = verdict
        rec["verdict"] = "PASS" if verdict == "KEEP" else "FAIL" if verdict == "KILL" else "THIN"
        rec["live_untouched"] = "flatten_robust"
        out.append(rec)
    by = {(r["letter"], r["side"], r["exit"]): r for r in out}
    for r in out:
        if r["keep"] != "KEEP" or r["exit"] != "hold1":
            continue
        sib = by.get((r["letter"], r["side"], "hold2"))
        if not sib or sib.get("keep") != "KEEP":
            r["keep"] = "KILL"
            r["verdict"] = "FAIL"
            r["fail_reasons"] = list(r["fail_reasons"]) + ["hold1_without_hold2_keep"]
    out.sort(key=lambda r: (
        0 if r["keep"] == "KEEP" else 1,
        0 if r["exit"] == "hold2" else 1 if r["exit"] == "hold1" else 2,
        -(r.get("vs_uncond_pp") or -9),
    ))
    return out


def render(survivors, killed, n_raw, n_tickers):
    keeps = [r for r in survivors if r["keep"] == "KEEP"]
    kills = [r for r in survivors if r["keep"] == "KILL"]
    thins = [r for r in survivors if r["keep"] == "THIN"]
    sleeve = [r for r in keeps if r["exit"] in ("hold1", "hold2")]
    hold2 = [r for r in sleeve if r["exit"] == "hold2"]
    letters = sorted({r["letter"] for r in keeps})
    featured = [r for r in hold2 if r["letter"] in FEATURED]
    featured.sort(key=lambda r: FEATURED.index(r["letter"])
                  if r["letter"] in FEATURED else 99)
    peers = [r for r in hold2 if r["letter"] not in FEATURED]
    L = [
        "# Leftover KEEP harden — unique letters",
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge._",
        "",
        "## Plain English",
        "",
        "The full 3,603-name dump printed **126** leftover KEEP rows. "
        "Most of those are the same letter written three ways "
        "(equals 1 / at least 1 / greater than 0), or a green highlight "
        "and a number that turn on the same days. We keep **one** "
        "survivor per letter, side, and hold, prefer the next-day and "
        "same-day holds, then re-check the ship bar: Futubull fees, "
        "both ticker halves, both calendar halves, **Q3 (2026-07-01)**, "
        "both SPY tapes, fattest day under 25% of winning-day P&L, and "
        "at least 20 bps better than buying everyone.",
        "",
        "Five-session holds that only looked good because the tape was "
        "up are dropped. Twin defs are **KILL**. Light+green O on the "
        "A–O strip is unchanged. No card.",
        "",
        f"Raw KEEP **{n_raw}** → unique letter×side×hold **{len(survivors)}** "
        f"→ after ship bar **KEEP {len(keeps)}** · **KILL {len(kills)}** · "
        f"**THIN {len(thins)}**. Sleeve hold1/2 KEEP **{len(sleeve)}** "
        f"on letters `{'/'.join(letters) or '—'}`. "
        f"Twins killed **{len(killed)}**. N={n_tickers}.",
        "",
        "### Shortboard (hold2 survivors)",
        "",
        "Ranked by how much they beat 'buy everyone' after fees. "
        "Featured letters first (T, BA, AH, CZ, EH, IB), then peers.",
        "",
        "| rank | letter | what it is | clock | holdout | vs everyone | "
        "Q3 | day-lottery | tickers | verdict |",
        "|---:|---|---|---|---|---|---|---|---:|---|",
    ]
    rank = 0
    for r in featured + peers:
        rank += 1
        vs = r.get("vs_uncond_pp")
        vs_s = f"{vs*100:+.2f} pp" if vs is not None else "—"
        L.append(
            f"| {rank} | **{r['letter']}** | {r['plain']} | {r['clock']} | "
            f"{fmt_blk(r.get('holdout'))} | {vs_s} | "
            f"{fmt_blk(r.get('q3'))} | "
            f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | "
            f"{r.get('n_tickers')} | **{r['keep']}** |"
        )
    L += [
        "",
        "### Unique-letter KEEP list (hold1/2)",
        "",
        "| letter | hold | clock | side | what it is | disc | holdout | "
        "Q3 | vs everyone | day-lottery | survivor def |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in sleeve:
        vs = r.get("vs_uncond_pp")
        vs_s = f"{vs*100:+.2f} pp" if vs is not None else "—"
        L.append(
            f"| **{r['letter']}** | {r['exit']} | {r['clock']} | {r['side']} | "
            f"{r['plain']} | {fmt_blk(r.get('discovery'))} | "
            f"{fmt_blk(r.get('holdout'))} | {fmt_blk(r.get('q3'))} | "
            f"{vs_s} | {(r.get('lottery_day_frac') or 0)*100:.1f}% | "
            f"`{r['def']}` |"
        )
    if not sleeve:
        L.append("| — | — | — | — | none cleared | — | — | — | — | — | — |")
    L += [
        "",
        "### Killed twins",
        "",
        "Same letter, same days (or a weaker fill/value of the same "
        "letter). Not a second edge.",
        "",
        "| killed def | letter | hold | why | survivor |",
        "|---|---|---|---|---|",
    ]
    for t in sorted(killed, key=lambda x: (x["letter"], x["exit"], x["def"])):
        L.append(
            f"| `{t['def']}` | {t['letter']} | {t['exit']} | "
            f"{','.join(t['fail_reasons'])} | `{t['twin_of']}` |"
        )
    L += [
        "",
        "### Survivors that died on re-judge",
        "",
        "| letter | hold | def | verdict | why |",
        "|---|---|---|---|---|",
    ]
    dead = [r for r in survivors if r["keep"] != "KEEP"]
    if not dead:
        L.append("| — | — | — | — | none |")
    for r in dead:
        L.append(
            f"| {r['letter']} | {r['exit']} | `{r['def']}` | **{r['keep']}** | "
            f"{','.join(r['fail_reasons']) or '—'} |"
        )
    L += [
        "",
        "### What this does not change",
        "",
        "- Standing A–O research keeps stay the **three light + green O** "
        "recipes. This table does not touch them.",
        "- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.",
        "- Live `flatten_robust` is not imported or changed. No cards.",
        f"- Calendar half cut **{HALF_CUT}**. Q3 cut **{Q3_CUT}**. "
        "Futubull 0.15% long / 0.20% short.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def slim(r):
    keys = (
        "def", "letter", "plain", "family", "clock", "side", "exit",
        "n_tickers", "n_dates", "discovery", "holdout", "early", "late",
        "spy_up", "spy_dn", "q12", "q3", "baseline", "vs_uncond_pp",
        "lottery_day_frac", "keep", "verdict", "fail_reasons",
        "raw_keep", "live_untouched",
    )
    return {k: r[k] for k in keys if k in r}


def main():
    os.chdir(ROOT)
    raw = json.load(open(SWEEP_JSON))
    keepers = list(raw.get("keepers") or [])
    if not keepers:
        raise SystemExit("no keepers in unmined_sweep.json")
    survivors, killed = collapse(keepers)
    scored = harden(survivors)
    n_tickers = raw.get("n_tickers") or 3603
    md = render(scored, killed, len(keepers), n_tickers)
    keeps = [r for r in scored if r["keep"] == "KEEP"]
    payload = {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "n_tickers": n_tickers,
        "n_raw_keep": len(keepers),
        "n_unique": len(survivors),
        "n_twins_killed": len(killed),
        "n_keep": len(keeps),
        "n_kill": sum(1 for r in scored if r["keep"] == "KILL"),
        "n_thin": sum(1 for r in scored if r["keep"] == "THIN"),
        "n_keep_hold12": sum(1 for r in keeps if r["exit"] in ("hold1", "hold2")),
        "keep_letters": sorted({r["letter"] for r in keeps}),
        "featured": list(FEATURED),
        "q3_cut": Q3_CUT,
        "half_cut": HALF_CUT,
        "standing_ao_keeps": "light+green O unchanged",
        "finviz": "BLOCKED",
        "survivors": [slim(r) for r in scored],
        "twins_killed": killed,
        "scale": "full_rows_cache",
    }
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = MARKER + "\n\n" + md
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER,
        MARKER + "\n\n"
        f"Leftover KEEP harden: raw {payload['n_raw_keep']} → unique "
        f"{payload['n_unique']} → KEEP {payload['n_keep']} "
        f"({payload['n_keep_hold12']} hold1/2). Twins killed "
        f"{payload['n_twins_killed']}. Letters: "
        f"{', '.join(payload['keep_letters']) or 'none'}. "
        "Light+O unchanged. Finviz BLOCKED. See `UNMINED_HARDEN.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    sw = splice_md(SWEEP_MD, MARKER, block, require="Plain English")
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    open(SWEEP_MD, "w", encoding="utf-8").write(sw)
    print(f"KEEP {payload['n_keep']} unique={payload['n_unique']} "
          f"twins={payload['n_twins_killed']} letters={payload['keep_letters']}",
          flush=True)
    return payload


if __name__ == "__main__":
    main()
