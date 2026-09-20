"""Blind ≤2026-09-09 factor-mine FORMATION remine WITH Clock-B / oppset.

Same method as ``src.factor_mine_blind`` (#286) — leak-free panel,
default auto + auto-tweak, combo-engine primitives, Cyrus Starts YES
+ Book% featuring, OOS 9/10–9/18 never ranks — but the 9/9 toolbox
**includes** Theme Radar Clock-B / oppset aisle primitives.

Freeze contract
---------------
* 9/9 grid + combo specs: ``cb7f09ae`` (2026-09-09, combo books #175).
* Clock-B / oppset *builders* first landed ``e3aabf24`` (2026-09-19,
  #279). They did **not** exist as named recipes on 9/9 close.
* Features those builders read *were* T−1-clean as of 9/9: Theme Radar
  ``finviz_asof < join_morning``; IS ``join_morning ≤ 2026-09-09`` uses
  ``asof ≤ 2026-09-08`` only. Clock-B atoms are prior tape / morning
  packet / prior Finviz export (see ``clock_b_tells.LEAK_FIELDS``).

Still stripped (live-board contamination that did not exist on 9/9):
``union_hot_n4_holdup`` / ``s_boost=holdup``, overnight_mega,
WORKABLE_ALWAYS / FOCUS live pins as *seeds*. Clock-B / oppset names
may be featured only if the ≤9/9 grid rediscovers them.

Aisle: panel ∪ T−1 oppset (``FULLSCAN_OPPSET_UNION`` / top-30 by
``opp_rvol``), same spirit as morning_scan / remine oppset path.
OOS ``join_morning`` rows never enter ranking.

CLI::

    FULLSCAN_OPPSET_UNION=1 python -m src.factor_mine --pull-oppset \\
      --from-date 2026-08-13 --to-date 2026-09-09 \\
      --write --auto-tweak --holdout --blind-0909-oppset \\
      --out-root 03_scoreboard/factor_mine_blind_0909_oppset \\
      --dash-dir dashboard/factor-mine-blind-0909-oppset \\
      --asof-md 03_scoreboard/FACTOR_MINE_BLIND_0909_OPPSET.md
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from . import clock_b_tells as cbt
from . import factor_mine as fm
from . import factor_mine_asof as fma
from . import factor_mine_blind as fmbld
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import gainer_asof as ga
from . import morning_scan as ms
from . import oppset_clock_b as opp
from . import ticker_lookback as tl

CUTOFF = fmbld.CUTOFF
OOS_START = fmbld.OOS_START
FROM_DATE = fmbld.FROM_DATE
GRID_FREEZE_SHA = fmbld.RECIPE_FREEZE_SHA  # cb7f09ae
CLOCK_B_BUILDER_SHA = "e3aabf24"
CLOCK_B_BUILDER_NOTE = (
    "Clock-B / oppset recipe builders first landed e3aabf24 "
    "(2026-09-19, #279). Named union_clk_* / oppset_* sleeves were "
    "not in the 9/9 close menu; the atoms they stamp (prior tape, "
    "morning packet, prior Finviz, Theme Radar T−1 gap+RelVol) were "
    "T−1-clean as of 2026-09-09."
)
RECIPE_FREEZE_SHA = f"{GRID_FREEZE_SHA}+{CLOCK_B_BUILDER_SHA}"
RECIPE_FREEZE_NOTE = (
    f"9/9 grid + combo-spec definitions frozen to {GRID_FREEZE_SHA} "
    f"(2026-09-09, Factor-mine combo books #175). Clock-B / oppset "
    f"catalogue builders spliced from {CLOCK_B_BUILDER_SHA} (first "
    f"commit that had them). Current cash-book / mark / fee engine "
    f"scores those frozen definitions. holdup / overnight_mega / "
    f"WORKABLE_ALWAYS / FOCUS live pins stay stripped."
)

POST_0909_CONTAM = fmbld.POST_0909_NAMES
CLOCK_B_MENU = cbt.CLOCK_B_RECIPES + cbt.CLOCK_B_OPPSET_RECIPES
CLOCK_B_REPORT = cbt.CLOCK_B_CORE + cbt.CLOCK_B_OPPSET_RECIPES
OPPSET_TOP_N = 30

DEFAULT_OUT_ROOT = "03_scoreboard/factor_mine_blind_0909_oppset"
DEFAULT_DASH = "dashboard/factor-mine-blind-0909-oppset"
DEFAULT_MD = "03_scoreboard/FACTOR_MINE_BLIND_0909_OPPSET.md"


def enable_oppset_union() -> None:
    os.environ["FULLSCAN_OPPSET_UNION"] = "1"


def ensure_oppset_csv(*, pull: bool = False) -> Path:
    """Pull Theme Radar CSV if asked or missing. No git clone."""
    have = opp.discover_csv()
    if pull or have is None:
        dest = opp.pull()
        opp.reset_index()
        return dest
    return have


def filter_index(index: dict, *, join_morning_max: str | None = None,
                 join_morning_min: str | None = None) -> dict:
    """Keep T−1 rows inside a join_morning window. Ranking uses IS only."""
    out = {}
    for key, row in (index or {}).items():
        morning = key[0] if isinstance(key, tuple) else str(row.get("join_morning") or "")[:10]
        if join_morning_min and morning < join_morning_min:
            continue
        if join_morning_max and morning > join_morning_max:
            continue
        out[key] = row
    return out


def t1_proof(index: dict, *, is_max: str = CUTOFF) -> dict:
    """Theme Radar contract: finviz_asof < join_morning; IS asof ≤ 09-08."""
    n = 0
    leaks = 0
    is_n = 0
    is_asofs: list[str] = []
    mornings: set[str] = set()
    for (morning, _ticker), row in (index or {}).items():
        n += 1
        mornings.add(morning)
        asof = str(row.get("finviz_asof") or "")[:10]
        if asof and asof >= morning:
            leaks += 1
        if morning <= is_max:
            is_n += 1
            if asof:
                is_asofs.append(asof)
    is_asof_max = max(is_asofs) if is_asofs else None
    is_asof_min = min(is_asofs) if is_asofs else None
    return {
        "n": n,
        "n_mornings": len(mornings),
        "leaks_asof_ge_morning": leaks,
        "is_join_morning_max": is_max,
        "is_n": is_n,
        "is_asof_min": is_asof_min,
        "is_asof_max": is_asof_max,
        "is_asof_cap": "2026-09-08",
        "is_asof_ok": (is_asof_max is None) or (is_asof_max <= "2026-09-08"),
        "t1_clean": leaks == 0,
    }


def union_oppset_on_panel(panel: dict, *, top_n: int = OPPSET_TOP_N,
                          index: dict | None = None,
                          persist: bool = False) -> dict:
    """In-memory panel ∪ T−1 oppset (top_n by rvol). Never writes live panel.

    Existing rows are stamped. Missing top-n flagged names get a full
    ``_attach_row`` (same spirit as ``FULLSCAN_OPPSET_UNION`` remine /
    overnight stamp). Slim T−1 membership is the fallback.
    """
    if persist:
        raise RuntimeError("refusing to persist oppset union onto the live panel")
    panel = fm.rehydrate_panel(dict(panel or {}))
    idx = index if index is not None else opp.load_index()
    opp.attach_panel(panel, idx)
    cbt.attach_panel(panel)
    cal = list(panel.get("session_dates") or [])
    if not cal:
        panel["oppset_union"] = {"tagged": 0, "added": 0, "slim": 0, "top_n": top_n}
        return panel
    full_cal = fm.panel_lookback_calendar(
        panel.get("from_date") or cal[0], panel.get("to_date") or cal[-1])
    if not full_cal:
        full_cal = list(cal)
    end = panel.get("to_date") or cal[-1]
    sess_map, _ = fm._session_map(full_cal[0], end)
    by_date = panel.get("by_date") or {}
    rows = list(panel.get("rows") or [])
    tagged = added = slim = 0
    for date in cal:
        prior = fm.feature_export_date(full_cal, date)
        try:
            prior_df = ga.load_finviz(prior) if prior else None
        except Exception:
            prior_df = None
        existing = {str(r.get("ticker") or "").upper(): r
                    for r in (by_date.get(date) or [])}
        extra = [t for t in opp.flagged_tickers(date, top_n=top_n, index=idx)
                 if t and t not in existing]
        sess = sess_map.get(date)
        prev = sess_map.get(prior) if prior else None
        for i, t in enumerate(extra):
            if sess is None:
                hit = opp.lookup(date, t, idx) or {"ticker": t}
                rec = ms._slim_oppset_row(date, {**hit, "ticker": t})
                slim += 1
            else:
                try:
                    rec = fm._attach_row(
                        date, t, ["oppset"], 50 + i, sess, prev, prior, prior_df)
                except (Exception, SystemExit):
                    hit = opp.lookup(date, t, idx) or {"ticker": t}
                    rec = ms._slim_oppset_row(date, {**hit, "ticker": t})
                    slim += 1
            rec.setdefault("sources", ["oppset"])
            if "oppset" not in (rec.get("sources") or []):
                rec["sources"] = list(rec.get("sources") or []) + ["oppset"]
            opp.stamp_row(rec, idx)
            if not rec.get("_clock_b"):
                cbt.stamp_row(rec)
            rows.append(rec)
            by_date.setdefault(date, []).append(rec)
            existing[t] = rec
            added += 1
        tagged += sum(1 for r in (by_date.get(date) or []) if fm.on_oppset(r))
        print(f"[blind-oppset] aisle {date} extra={len(extra)} "
              f"oppset_on_day={sum(1 for r in (by_date.get(date) or []) if fm.on_oppset(r))}",
              flush=True)
    rows.sort(key=lambda r: (
        r.get("date") or "", int(r.get("src_rank") or 0), r.get("ticker") or "",
    ))
    panel["rows"] = rows
    panel["by_date"] = by_date
    panel["n_rows"] = len(rows)
    panel["oppset_union"] = {
        "tagged": tagged, "added": added, "slim": slim, "top_n": top_n,
        "persist": False, "aisle": ms.AISLE,
    }
    panel["_oppset"] = True
    panel["_clock_b"] = True
    return panel


def build_recipes_asof_0909_oppset() -> list[dict]:
    """9/9 close menu + Clock-B / oppset catalogue (no holdup / overnight)."""
    recs = list(fmbld.build_recipes_asof_0909())
    have = {r["name"] for r in recs}
    for rec in cbt.clock_b_recipes(fm.make_recipe):
        if rec["name"] not in have:
            recs.append(rec)
            have.add(rec["name"])
    return recs


def blind_recipes(*, universe="auto", hold="auto", gate="auto",
                  rank="auto", side="auto", top_n="auto", exit="auto",
                  entry="auto", size="auto", sell="auto", s_boost="auto",
                  auto_tweak=True) -> list[dict]:
    """Standing auto + auto-tweak over 9/9 + Clock-B/oppset. Holdup out."""
    recs = fmb.recipes_from_action(
        universe=universe, hold=hold, gate=gate, rank=rank, side=side,
        top_n=top_n, exit=exit, entry=entry, size=size, sell=sell,
        s_boost=s_boost, auto_tweak=auto_tweak,
        base=build_recipes_asof_0909_oppset(),
    )
    banned = set(POST_0909_CONTAM)
    out = []
    for r in recs:
        name = r.get("name") or ""
        if name in banned:
            continue
        if (r.get("s_boost") or "none") == "holdup":
            continue
        if (r.get("universe") or "") in ("overnight_mega", "overnight"):
            continue
        out.append(r)
    return out


def combo_specs_asof_0909_oppset() -> list[dict]:
    """9/9 combo engine + Clock-B/oppset singles mixed with the same primitives."""
    specs = list(fmbld.combo_specs_asof_0909())
    seen = {s["name"] for s in specs}
    seen_members = {tuple(s.get("members") or []) for s in specs}
    S, E, H = "short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"

    def add(name, members, weights, *, net="priority", pool="shared"):
        if name in seen:
            return
        key = tuple(members)
        if key in seen_members and list(weights) == [1, 1]:
            return
        seen.add(name)
        seen_members.add(key)
        specs.append({
            "name": name,
            "members": list(members),
            "weights": [float(w) for w in weights],
            "net": net,
            "pool": pool,
            "formed": False,
            "clock_b": True,
        })

    longs = [
        n for n in CLOCK_B_MENU
        if not str(n).startswith("short_")
    ]
    shorts = [n for n in CLOCK_B_MENU if str(n).startswith("short_")]

    def tag_of(name: str) -> str:
        """Stable unique tag — _abbr collides on oppset_h1 / union_oppset_h1."""
        raw = str(name)
        if raw == "union_oppset_h1":
            return "uopp"
        if raw == "oppset_h1":
            return "opp"
        if raw.startswith("union_clk_"):
            return "c" + raw.replace("union_clk_", "").replace("_", "")[:8]
        if raw.startswith("short_clk_"):
            return "sc" + raw.replace("short_clk_", "").replace("_", "")[:8]
        return fmbld._abbr(raw)

    for clk in longs:
        tag = tag_of(clk)
        add(f"combo_s{tag}_5050_shared", [S, clk], [1, 1])
        add(f"combo_e{tag}_5050_shared", [E, clk], [1, 1])
    for clk in shorts:
        tag = tag_of(clk)
        add(f"combo_{tag}h_5050_shared", [clk, H], [1, 1])
        add(f"combo_{tag}e_5050_shared", [clk, E], [1, 1])
    for clk in ("union_clk_mom_break_peer_h1", "union_clk_fresh_cat_coil_h1",
                "union_oppset_h1", "oppset_h1"):
        tag = tag_of(clk)
        add(f"combo_se{tag}_333_shared", [S, E, clk], [1, 1, 1])
    return specs


def clock_b_twins(payload: dict) -> dict[str, list[str]]:
    """Named Clock-B / oppset sleeves present in the scored menu."""
    names = {r.get("name") for r in (payload.get("recipes") or []) if r.get("name")}
    return {
        "core": [n for n in cbt.CLOCK_B_CORE if n in names],
        "oppset": [n for n in cbt.CLOCK_B_OPPSET_RECIPES if n in names],
        "catalogue": [n for n in cbt.CLOCK_B_RECIPES if n in names],
    }


def freeze_names(payload: dict) -> list[str]:
    """IS freeze: Cyrus ∪ formal KEEP ∪ hot4 / Clock-B contamination checks."""
    names = list(fmbld.freeze_names(payload))
    seen = set(names)
    scored = {s.get("name") for s in (payload.get("stats") or []) if s.get("name")}
    for n in CLOCK_B_REPORT:
        if n in scored and n not in seen:
            names.append(n)
            seen.add(n)
    return names


def apply_blind_featured(payload: dict, *, aisle: dict | None = None) -> dict:
    featured = fmbld.cyrus_names(payload.get("stats") or [])
    payload["featured"] = featured
    twins = clock_b_twins(payload)
    payload["blind"] = {
        "cutoff": CUTOFF,
        "kind": "blind_formation_0909_oppset",
        "recipe_freeze": RECIPE_FREEZE_SHA,
        "recipe_freeze_note": RECIPE_FREEZE_NOTE,
        "grid_freeze": GRID_FREEZE_SHA,
        "clock_b_builder": CLOCK_B_BUILDER_SHA,
        "clock_b_builder_note": CLOCK_B_BUILDER_NOTE,
        "excluded_seeds": list(POST_0909_CONTAM) + [
            "WORKABLE_ALWAYS",
            "FOCUS / LONG_LED_PIN live featured pins",
            "s_boost=holdup (landed 2026-09-19)",
        ],
        "included_toolbox": [
            "Clock-B catalogue 1–10 (T−1 atoms)",
            "Theme Radar oppset stamp + union top-30",
            "CLOCK_B_CORE + CLOCK_B_OPPSET_RECIPES",
        ],
        "cyrus_featured": featured,
        "formal_keep": fmbld.formal_keep_names(payload.get("stats") or []),
        "cyrus_rule": (
            f"Starts YES ≥{fmbld.CYRUS_START_YES}/{fmbld.CYRUS_START_N_REF} "
            f"(or ≥{fmbld.CYRUS_START_RATE:.0%} when start_n < {fmbld.CYRUS_START_N_REF}), "
            f"Book% > {fmbld.CYRUS_MIN_BOOK:g}, n ≥ {fmbld.CYRUS_MIN_TRADES}. "
            "No ALWAYS / FOCUS pins."
        ),
        "hot4_twins": fmbld.twins_of(payload, fmbld.HOT4_SIG),
        "holdup_twins": fmbld.twins_of(payload, fmbld.HOLDUP_SIG),
        "clock_b_twins": twins,
        "aisle": aisle or {},
    }
    return payload


def merge_formed_combos(payload: dict, combo_stats: list[dict],
                        combo_books: dict, *, aisle: dict | None = None) -> dict:
    fmc.merge_into_payload(payload, combo_stats, combo_books)
    return apply_blind_featured(payload, aisle=aisle)


def _md_pct(v) -> str:
    return fmbld._md_pct(v)


def _md_pp(v) -> str:
    return fmbld._md_pp(v)


def _load_json(path: Path) -> dict | None:
    if not path or not Path(path).is_file():
        return None
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def render_blind_md(payload: dict, rows: list[dict], *,
                    cutoff: str, oos_start: str, oos_end: str) -> str:
    blind = payload.get("blind") or {}
    n_mined = int(payload.get("n_mined") or payload.get("n_recipes") or 0)
    n_singles = int(payload.get("n_singles") or 0)
    n_combos = int((payload.get("combos") or {}).get("n") or 0)
    n_formed = int(payload.get("n_formed_combos") or 0)
    n_clk_specs = int((payload.get("combos") or {}).get("clock_b_n") or 0)
    bar = fm.WORKABLE_BAR
    cyrus = set(blind.get("cyrus_featured") or [])
    formal = set(blind.get("formal_keep") or [])
    hot_twins = blind.get("hot4_twins") or []
    hold_twins = blind.get("holdup_twins") or []
    clk_twins = blind.get("clock_b_twins") or {}
    aisle = blind.get("aisle") or {}
    proof = aisle.get("t1_proof") or {}
    by = {r["name"]: r for r in rows}
    rec_by = {r.get("name"): r for r in (payload.get("recipes") or []) if r.get("name")}

    lines = [
        f"# Factor mine blind formation — as-of {cutoff} WITH Clock-B / oppset",
        "",
        "This is a **blind formation remine** of the 9/9 method "
        "**including** Theme Radar Clock-B / oppset. It is not a "
        "KEEP-selection cut of the live menu ([PR #285](https://github.com/SRoyaltyy/fullscan/pull/285)) "
        "and it is not the no-oppset remine "
        "([PR #286](https://github.com/SRoyaltyy/fullscan/pull/286)).",
        "",
        f"In-sample formation: **{payload.get('from_date')} → {cutoff}** "
        f"({payload.get('n_sessions')} sessions, {payload.get('n_rows')} rows). "
        f"Out-of-sample (frozen discoveries only): **{oos_start} → {oos_end}**.",
        "",
        "## Freeze contract",
        "",
        f"- **Panel / IS window:** `{FROM_DATE}` → `{cutoff}` only. "
        f"Oppset filter: `join_morning <= {cutoff}`. "
        "No 9/10–9/18 row entered ranking, tweaking, or featuring.",
        f"- **9/9 grid freeze:** `{GRID_FREEZE_SHA}` (2026-09-09 close, #175).",
        f"- **Clock-B / oppset builder freeze:** `{CLOCK_B_BUILDER_SHA}` "
        f"— {CLOCK_B_BUILDER_NOTE}",
        f"- **Combined freeze id:** `{RECIPE_FREEZE_SHA}`.",
        "- **Still stripped:** `union_hot_n4_holdup` / `s_boost=holdup` "
        "(landed 9/19), overnight_mega, WORKABLE_ALWAYS extras, "
        "FOCUS / LONG_LED_PIN live featured pins as *seeds*.",
        "- **Included toolbox:** Clock-B catalogue flags (T−1 atoms) + "
        "Theme Radar oppset stamp + `FULLSCAN_OPPSET_UNION` top-30 aisle "
        "+ `CLOCK_B_CORE` / `CLOCK_B_OPPSET_RECIPES`.",
        f"- **Holdup primitive:** still absent. Twins: "
        + (", ".join(f"`{n}`" for n in hold_twins) or "none") + ".",
        "",
        "## T−1 / aisle proof",
        "",
        f"- Theme Radar leaks (`finviz_asof >= join_morning`): "
        f"**{proof.get('leaks_asof_ge_morning', '—')}** "
        f"({'clean' if proof.get('t1_clean') else 'CHECK'}).",
        f"- IS oppset rows: **{proof.get('is_n', '—')}**. "
        f"asof min `{proof.get('is_asof_min') or '—'}` · "
        f"max `{proof.get('is_asof_max') or '—'}` "
        f"(cap `{proof.get('is_asof_cap')}`; "
        f"{'OK' if proof.get('is_asof_ok') else 'FAIL'} — IS uses asof≤09-08 only).",
        f"- Aisle adds (full panel, not persisted): tagged "
        f"{(aisle.get('union') or {}).get('tagged', '—')}, added "
        f"{(aisle.get('union') or {}).get('added', '—')}, slim fallback "
        f"{(aisle.get('union') or {}).get('slim', '—')}, top_n="
        f"{(aisle.get('union') or {}).get('top_n', OPPSET_TOP_N)}.",
        f"- IS slice rows after union: **{aisle.get('is_n_rows') or payload.get('n_rows')}** "
        f"(#286 no-oppset was 1668).",
        "- Live `data/factor_mine/panel.json` was not rewritten.",
        "",
        "## Method (same as the current board, knowledge-capped, + aisle)",
        "",
        "1. Leak-free 09:30 panel ∪ T−1 oppset (top 30 by `opp_rvol`) / "
        "$10k cash books (current engine). Clock-B flags stamped from "
        "T−1 features only.",
        "2. Default **auto** slice + **auto-tweak** on the frozen 9/9 "
        f"menu **plus** Clock-B / oppset catalogue (**{n_singles or '—'}** singles).",
        "3. Combo construction: 9/9 combo-engine specs plus Clock-B/oppset "
        f"× S/E/H 50/50 / 333 mixes (**{n_clk_specs}** Clock-B specs) plus "
        f"extras **formed from IS singles** (**{n_formed}**). "
        f"Total scored: **{n_mined}**.",
        "4. Rank / KEEP from IS only. Cyrus would-have-featured: "
        f"{blind.get('cyrus_rule') or 'Starts YES + Book% > 0 + n ≥ 30'} "
        "Formal WORKABLE_BAR is reported beside it and does **not** pin "
        "FOCUS / WORKABLE_ALWAYS / hot4-holdup.",
        "5. Frozen discoveries replayed OOS continued + fresh $10k. "
        "OOS `join_morning` never added a name.",
        "",
        "Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` "
        "were not written.",
        "",
        f"Formal WORKABLE_BAR (reported, not a live-board pin): "
        f"min_trades {bar['min_trades']}, min_win {bar['min_win']}, "
        f"min_book_pct {bar['min_book_pct']}, min_start {bar['min_start']}, "
        f"min_dollar_days {bar['min_dollar_days']}.",
        "",
        "## Blindly formed keepers (IS) + OOS book",
        "",
        "| Strategy | Side | Cyrus | Formal bar | "
        "IS start | IS book% | IS win% | IS n | "
        "OOS cont book% | Fresh $10k | Members / note |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    formed_notes = []
    clk_set = set(CLOCK_B_MENU)
    for r in rows:
        name = r["name"]
        rec = rec_by.get(name) or {}
        members = list(rec.get("members") or [])
        note = ""
        if str(name).startswith("combo_form_"):
            note = "formed · " + " + ".join(f"`{m}`" for m in members)
            formed_notes.append((name, members))
        elif members:
            note = " + ".join(f"`{m}`" for m in members)
        elif name == "union_hot_n4_h1":
            note = "9/9 grid point (union / h1 / n4 / hot_score); contamination check"
        elif name in clk_set:
            note = "Clock-B / oppset catalogue (T−1 toolbox)"
        elif name == "short_news_r_h3":
            note = "9/9 short grid (news🔴 hold 3)"
        cys = "YES" if name in cyrus else "no"
        frm = "YES" if name in formal else "no"
        start = "—"
        if r.get("is_start_n"):
            start = f"{r.get('is_start_green') or 0}/{r.get('is_start_n')}"
        lines.append(
            f"| `{name}` | {r.get('side') or '—'} | {cys} | {frm} | "
            f"{start} | {_md_pp(r.get('is_book_pct'))} | "
            f"{_md_pct(r.get('is_win_rate'))} | {r.get('is_n_trades') or 0} | "
            f"{_md_pp(r.get('oos_book_pct_continued'))} | "
            f"{_md_pp(r.get('fresh_book_pct'))} | {note} |"
        )

    if formed_notes:
        lines += [
            "",
            "Formed extras (IS singles mixed with combo-engine primitives; "
            "not live FOCUS pins):",
            "",
        ]
        for name, members in formed_notes:
            lines.append(
                f"- `{name}` = " + " + ".join(f"`{m}`" for m in members)
            )

    hot = by.get("union_hot_n4_h1") or {}
    cyrus_surv = [
        r for r in rows
        if r["name"] in cyrus
        and r.get("oos_book_pct_continued") is not None
        and float(r["oos_book_pct_continued"]) > 0
    ]
    cyrus_fade = [
        r for r in rows
        if r["name"] in cyrus
        and (r.get("oos_book_pct_continued") is None
             or float(r["oos_book_pct_continued"]) <= 0)
    ]
    clk_featured = [n for n in (blind.get("cyrus_featured") or []) if n in clk_set
                    or any(m in clk_set for m in (rec_by.get(n) or {}).get("members") or [])]
    clk_surv = [r for r in cyrus_surv if r["name"] in clk_featured
                or any(m in clk_set for m in (rec_by.get(r["name"]) or {}).get("members") or [])]

    def _names(xs):
        if xs and isinstance(xs[0], dict):
            return ", ".join(f"`{r['name']}`" for r in xs) or "none"
        return ", ".join(f"`{n}`" for n in xs) or "none"

    hot_cyrus = "union_hot_n4_h1" in cyrus
    hot_formal = "union_hot_n4_h1" in formal
    if hot_cyrus:
        hot_ans = (
            "Yes — featured on IS "
            f"(Starts {hot.get('is_start_green') or 0}/{hot.get('is_start_n') or 0}, "
            f"book {_md_pp(hot.get('is_book_pct'))}%)."
        )
    elif hot and hot_formal:
        hot_ans = (
            "Partly — in the 9/9 menu and formal WORKABLE_BAR, misses Cyrus "
            f"(Starts {hot.get('is_start_green') or 0}/{hot.get('is_start_n') or 0})."
        )
    elif hot:
        hot_ans = (
            "No — 9/9 grid point, failed featuring "
            f"(Starts {hot.get('is_start_green') or 0}/{hot.get('is_start_n') or 0}, "
            f"win {_md_pct(hot.get('is_win_rate'))}, book "
            f"{_md_pp(hot.get('is_book_pct'))}%). Twins: "
            + (", ".join(f"`{n}`" for n in hot_twins) or "none") + "."
        )
    else:
        hot_ans = "No — `union_hot_n4_h1` was not in the scored payload."

    if hold_twins:
        hold_ans = (
            "A holdup twin appeared independently: "
            + ", ".join(f"`{n}`" for n in hold_twins) + "."
        )
    else:
        hold_ans = (
            "No — `union_hot_n4_holdup` / `s_boost=holdup` stayed stripped "
            "(landed 2026-09-19). No holdup twin was invented."
        )

    clk_core = clk_twins.get("core") or []
    clk_opp = clk_twins.get("oppset") or []
    if clk_featured:
        clk_ans = (
            "Yes — a 9/9 researcher with this toolbox would have featured "
            + _names(clk_featured) + "."
        )
    else:
        clk_ans = (
            "Catalogue singles were in the seed set "
            f"(core {len(clk_core)}, oppset {len(clk_opp)}) but **none** "
            "cleared Cyrus Starts YES + Book% on 8/13–9/9. They were "
            "available; they were not featured."
        )

    oos_hot = ""
    if hot:
        oos_hot = (
            f" Hot4 OOS continued {_md_pp(hot.get('oos_book_pct_continued'))}% · "
            f"fresh $10k {_md_pp(hot.get('fresh_book_pct'))}%."
        )

    lines += [
        "",
        "## Frozen recipe names (Excel fee-aware OOS handoff)",
        "",
        "IS freeze only — do not re-select on 9/10–9/18. Same list as "
        f"`{(payload.get('paths') or {}).get('frozen_list') or 'FROZEN_RECIPES.txt'}`.",
        "",
        "```",
        "\n".join(r["name"] for r in rows),
        "```",
        "",
        "## Verdict",
        "",
        "### Contrast vs #286 (no-oppset) and the live contaminated board",
        "",
        "- **#286** used this same 9/9 method **without** Clock-B / oppset "
        "seeds. It featured 22 Cyrus sleeves (mostly `combo_sh_*` / "
        "`combo_seh_*` / sj / sn / news-vol+short). hot4 failed featuring "
        "(11/19); holdup was not found. OOS Book% survivors were the six "
        "sh / seh mixes.",
        "- **This remine** adds the T−1 Clock-B / oppset aisle to that "
        "toolbox and lets the ≤9/9 grid rediscover winners. Live FOCUS / "
        "ALWAYS still never enter the keep set.",
        "- **Live Pages board** (8/13→9/18) ranked while seeing 9/10–9/18 "
        "and pins `union_hot_n4_holdup`, overnight_mega, Clock-B catalogue "
        "as live contamination. That pack is not this freeze.",
        "",
        "### Would a 9/9 researcher with Clock-B / oppset have found them?",
        "",
        f"- **hot4 / `union_hot_n4_h1`:** {hot_ans}{oos_hot}",
        f"- **holdup / `union_hot_n4_holdup`:** {hold_ans}",
        f"- **Clock-B / oppset twins:** {clk_ans}",
        "",
        f"Cyrus featured that still print a positive continued OOS book: "
        f"{_names(cyrus_surv)}.",
        "",
        f"Of those, Clock-B / oppset-touched: {_names(clk_surv)}.",
        "",
        f"Cyrus featured that fade after the cut: {_names(cyrus_fade)}.",
        "",
        f"Formal-bar KEEP count: **{len(formal)}**. "
        f"Cyrus featured count: **{len(cyrus)}**.",
        "",
        "OOS **continued** walks the 9/9 cash book forward. "
        f"OOS **fresh $10k** wakes the frozen recipe on {oos_start} "
        "with empty lots.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_frozen_list(names: list[str], dest: Path) -> None:
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        "# Frozen ≤2026-09-09 oppset/Clock-B recipe names\n"
        "# Excel fee-aware OOS handoff. Do not re-select on 9/10–9/18.\n"
        + "\n".join(names) + "\n",
        encoding="utf-8",
    )


def write_blind_report(payload: dict, full_panel: dict, *,
                       cutoff: str = CUTOFF,
                       oos_start: str = OOS_START,
                       out_root: Path | None = None,
                       asof_md: Path | None = None,
                       bars=None, fees=None, regime=None) -> dict:
    full_panel = fm.rehydrate_panel(full_panel)
    cal = list(full_panel.get("session_dates") or [])
    oos_end = cal[-1] if cal else oos_start
    names = freeze_names(payload)
    rows = fma.score_holdout(
        payload, full_panel, cutoff=cutoff, oos_start=oos_start,
        names=names, bars=bars, fees=fees, regime=regime)
    if out_root:
        write_frozen_list(names, Path(out_root) / "FROZEN_RECIPES.txt")
        payload.setdefault("paths", {})["frozen_list"] = str(
            Path(out_root) / "FROZEN_RECIPES.txt")
    md = render_blind_md(
        payload, rows, cutoff=cutoff, oos_start=oos_start, oos_end=oos_end)
    blob = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "kind": "blind_formation_0909_oppset",
        "cutoff": cutoff,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "bar": dict(fm.WORKABLE_BAR),
        "always": [],
        "cyrus_featured": (payload.get("blind") or {}).get("cyrus_featured") or [],
        "formal_keep": (payload.get("blind") or {}).get("formal_keep") or [],
        "n_report": len(rows),
        "rows": rows,
        "recipe_freeze": RECIPE_FREEZE_SHA,
        "grid_freeze": GRID_FREEZE_SHA,
        "clock_b_builder": CLOCK_B_BUILDER_SHA,
        "aisle": (payload.get("blind") or {}).get("aisle") or {},
        "frozen_names": names,
    }
    if out_root:
        out_root = Path(out_root)
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / "holdout.json").write_text(
            json.dumps(blob, indent=2), encoding="utf-8")
        (out_root / "FACTOR_MINE_BLIND.md").write_text(md, encoding="utf-8")
    dest_md = Path(asof_md) if asof_md else None
    if dest_md:
        dest_md.parent.mkdir(parents=True, exist_ok=True)
        dest_md.write_text(md, encoding="utf-8")
        print(f"[blind-oppset] wrote {dest_md}", flush=True)
    return blob


def prepare_panels(*, from_date: str = FROM_DATE, to_date: str = CUTOFF,
                   pull: bool = False, rebuild_panel: bool = False) -> tuple[dict, dict, dict]:
    """Load standing tape, union oppset in-memory, slice IS. Live panel stays."""
    enable_oppset_union()
    csv_path = ensure_oppset_csv(pull=pull)
    idx = opp.load_index(csv_path)
    proof = t1_proof(idx, is_max=to_date or CUTOFF)
    if not proof.get("t1_clean") or not proof.get("is_asof_ok"):
        raise SystemExit(f"oppset T−1 contract failed: {proof}")
    full_panel = fm.load_or_build_panel(from_date, None, rebuild=rebuild_panel)
    full_panel = fm.rehydrate_panel(full_panel)
    fm.attach_tape_flow(full_panel)
    # Full aisle (IS + OOS mornings) for holdout scoring only.
    full_panel = union_oppset_on_panel(full_panel, index=idx, persist=False)
    is_idx = filter_index(idx, join_morning_max=to_date or CUTOFF)
    panel = fm.slice_panel(full_panel, from_date, to_date)
    # Re-stamp IS slice from the IS-only index so OOS join_morning
    # cannot leak onto IS rows (dates already exclude OOS; belt + braces).
    opp.reset_index()
    opp.attach_panel(panel, is_idx)
    cbt.attach_panel(panel)
    aisle = {
        "csv": str(csv_path),
        "ref": opp.DEFAULT_REF,
        "t1_proof": proof,
        "union": dict(full_panel.get("oppset_union") or {}),
        "is_n_rows": panel.get("n_rows"),
        "full_n_rows": full_panel.get("n_rows"),
        "is_oppset_n": sum(1 for r in (panel.get("rows") or []) if fm.on_oppset(r)),
        "join_morning_is_max": to_date or CUTOFF,
    }
    return panel, full_panel, aisle


def run_blind(*, from_date: str = FROM_DATE, to_date: str = CUTOFF,
              write: bool = False, auto_tweak: bool = True,
              paths: dict | None = None, panel: dict | None = None,
              full_panel: dict | None = None, aisle: dict | None = None,
              holdout: bool = False, asof_md: Path | None = None,
              bars=None, universe="auto", hold="auto", gate="auto",
              rank="auto", side="auto", top_n="auto", exit="auto",
              entry="auto", size="auto", sell="auto", s_boost="auto",
              rebuild_panel: bool = False, pull_oppset: bool = False) -> dict:
    enable_oppset_union()
    if panel is None or full_panel is None or aisle is None:
        panel, full_panel, aisle = prepare_panels(
            from_date=from_date, to_date=to_date, pull=pull_oppset,
            rebuild_panel=rebuild_panel)
    recipes = blind_recipes(
        universe=universe, hold=hold, gate=gate, rank=rank, side=side,
        top_n=top_n, exit=exit, entry=entry, size=size, sell=sell,
        s_boost=s_boost, auto_tweak=auto_tweak)
    print(f"[blind-oppset] singles={len(recipes)} "
          f"(Clock-B/oppset ON; holdup / overnight_mega / ALWAYS stripped) "
          f"is_rows={panel.get('n_rows')} aisle_added="
          f"{(aisle.get('union') or {}).get('added')}",
          flush=True)
    persist = bool(write) and bool((paths or {}).get("persist_panel"))
    if persist:
        raise SystemExit("refusing to persist the live factor-mine panel")
    payload = fm.run(
        from_date, to_date, write=False, recipes=recipes, panel=panel,
        rebuild_panel=False, persist_panel=False, book=True,
        bars=bars, combos=False, paths=paths)
    payload["n_singles"] = len(recipes)
    frozen_specs = combo_specs_asof_0909_oppset()
    n_clk_specs = sum(1 for s in frozen_specs if s.get("clock_b"))
    formed = fmbld.form_combos_from_is(
        payload.get("stats") or [], existing=frozen_specs)
    specs = frozen_specs + formed
    print(f"[blind-oppset] combo specs frozen={len(frozen_specs)} "
          f"clock_b={n_clk_specs} formed={len(formed)}", flush=True)
    fees = fm.pt_fees()
    regime = fmb.load_regime()
    combo_stats, combo_books = fmc.run_combos(
        panel, recipes, bars=bars, fees=fees, regime=regime,
        member_stat_by={s["name"]: s for s in (payload.get("stats") or [])},
        specs=specs)
    if write:
        fmc.write_combo_sidecar(
            combo_stats, dest=(paths or {}).get("combo"))
    payload = merge_formed_combos(payload, combo_stats, combo_books, aisle=aisle)
    payload["n_mined"] = len(payload.get("stats") or [])
    payload["n_formed_combos"] = len(formed)
    combo_meta = dict(payload.get("combos") or {})
    combo_meta["frozen_n"] = len(frozen_specs)
    combo_meta["clock_b_n"] = n_clk_specs
    combo_meta["formed_n"] = len(formed)
    combo_meta["formed"] = [s["name"] for s in formed]
    payload["combos"] = combo_meta
    payload = apply_blind_featured(payload, aisle=aisle)
    keep = set(freeze_names(payload))
    if write:
        fm.write_outputs(
            payload, payload.get("stats"),
            books=combo_books if (paths or {}).get("write_actions") else None,
            paths=paths, always=(), keep_names=keep, pin_long_led=False)
        if paths and paths.get("json"):
            write_frozen_list(
                freeze_names(payload), Path(paths["json"]).parent / "FROZEN_RECIPES.txt")
    if holdout:
        write_blind_report(
            payload, full_panel,
            cutoff=to_date or CUTOFF, oos_start=OOS_START,
            out_root=(paths or {}).get("json") and Path(paths["json"]).parent,
            asof_md=asof_md, bars=bars, fees=fees, regime=regime)
    return payload


def run_cli(args, paths: dict) -> int:
    live_json = Path(paths["json"]).resolve()
    if live_json == fm.OUT_JSON.resolve():
        raise SystemExit("refusing to write live factor-mine board")
    if Path(paths["dash"]).resolve() == fm.DASH_DIR.resolve():
        raise SystemExit("refusing to write live dashboard/factor-mine/")
    from_date = args.from_date or FROM_DATE
    to_date = args.to_date or CUTOFF
    pull = bool(getattr(args, "pull_oppset", False))
    panel, full_panel, aisle = prepare_panels(
        from_date=from_date, to_date=to_date, pull=pull,
        rebuild_panel=bool(getattr(args, "rebuild_panel", False)))
    cal = list(panel.get("session_dates") or [])
    if from_date and from_date not in cal:
        raise SystemExit(
            f"panel missing --from-date {from_date}; "
            f"have {cal[:3]}…{cal[-3:] if len(cal) >= 3 else cal}")
    if to_date and to_date not in cal:
        raise SystemExit(
            f"panel missing --to-date {to_date}; "
            f"last session is {cal[-1] if cal else 'none'}")
    print(f"[blind-oppset] out-root {paths['json'].parent} "
          f"slice {panel.get('from_date')}→{panel.get('to_date')} "
          f"rows={panel.get('n_rows')} oppset={aisle.get('is_oppset_n')} "
          f"(live panel untouched)",
          flush=True)
    asof_md = Path(args.asof_md) if getattr(args, "asof_md", "") else None
    if asof_md and not asof_md.is_absolute():
        asof_md = fm.ROOT / asof_md
    if asof_md is None and args.write:
        asof_md = fm.ROOT / DEFAULT_MD
    payload = run_blind(
        from_date=from_date, to_date=to_date, write=args.write,
        auto_tweak=args.auto_tweak, paths=paths if args.write else None,
        panel=panel, full_panel=full_panel, aisle=aisle,
        holdout=bool(getattr(args, "holdout", False)),
        asof_md=asof_md,
        universe=args.universe, hold=args.hold, gate=args.gate,
        rank=args.rank, side=args.side, top_n=args.top_n, exit=args.exit,
        entry=args.entry, size=args.size, sell=args.sell,
        s_boost=args.s_boost, rebuild_panel=False,
        pull_oppset=False)
    print(f"[blind-oppset] recipes={payload.get('n_recipes')} "
          f"singles={payload.get('n_singles')} "
          f"formed={payload.get('n_formed_combos')} "
          f"cyrus={len((payload.get('blind') or {}).get('cyrus_featured') or [])} "
          f"to={payload.get('to_date')}")
    for s in (payload.get("stats") or [])[:8]:
        print(f"  {s['name']:32s}  win={fm._pct(s.get('win_rate'))}  "
              f"starts={s.get('start_green')}/{s.get('start_n')}  "
              f"tot={fm._n(s.get('total_ret_pct'))}%")
    return 0


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=FROM_DATE)
    ap.add_argument("--to-date", default=CUTOFF)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--holdout", action="store_true")
    ap.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    ap.add_argument("--dash-dir", default=DEFAULT_DASH)
    ap.add_argument("--asof-md", default=DEFAULT_MD)
    ap.add_argument("--auto-tweak", dest="auto_tweak", action="store_true",
                    default=True)
    ap.add_argument("--no-auto-tweak", dest="auto_tweak", action="store_false")
    ap.add_argument("--rebuild-panel", action="store_true")
    ap.add_argument("--pull-oppset", action="store_true")
    ap.add_argument("--universe", default="auto")
    ap.add_argument("--hold", default="auto")
    ap.add_argument("--gate", default="auto")
    ap.add_argument("--rank", default="auto")
    ap.add_argument("--side", default="auto")
    ap.add_argument("--top-n", default="auto")
    ap.add_argument("--exit", default="auto")
    ap.add_argument("--entry", default="auto")
    ap.add_argument("--size", default="auto")
    ap.add_argument("--sell", default="auto")
    ap.add_argument("--s-boost", dest="s_boost", default="auto")
    args = ap.parse_args(argv)
    paths = fm.publish_paths(args.out_root, args.dash_dir)
    return run_cli(args, paths)


if __name__ == "__main__":
    raise SystemExit(main())
