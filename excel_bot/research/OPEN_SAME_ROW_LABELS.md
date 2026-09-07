# Same-row open labels (value / text / fill gate)

_Generated 2026-09-07 · research only · live `flatten_robust` frozen._

## Plain English

This file is the **Excel-locked no-peek gate** for the reopen mine (numbers + text + fills, not highlights alone). The miner asserts this 44 + open-fill list against `clock_map.json` and will refuse to invent clocks.

- **Same-row day X** is usable at the **open** only when the clock map says open.
- **Upper rows** (t−1, t−2, …) are always fair — already known by the open of day X.
- Do **not** re-litigate killed highlight ghosts. Standing open keep stays five-cell light + green O ± AH/FR.

Source of truth: `CLOCK_MAP.md` + `clock_map.json` tip on this branch.

## Same-row OPEN — fill / highlight (timing-proven)

Only these fills may gate an **open entry** on day X’s own row:

`A, B, C, G, J, K, L, M, O, IR, IS, IT`

(`IR/IS/IT` are STOCKHISTORY spill aliases of A–F family; treat like measured A–O.)

## Same-row OPEN — number / text values (`value_mine_open`, 44)

These formula values may gate an **open entry** on day X’s own row:

`A, C, J, Q, Z, AC, AH, BT, BV, CG, CH, DC, DE, EB, EK, EN, EP, EQ, ER, ES, ET, EU, EV, FQ, FR, FS, FU, GD, GE, GF, HF, HG, HW, II, IR, IT, IY, IZ, JB, JC, JD, JE, JF, JL`

Clustered for pilots: `EP–EV`, `GD–GF`, `JB–JF`.

## Same-row CLOSE landmines (do not peek)

| letter | trap |
|---|---|
| B, G, K, M | fill can look open; **value is close** |
| O | **green fill** is open; **O number** is unknown→mine **close** |
| L | value unknown→mine **close** |
| D, E, F | same-row close always |
| **H, I** | **labels only** (intraday % / daily %). Never same-row features on day X. Lags t−1+ are fair. |
| N | close |
| `core_score` / any def that reads D/E/F/H/I same-row | **close entry only** |

Unknown fill or unknown value → mine as **close**. Mixing any close-mine atom into a rule forces **close entry**.

## Lags (upper rows)

Any letter at lag t−1+ is fair for open-entry features. Example shape Cyrus named:

- `O[t−2] < 1` — OK at open (prior row).
- `AA[t] = 1` — AA today is **value_mine_close** → that pair is **close-entry** unless AA is shifted to a prior row.

## Close-entry today legs (this beat)

Same-row **value-close** letters may gate a **close entry** on day X’s own row. The 44 `value_mine_open` letters and the timing-tested **open fills** stay open — do not treat them as close-today.

Bounded then expand: A–O value-close + AA, then through AO, skipping killed highlight letters (T/BA, CZ/EH/IB/HO/IL/GV) and **never H/I same-row**. Timing-close fills as features: `D, E, F, N` only — not H/I fills. O green is still open; O number is close.

Excel confirmed: H and I are labels only. Open features = this file’s open lists only. Close-entry may use other close cols, not feed H/I into themselves.

## Pilot order (for Taskforce)

1. **Open-entry pairwise** first: open fills ∪ 44 `value_mine_open` values, lags on any letter, singles then pairwise under ship + ghost bar. **Accepted null** (KEEP 0 / KILL 4266).
2. **Close-entry** beat second (includes AA-today-style pairs). This beat.
3. Do not reopen killed highlight ghosts (T/BA, CZ/EH/IB/HO/IL/GV, same-day counts, etc.).

## Counts (from clock map)

| set | n |
|---|---:|
| fill_mine_open | 12 |
| value_mine_open | 44 |
| value_mine_close | 231 |
| fill_mine_close (unk→close) | 263 |

Research only. Live frozen.
