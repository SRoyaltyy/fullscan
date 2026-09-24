# Factor mine action — `union_clk_mom_break_peer_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #1 ∩ Theme Radar T−1 oppset

Cash book **+15.77%** ($11,577) · signal-only (no cash/fees) was +20.76%. Starts YES **26/30**. Fills 132 · skips 48 · realized $+1577.14.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Clock-B #1: moderate prior momentum, a completed 10-session breakout (or candle capture), and peer or sector camera green.
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `clk_mom_break_peer=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,577.13.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `YSS` | 331 | — | $10.06 | +0.00 | $10.93 | +287.97 | +287.97 | +0.00 | +287.97 |
| 2026-08-14 | `ADUR` | 202 | — | $16.50 | +0.00 | $16.17 | -66.66 | -66.66 | +0.00 | -66.66 |
| 2026-08-14 | `WDC` | 6 | — | $503.50 | +0.00 | $508.80 | +31.80 | +31.80 | +0.00 | +31.80 |
| 2026-08-17 | `YSS` | 331 | $10.93 | $10.36 | -188.67 | — | +0.00 | -188.67 | +99.30 | — |
| 2026-08-17 | `ADUR` | 202 | $16.17 | $15.73 | -88.88 | — | +0.00 | -88.88 | -155.54 | — |
| 2026-08-17 | `WDC` | 6 | $508.80 | $525.53 | +100.38 | — | +0.00 | +100.38 | +132.18 | — |
| 2026-08-17 | `ALM` | 620 | — | $16.20 | +0.00 | $16.36 | +99.20 | +99.20 | +0.00 | +99.20 |
| 2026-08-18 | `ALM` | 620 | $16.36 | $15.78 | -359.60 | — | +0.00 | -359.60 | -260.40 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MSTR` | 10 | — | $113.23 | +0.00 | $112.39 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-20 | `DNA` | 164 | — | $7.45 | +0.00 | $6.96 | -80.36 | -80.36 | +0.00 | -80.36 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `ATAT` | 35 | — | $34.05 | +0.00 | $34.25 | +7.00 | +7.00 | +0.00 | +7.00 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BLSH` | 41 | — | $29.20 | +0.00 | $28.44 | -31.16 | -31.16 | +0.00 | -31.16 |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `DNA` | 164 | $6.96 | $7.09 | +21.32 | — | +0.00 | +21.32 | -59.04 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `ATAT` | 35 | $34.25 | $34.31 | +2.10 | — | +0.00 | +2.10 | +9.10 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `GMAB` | 75 | — | $33.36 | +0.00 | $33.45 | +6.75 | +6.75 | +0.00 | +6.75 |
| 2026-08-21 | `DE` | 4 | — | $623.26 | +0.00 | $647.47 | +96.84 | +96.84 | +0.00 | +96.84 |
| 2026-08-21 | `CF` | 19 | — | $127.43 | +0.00 | $129.60 | +41.23 | +41.23 | +0.00 | +41.23 |
| 2026-08-21 | `DXYZ` | 72 | — | $34.89 | +0.00 | $34.43 | -33.12 | -33.12 | +0.00 | -33.12 |
| 2026-08-24 | `GMAB` | 75 | $33.45 | $32.82 | -47.25 | — | +0.00 | -47.25 | -40.50 | — |
| 2026-08-24 | `DE` | 4 | $647.47 | $653.04 | +22.28 | — | +0.00 | +22.28 | +119.12 | — |
| 2026-08-24 | `CF` | 19 | $129.60 | $129.99 | +7.41 | — | +0.00 | +7.41 | +48.64 | — |
| 2026-08-24 | `DXYZ` | 72 | $34.43 | $33.10 | -95.76 | — | +0.00 | -95.76 | -128.88 | — |
| 2026-08-25 | `VALE` | 672 | — | $15.01 | +0.00 | $15.33 | +215.04 | +215.04 | +0.00 | +215.04 |
| 2026-08-26 | `VALE` | 672 | $15.33 | $15.37 | +26.88 | — | +0.00 | +26.88 | +241.92 | — |
| 2026-08-26 | `BZ` | 615 | — | $16.77 | +0.00 | $18.84 | +1273.05 | +1273.05 | +0.00 | +1273.05 |
| 2026-08-27 | `BZ` | 615 | $18.84 | $18.50 | -209.10 | — | +0.00 | -209.10 | +1063.95 | — |
| 2026-08-28 | `SEDG` | 43 | — | $32.90 | +0.00 | $31.41 | -64.07 | -64.07 | +0.00 | -64.07 |
| 2026-08-28 | `ZYME` | 49 | — | $28.91 | +0.00 | $28.27 | -31.36 | -31.36 | +0.00 | -31.36 |
| 2026-08-28 | `TH` | 74 | — | $19.00 | +0.00 | $18.55 | -33.30 | -33.30 | +0.00 | -33.30 |
| 2026-08-28 | `HAFN` | 170 | — | $8.35 | +0.00 | $8.47 | +20.40 | +20.40 | +0.00 | +20.40 |
| 2026-08-28 | `S` | 66 | — | $21.49 | +0.00 | $21.54 | +3.30 | +3.30 | +0.00 | +3.30 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `PD` | 108 | — | $13.09 | +0.00 | $13.83 | +79.92 | +79.92 | +0.00 | +79.92 |
| 2026-08-28 | `RBRK` | 14 | — | $98.95 | +0.00 | $93.05 | -82.60 | -82.60 | +0.00 | -82.60 |
| 2026-08-31 | `SEDG` | 43 | $31.41 | $31.15 | -11.18 | — | +0.00 | -11.18 | -75.25 | — |
| 2026-08-31 | `ZYME` | 49 | $28.27 | $28.06 | -10.29 | — | +0.00 | -10.29 | -41.65 | — |
| 2026-08-31 | `TH` | 74 | $18.55 | $18.12 | -31.45 | — | +0.00 | -31.45 | -64.75 | — |
| 2026-08-31 | `HAFN` | 170 | $8.47 | $8.53 | +10.20 | — | +0.00 | +10.20 | +30.60 | — |
| 2026-08-31 | `S` | 66 | $21.54 | $21.45 | -5.94 | — | +0.00 | -5.94 | -2.64 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `PD` | 108 | $13.83 | $13.58 | -27.00 | — | +0.00 | -27.00 | +52.92 | — |
| 2026-08-31 | `RBRK` | 14 | $93.05 | $92.83 | -3.08 | — | +0.00 | -3.08 | -85.68 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `DELL` | 5 | — | $486.31 | +0.00 | $516.39 | +150.40 | +150.40 | +0.00 | +150.40 |
| 2026-09-03 | `VSTM` | 346 | — | $8.03 | +0.00 | $7.98 | -17.30 | -17.30 | +0.00 | -17.30 |
| 2026-09-03 | `MEI` | 184 | — | $15.09 | +0.00 | $15.32 | +42.32 | +42.32 | +0.00 | +42.32 |
| 2026-09-03 | `ATRC` | 52 | — | $52.88 | +0.00 | $52.46 | -21.84 | -21.84 | +0.00 | -21.84 |
| 2026-09-04 | `DELL` | 5 | $516.39 | $513.78 | -13.05 | $524.14 | +51.80 | +38.75 | +137.35 | +189.15 |
| 2026-09-04 | `VSTM` | 346 | $7.98 | $7.91 | -24.22 | — | +0.00 | -24.22 | -41.52 | — |
| 2026-09-04 | `MEI` | 184 | $15.32 | $15.34 | +3.68 | — | +0.00 | +3.68 | +46.00 | — |
| 2026-09-04 | `ATRC` | 52 | $52.46 | $52.03 | -22.36 | — | +0.00 | -22.36 | -44.20 | — |
| 2026-09-04 | `LULU` | 12 | — | $98.15 | +0.00 | $100.61 | +29.52 | +29.52 | +0.00 | +29.52 |
| 2026-09-04 | `MRX` | 16 | — | $75.65 | +0.00 | $78.27 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-09-04 | `MSTR` | 8 | — | $137.35 | +0.00 | $142.80 | +43.60 | +43.60 | +0.00 | +43.60 |
| 2026-09-04 | `KYIV` | 86 | — | $14.24 | +0.00 | $14.11 | -11.18 | -11.18 | +0.00 | -11.18 |
| 2026-09-04 | `GWRE` | 7 | — | $167.55 | +0.00 | $162.42 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-09-04 | `BLSH` | 35 | — | $34.69 | +0.00 | $36.00 | +45.85 | +45.85 | +0.00 | +45.85 |
| 2026-09-04 | `ZETA` | 37 | — | $32.65 | +0.00 | $31.35 | -48.10 | -48.10 | +0.00 | -48.10 |
| 2026-09-08 | `DELL` | 5 | $524.14 | $521.15 | -14.95 | — | +0.00 | -14.95 | +174.20 | — |
| 2026-09-08 | `LULU` | 12 | $100.61 | $100.58 | -0.36 | — | +0.00 | -0.36 | +29.16 | — |
| 2026-09-08 | `MRX` | 16 | $78.27 | $78.84 | +9.12 | — | +0.00 | +9.12 | +51.04 | — |
| 2026-09-08 | `MSTR` | 8 | $142.80 | $137.62 | -41.44 | — | +0.00 | -41.44 | +2.16 | — |
| 2026-09-08 | `KYIV` | 86 | $14.11 | $14.14 | +2.58 | — | +0.00 | +2.58 | -8.60 | — |
| 2026-09-08 | `GWRE` | 7 | $162.42 | $160.52 | -13.30 | — | +0.00 | -13.30 | -49.21 | — |
| 2026-09-08 | `BLSH` | 35 | $36.00 | $35.90 | -3.50 | — | +0.00 | -3.50 | +42.35 | — |
| 2026-09-08 | `ZETA` | 37 | $31.35 | $31.08 | -9.99 | — | +0.00 | -9.99 | -58.09 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `VIST` | 18 | — | $77.33 | +0.00 | $76.27 | -19.08 | -19.08 | +0.00 | -19.08 |
| 2026-09-11 | `PBR` | 66 | — | $21.21 | +0.00 | $21.20 | -0.66 | -0.66 | +0.00 | -0.66 |
| 2026-09-11 | `GME` | 66 | — | $21.04 | +0.00 | $21.15 | +7.26 | +7.26 | +0.00 | +7.26 |
| 2026-09-11 | `INSP` | 20 | — | $69.88 | +0.00 | $73.00 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-09-11 | `PUMP` | 121 | — | $11.57 | +0.00 | $11.45 | -14.52 | -14.52 | +0.00 | -14.52 |
| 2026-09-11 | `PAGS` | 138 | — | $10.11 | +0.00 | $10.12 | +1.38 | +1.38 | +0.00 | +1.38 |
| 2026-09-11 | `BAND` | 26 | — | $52.55 | +0.00 | $56.87 | +112.32 | +112.32 | +0.00 | +112.32 |
| 2026-09-11 | `FUBO` | 121 | — | $11.55 | +0.00 | $11.53 | -2.42 | -2.42 | +0.00 | -2.42 |
| 2026-09-14 | `VIST` | 18 | $76.27 | $77.10 | +14.94 | — | +0.00 | +14.94 | -4.14 | — |
| 2026-09-14 | `PBR` | 66 | $21.20 | $21.23 | +1.98 | — | +0.00 | +1.98 | +1.32 | — |
| 2026-09-14 | `GME` | 66 | $21.15 | $21.00 | -9.90 | $21.62 | +40.92 | +31.02 | -2.64 | +38.28 |
| 2026-09-14 | `INSP` | 20 | $73.00 | $72.14 | -17.20 | — | +0.00 | -17.20 | +45.20 | — |
| 2026-09-14 | `PUMP` | 121 | $11.45 | $11.17 | -33.88 | — | +0.00 | -33.88 | -48.40 | — |
| 2026-09-14 | `PAGS` | 138 | $10.12 | $10.00 | -16.56 | — | +0.00 | -16.56 | -15.18 | — |
| 2026-09-14 | `BAND` | 26 | $56.87 | $56.90 | +0.78 | — | +0.00 | +0.78 | +113.10 | — |
| 2026-09-14 | `FUBO` | 121 | $11.53 | $11.56 | +3.63 | — | +0.00 | +3.63 | +1.21 | — |
| 2026-09-15 | `GME` | 66 | $21.62 | $21.51 | -7.26 | — | +0.00 | -7.26 | +31.02 | — |
| 2026-09-16 | `TALO` | 79 | — | $17.87 | +0.00 | $17.42 | -35.55 | -35.55 | +0.00 | -35.55 |
| 2026-09-16 | `VAL` | 16 | — | $87.40 | +0.00 | $82.52 | -78.08 | -78.08 | +0.00 | -78.08 |
| 2026-09-16 | `RIG` | 241 | — | $5.87 | +0.00 | $5.54 | -79.53 | -79.53 | +0.00 | -79.53 |
| 2026-09-16 | `ILMN` | 6 | — | $224.49 | +0.00 | $228.93 | +26.64 | +26.64 | +0.00 | +26.64 |
| 2026-09-16 | `ADPT` | 52 | — | $27.09 | +0.00 | $27.67 | +30.16 | +30.16 | +0.00 | +30.16 |
| 2026-09-16 | `MRCY` | 16 | — | $87.52 | +0.00 | $87.25 | -4.32 | -4.32 | +0.00 | -4.32 |
| 2026-09-16 | `TEM` | 20 | — | $68.79 | +0.00 | $69.97 | +23.60 | +23.60 | +0.00 | +23.60 |
| 2026-09-16 | `SM` | 35 | — | $39.99 | +0.00 | $38.16 | -64.05 | -64.05 | +0.00 | -64.05 |
| 2026-09-17 | `TALO` | 79 | $17.42 | $17.19 | -18.17 | — | +0.00 | -18.17 | -53.72 | — |
| 2026-09-17 | `VAL` | 16 | $82.52 | $83.20 | +10.88 | — | +0.00 | +10.88 | -67.20 | — |
| 2026-09-17 | `RIG` | 241 | $5.54 | $5.58 | +9.64 | — | +0.00 | +9.64 | -69.89 | — |
| 2026-09-17 | `ILMN` | 6 | $228.93 | $233.85 | +29.52 | — | +0.00 | +29.52 | +56.16 | — |
| 2026-09-17 | `ADPT` | 52 | $27.67 | $28.23 | +29.12 | — | +0.00 | +29.12 | +59.28 | — |
| 2026-09-17 | `MRCY` | 16 | $87.25 | $89.27 | +32.32 | — | +0.00 | +32.32 | +28.00 | — |
| 2026-09-17 | `TEM` | 20 | $69.97 | $72.70 | +54.60 | — | +0.00 | +54.60 | +78.20 | — |
| 2026-09-17 | `SM` | 35 | $38.16 | $37.57 | -20.65 | — | +0.00 | -20.65 | -84.70 | — |
| 2026-09-17 | `ARQT` | 72 | — | $25.95 | +0.00 | $26.46 | +36.72 | +36.72 | +0.00 | +36.72 |
| 2026-09-17 | `AMRX` | 100 | — | $18.56 | +0.00 | $18.28 | -28.00 | -28.00 | +0.00 | -28.00 |
| 2026-09-17 | `PGEN` | 246 | — | $7.59 | +0.00 | $7.87 | +68.88 | +68.88 | +0.00 | +68.88 |
| 2026-09-17 | `FTAI` | 9 | — | $196.50 | +0.00 | $195.07 | -12.87 | -12.87 | +0.00 | -12.87 |
| 2026-09-17 | `SMTC` | 10 | — | $170.85 | +0.00 | $178.19 | +73.40 | +73.40 | +0.00 | +73.40 |
| 2026-09-17 | `FOSL` | 342 | — | $5.46 | +0.00 | $5.63 | +58.14 | +58.14 | +0.00 | +58.14 |
| 2026-09-18 | `ARQT` | 72 | $26.46 | $26.14 | -23.04 | — | +0.00 | -23.04 | +13.68 | — |
| 2026-09-18 | `AMRX` | 100 | $18.28 | $18.12 | -16.00 | — | +0.00 | -16.00 | -44.00 | — |
| 2026-09-18 | `PGEN` | 246 | $7.87 | $7.98 | +27.06 | — | +0.00 | +27.06 | +95.94 | — |
| 2026-09-18 | `FTAI` | 9 | $195.07 | $195.55 | +4.32 | — | +0.00 | +4.32 | -8.55 | — |
| 2026-09-18 | `SMTC` | 10 | $178.19 | $182.33 | +41.40 | — | +0.00 | +41.40 | +114.80 | — |
| 2026-09-18 | `FOSL` | 342 | $5.63 | $5.63 | +0.00 | — | +0.00 | +0.00 | +58.14 | — |
| 2026-09-18 | `RARE` | 110 | — | $14.79 | +0.00 | $14.51 | -30.80 | -30.80 | +0.00 | -30.80 |
| 2026-09-18 | `BHVN` | 116 | — | $14.07 | +0.00 | $13.62 | -52.20 | -52.20 | +0.00 | -52.20 |
| 2026-09-18 | `TH` | 78 | — | $20.91 | +0.00 | $21.19 | +21.84 | +21.84 | +0.00 | +21.84 |
| 2026-09-18 | `SYM` | 36 | — | $44.70 | +0.00 | $41.89 | -101.16 | -101.16 | +0.00 | -101.16 |
| 2026-09-18 | `AMD` | 2 | — | $547.37 | +0.00 | $559.82 | +24.90 | +24.90 | +0.00 | +24.90 |
| 2026-09-18 | `SHLS` | 213 | — | $7.64 | +0.00 | $7.60 | -8.52 | -8.52 | +0.00 | -8.52 |
| 2026-09-18 | `BNC` | 280 | — | $5.83 | +0.00 | $5.98 | +42.00 | +42.00 | +0.00 | +42.00 |
| 2026-09-21 | `RARE` | 110 | $14.51 | $14.58 | +7.70 | — | +0.00 | +7.70 | -23.10 | — |
| 2026-09-21 | `BHVN` | 116 | $13.62 | $13.90 | +32.48 | — | +0.00 | +32.48 | -19.72 | — |
| 2026-09-21 | `TH` | 78 | $21.19 | $21.65 | +35.88 | — | +0.00 | +35.88 | +57.72 | — |
| 2026-09-21 | `SYM` | 36 | $41.89 | $42.42 | +19.08 | — | +0.00 | +19.08 | -82.08 | — |
| 2026-09-21 | `AMD` | 2 | $559.82 | $583.88 | +48.12 | — | +0.00 | +48.12 | +73.02 | — |
| 2026-09-21 | `SHLS` | 213 | $7.60 | $7.71 | +23.43 | — | +0.00 | +23.43 | +14.91 | — |
| 2026-09-21 | `BNC` | 280 | $5.98 | $6.42 | +121.80 | — | +0.00 | +121.80 | +163.80 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +253.11 | YSS, ADUR, WDC | — | $307.26 | $10,244.23 | YSS×331, ADUR×202, WDC×6 |
| 2026-08-17 | +2.25 | $307.26 | YSS×331, ADUR×202, WDC×6 | $10,067.06 | -177.17 | +99.20 | ALM | YSS, ADUR, WDC | $6.00 | $10,149.20 | ALM×620 |
| 2026-08-18 | -6.20 | $6.00 | ALM×620 | $9,789.60 | -359.60 | +0.00 | — | ALM | $9,781.42 | $9,781.42 | — |
| 2026-08-19 | -7.20 | $9,781.42 | — | $9,781.42 | -0.00 | +0.00 | — | — | $9,781.42 | $9,781.42 | — |
| 2026-08-20 | +1.12 | $9,781.42 | — | $9,781.42 | -0.00 | +86.64 | MSTR, DNA, IAG, BHP, ATAT, KGC, AG, BLSH | — | $193.70 | $9,850.86 | MSTR×10, DNA×164, IAG×62, BHP×13, ATAT×35, KGC×41, AG×59, BLSH×41 |
| 2026-08-21 | +3.25 | $193.70 | MSTR×10, DNA×164, IAG×62, BHP×13, ATAT×35, KGC×41, AG×59, BLSH×41 | $10,141.93 | +291.07 | +111.70 | GMAB, DE, CF, DXYZ | MSTR, DNA, IAG, BHP, ATAT, KGC, AG, BLSH | $187.80 | $10,227.79 | GMAB×75, DE×4, CF×19, DXYZ×72 |
| 2026-08-24 | -5.17 | $187.80 | GMAB×75, DE×4, CF×19, DXYZ×72 | $10,114.47 | -113.32 | +0.00 | — | GMAB, DE, CF, DXYZ | $10,105.88 | $10,105.88 | — |
| 2026-08-25 | +1.80 | $10,105.88 | — | $10,105.88 | -0.00 | +215.04 | VALE | — | $10.49 | $10,312.25 | VALE×672 |
| 2026-08-26 | +2.02 | $10.49 | VALE×672 | $10,339.13 | +26.88 | +1,273.05 | BZ | VALE | $8.78 | $11,595.38 | BZ×615 |
| 2026-08-27 | — | $8.78 | BZ×615 | $11,386.28 | -209.10 | +0.00 | — | BZ | $11,378.15 | $11,378.15 | — |
| 2026-08-28 | +0.75 | $11,378.15 | — | $11,378.15 | +0.00 | -110.21 | SEDG, ZYME, TH, HAFN, S, ADSK, PD, RBRK | — | $180.70 | $11,250.44 | SEDG×43, ZYME×49, TH×74, HAFN×170, S×66, ADSK×5, PD×108, RBRK×14 |
| 2026-08-31 | -5.85 | $180.70 | SEDG×43, ZYME×49, TH×74, HAFN×170, S×66, ADSK×5, PD×108, RBRK×14 | $11,156.95 | -93.49 | +0.00 | — | SEDG, ZYME, TH, HAFN, S, ADSK, PD, RBRK | $11,139.24 | $11,139.24 | — |
| 2026-09-01 | -6.30 | $11,139.24 | — | $11,139.24 | +0.00 | +0.00 | — | — | $11,139.24 | $11,139.24 | — |
| 2026-09-02 | -3.83 | $11,139.24 | — | $11,139.24 | +0.00 | +0.00 | — | — | $11,139.24 | $11,139.24 | — |
| 2026-09-03 | -0.90 | $11,139.24 | — | $11,139.24 | +0.00 | +153.58 | DELL, VSTM, MEI, ATRC | — | $391.84 | $11,281.67 | DELL×5, VSTM×346, MEI×184, ATRC×52 |
| 2026-09-04 | +2.25 | $391.84 | DELL×5, VSTM×346, MEI×184, ATRC×52 | $11,225.72 | -55.95 | +117.50 | LULU, MRX, MSTR, KYIV, GWRE, BLSH, ZETA | VSTM, MEI, ATRC | $326.28 | $11,319.37 | DELL×5, LULU×12, MRX×16, MSTR×8, KYIV×86, GWRE×7, BLSH×35, ZETA×37 |
| 2026-09-08 | -11.47 | $326.28 | DELL×5, LULU×12, MRX×16, MSTR×8, KYIV×86, GWRE×7, BLSH×35, ZETA×37 | $11,247.53 | -71.84 | +0.00 | — | DELL, LULU, MRX, MSTR, KYIV, GWRE, BLSH, ZETA | $11,230.81 | $11,230.81 | — |
| 2026-09-09 | -13.95 | $11,230.81 | — | $11,230.81 | +0.00 | +0.00 | — | — | $11,230.81 | $11,230.81 | — |
| 2026-09-10 | -13.28 | $11,230.81 | — | $11,230.81 | +0.00 | +0.00 | — | — | $11,230.81 | $11,230.81 | — |
| 2026-09-11 | +0.50 | $11,230.81 | — | $11,230.81 | +0.00 | +146.68 | VIST, PBR, GME, INSP, PUMP, PAGS, BAND, FUBO | — | $76.13 | $11,359.85 | VIST×18, PBR×66, GME×66, INSP×20, PUMP×121, PAGS×138, BAND×26, FUBO×121 |
| 2026-09-14 | -11.00 | $76.13 | VIST×18, PBR×66, GME×66, INSP×20, PUMP×121, PAGS×138, BAND×26, FUBO×121 | $11,303.64 | -56.21 | +40.92 | — | VIST, PBR, INSP, PUMP, PAGS, BAND, FUBO | $9,901.99 | $11,328.91 | GME×66 |
| 2026-09-15 | -3.84 | $9,901.99 | GME×66 | $11,321.65 | -7.26 | +0.00 | — | GME | $11,319.44 | $11,319.44 | — |
| 2026-09-16 | +5.30 | $11,319.44 | — | $11,319.44 | +0.00 | -181.13 | TALO, VAL, RIG, ILMN, ADPT, MRCY, TEM, SM | — | $145.54 | $11,120.60 | TALO×79, VAL×16, RIG×241, ILMN×6, ADPT×52, MRCY×16, TEM×20, SM×35 |
| 2026-09-17 | +7.38 | $145.54 | TALO×79, VAL×16, RIG×241, ILMN×6, ADPT×52, MRCY×16, TEM×20, SM×35 | $11,247.86 | +127.26 | +196.27 | ARQT, AMRX, PGEN, FTAI, SMTC, FOSL | TALO, VAL, RIG, ILMN, ADPT, MRCY, TEM, SM | $277.97 | $11,410.10 | ARQT×72, AMRX×100, PGEN×246, FTAI×9, SMTC×10, FOSL×342 |
| 2026-09-18 | +4.86 | $277.97 | ARQT×72, AMRX×100, PGEN×246, FTAI×9, SMTC×10, FOSL×342 | $11,443.84 | +33.74 | -103.94 | RARE, BHVN, TH, SYM, AMD, SHLS, BNC | ARQT, AMRX, PGEN, FTAI, SMTC, FOSL | $556.49 | $11,306.21 | RARE×110, BHVN×116, TH×78, SYM×36, AMD×2, SHLS×213, BNC×280 |
| 2026-09-21 | +12.87 | $556.49 | RARE×110, BHVN×116, TH×78, SYM×36, AMD×2, SHLS×213, BNC×280 | $11,594.70 | +288.49 | +0.00 | — | RARE, BHVN, TH, SYM, AMD, SHLS, BNC | $11,577.13 | $11,577.13 | — |
| 2026-09-22 | -0.50 | $11,577.13 | — | $11,577.13 | -0.00 | +0.00 | — | — | $11,577.13 | $11,577.13 | — |
| 2026-09-23 | +2.29 | $11,577.13 | — | $11,577.13 | -0.00 | +0.00 | — | — | $11,577.13 | $11,577.13 | — |
| 2026-09-24 | -7.66 | $11,577.13 | — | $11,577.13 | -0.00 | +0.00 | — | — | $11,577.13 | $11,577.13 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 331 | $10.06 | $4.27 | — | $6,665.87 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 202 | $16.50 | $2.61 | — | $3,330.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 6 | $503.50 | $2.01 | — | $307.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $307.26 | ▲ close $10,244.23 vs 09:30 $10,000.00 (session +253.11) | 16:00 close · cash $307.26 · equity $10,244.23 vs 09:30 $10,000.00 (+244.23; session marks +253.11) · 3 name(s) marked open→close (per-name table). YSS×331 09:30 $10.06 → close $10.93 +287.97; ADUR×202 09:30 $16.50 → close $16.17 -66.66; WDC×6 09:30 $503.50 → close $508.80 +31.80 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $307.26 | ▼ 09:30 equity $10,067.06 vs yday $10,244.23 (-177.17) | 09:30 open · cash $307.26 (unchanged overnight, no fees) · equity $10,067.06 vs prior close $10,244.23 (-177.17) · 3 name(s) re-marked at the open (per-name table). YSS×331 yday $10.93 → 09:30 $10.36 -188.67; ADUR×202 yday $16.17 → 09:30 $15.73 -88.88; WDC×6 yday $508.80 → 09:30 $525.53 +100.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 331 | $10.36 | $4.35 | $+90.68 | $3,732.06 | ▲ +90.68 after sell → book $10,062.70; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 202 | $15.73 | $2.67 | $-160.81 | $6,906.86 | ▼ -160.81 after sell → book $10,060.04; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 6 | $525.53 | $2.04 | $+128.13 | $10,058.00 | ▲ +128.13 after sell → book $10,058.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 620 | $16.20 | $8.00 | — | $6.00 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $10058.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.00 | ▲ close $10,149.20 vs 09:30 $10,067.06 (session +99.20) | 16:00 close · cash $6.00 · equity $10,149.20 vs 09:30 $10,067.06 (+82.14; session marks +99.20) · 1 name(s) marked open→close (per-name table). ALM×620 09:30 $16.20 → close $16.36 +99.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.00 | ▼ 09:30 equity $9,789.60 vs yday $10,149.20 (-359.60) | 09:30 open · cash $6.00 (unchanged overnight, no fees) · equity $9,789.60 vs prior close $10,149.20 (-359.60) · 1 name(s) re-marked at the open (per-name table). ALM×620 yday $16.36 → 09:30 $15.78 -359.60 | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 620 | $15.78 | $8.18 | $-276.58 | $9,781.42 | ▼ -276.58 after sell → book $9,781.42; vs 09:30 mark -8.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.42 | ▲ close $9,781.42 vs 09:30 $9,789.60 (session +0.00) | 16:00 close · cash $9,781.42 · no lots left · equity $9,781.42. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.42 | ▲ 09:30 equity $9,781.42 vs yday $9,781.42 (-0.00) | 09:30 open · cash $9,781.42 · no holdings · equity $9,781.42 vs prior close $9,781.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.42 | ▲ close $9,781.42 vs 09:30 $9,781.42 (session +0.00) | 16:00 close · cash $9,781.42 · no lots left · equity $9,781.42. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.42 | ▲ 09:30 equity $9,781.42 vs yday $9,781.42 (-0.00) | 09:30 open · cash $9,781.42 · no holdings · equity $9,781.42 vs prior close $9,781.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $8,647.10 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1222.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 164 | $7.45 | $2.48 | — | $7,422.82 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1222.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $6,203.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1222.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $5,018.42 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1222.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 35 | $34.05 | $2.10 | — | $3,824.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+9.3; leftover $1222.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,607.63 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1222.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $1,393.02 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1222.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $193.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1222.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.70 | ▲ close $9,850.86 vs 09:30 $9,781.42 (session +86.64) | 16:00 close · cash $193.70 · equity $9,850.86 vs 09:30 $9,781.42 (+69.44; session marks +86.64) · 8 name(s) marked open→close (per-name table). MSTR×10 09:30 $113.23 → close $112.39 -8.40; DNA×164 09:30 $7.45 → close $6.96 -80.36; IAG×62 09:30 $19.63 → close $20.50 +53.94; BHP×13 09:30 $91.01 → close $93.63 +34.06; ATAT×35 09:30 $34.05 → close $34.25 +7.00; KGC×41 09:30 $29.63 → close $31.43 +73.80; AG×59 09:30 $20.55 → close $21.19 +37.76; BLSH×41 09:30 $29.20 → close $28.44 -31.16 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.70 | ▲ 09:30 equity $10,141.93 vs yday $9,850.86 (+291.07) | 09:30 open · cash $193.70 (unchanged overnight, no fees) · equity $10,141.93 vs prior close $9,850.86 (+291.07) · 8 name(s) re-marked at the open (per-name table). MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; DNA×164 yday $6.96 → 09:30 $7.09 +21.32; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; ATAT×35 yday $34.25 → 09:30 $34.31 +2.10; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; AG×59 yday $21.19 → 09:30 $21.90 +41.89; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71 | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $1,388.56 | ▲ +60.54 after sell → book $10,139.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 164 | $7.09 | $2.52 | $-64.04 | $2,548.80 | ▼ -64.04 after sell → book $10,137.37; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $3,859.15 | ▲ +91.11 after sell → book $10,135.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $5,101.46 | ▲ +57.15 after sell → book $10,133.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 35 | $34.31 | $2.12 | $+4.89 | $6,300.19 | ▲ +4.89 after sell → book $10,131.01; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,617.03 | ▲ +99.89 after sell → book $10,128.88; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $8,906.94 | ▲ +75.30 after sell → book $10,126.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $10,124.56 | ▲ +18.30 after sell → book $10,124.56; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 75 | $33.36 | $2.21 | — | $7,620.34 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $2531.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 4 | $623.26 | $2.00 | — | $5,125.30 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2531.14 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 19 | $127.43 | $2.05 | — | $2,702.09 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2531.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 72 | $34.89 | $2.21 | — | $187.80 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.6; leftover $2531.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.80 | ▲ close $10,227.79 vs 09:30 $10,141.93 (session +111.70) | 16:00 close · cash $187.80 · equity $10,227.79 vs 09:30 $10,141.93 (+85.86; session marks +111.70) · 4 name(s) marked open→close (per-name table). GMAB×75 09:30 $33.36 → close $33.45 +6.75; DE×4 09:30 $623.26 → close $647.47 +96.84; CF×19 09:30 $127.43 → close $129.60 +41.23; DXYZ×72 09:30 $34.89 → close $34.43 -33.12 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.80 | ▼ 09:30 equity $10,114.47 vs yday $10,227.79 (-113.32) | 09:30 open · cash $187.80 (unchanged overnight, no fees) · equity $10,114.47 vs prior close $10,227.79 (-113.32) · 4 name(s) re-marked at the open (per-name table). GMAB×75 yday $33.45 → 09:30 $32.82 -47.25; DE×4 yday $647.47 → 09:30 $653.04 +22.28; CF×19 yday $129.60 → 09:30 $129.99 +7.41; DXYZ×72 yday $34.43 → 09:30 $33.10 -95.76 | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 75 | $32.82 | $2.25 | $-44.96 | $2,647.05 | ▼ -44.96 after sell → book $10,112.22; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 4 | $653.04 | $2.03 | $+115.09 | $5,257.18 | ▲ +115.09 after sell → book $10,110.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 19 | $129.99 | $2.08 | $+44.52 | $7,724.91 | ▲ +44.52 after sell → book $10,108.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 72 | $33.10 | $2.24 | $-133.32 | $10,105.88 | ▼ -133.32 after sell → book $10,105.88; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,105.88 | ▲ close $10,105.88 vs 09:30 $10,114.47 (session +0.00) | 16:00 close · cash $10,105.88 · no lots left · equity $10,105.88. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,105.88 | ▲ 09:30 equity $10,105.88 vs yday $10,105.88 (-0.00) | 09:30 open · cash $10,105.88 · no holdings · equity $10,105.88 vs prior close $10,105.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 672 | $15.01 | $8.67 | — | $10.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy; ⚪; ret5=+9.4; leftover $10105.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.49 | ▲ close $10,312.25 vs 09:30 $10,105.88 (session +215.04) | 16:00 close · cash $10.49 · equity $10,312.25 vs 09:30 $10,105.88 (+206.37; session marks +215.04) · 1 name(s) marked open→close (per-name table). VALE×672 09:30 $15.01 → close $15.33 +215.04 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.49 | ▲ 09:30 equity $10,339.13 vs yday $10,312.25 (+26.88) | 09:30 open · cash $10.49 (unchanged overnight, no fees) · equity $10,339.13 vs prior close $10,312.25 (+26.88) · 1 name(s) re-marked at the open (per-name table). VALE×672 yday $15.33 → 09:30 $15.37 +26.88 | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 672 | $15.37 | $8.86 | $+224.39 | $10,330.26 | ▲ +224.39 after sell → book $10,330.26; vs 09:30 mark -8.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 615 | $16.77 | $7.93 | — | $8.78 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $10330.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.78 | ▲ close $11,595.38 vs 09:30 $10,339.13 (session +1,273.05) | 16:00 close · cash $8.78 · equity $11,595.38 vs 09:30 $10,339.13 (+1256.25; session marks +1273.05) · 1 name(s) marked open→close (per-name table). BZ×615 09:30 $16.77 → close $18.84 +1273.05 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.78 | ▼ 09:30 equity $11,386.28 vs yday $11,595.38 (-209.10) | 09:30 open · cash $8.78 (unchanged overnight, no fees) · equity $11,386.28 vs prior close $11,595.38 (-209.10) · 1 name(s) re-marked at the open (per-name table). BZ×615 yday $18.84 → 09:30 $18.50 -209.10 | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 615 | $18.50 | $8.13 | $+1047.89 | $11,378.15 | ▲ +1,047.89 after sell → book $11,378.15; vs 09:30 mark -8.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,378.15 | ▲ close $11,378.15 vs 09:30 $11,386.28 (session +0.00) | 16:00 close · cash $11,378.15 · no lots left · equity $11,378.15. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,378.15 | ▲ 09:30 equity $11,378.15 vs yday $11,378.15 (+0.00) | 09:30 open · cash $11,378.15 · no holdings · equity $11,378.15 vs prior close $11,378.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $9,961.33 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1422.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 49 | $28.91 | $2.14 | — | $8,542.61 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $1422.27 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 74 | $19.00 | $2.21 | — | $7,134.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1422.27 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 170 | $8.35 | $2.50 | — | $5,712.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.1; leftover $1422.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 66 | $21.49 | $2.19 | — | $4,291.87 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+8.5; leftover $1422.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,984.06 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+7.8; leftover $1422.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 108 | $13.09 | $2.31 | — | $1,568.03 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+4.2; leftover $1422.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 14 | $98.95 | $2.03 | — | $180.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+9.7; leftover $1422.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.70 | ▼ close $11,250.44 vs 09:30 $11,378.15 (session -110.21) | 16:00 close · cash $180.70 · equity $11,250.44 vs 09:30 $11,378.15 (-127.71; session marks -110.21) · 8 name(s) marked open→close (per-name table). SEDG×43 09:30 $32.90 → close $31.41 -64.07; ZYME×49 09:30 $28.91 → close $28.27 -31.36; TH×74 09:30 $19.00 → close $18.55 -33.30; HAFN×170 09:30 $8.35 → close $8.47 +20.40; S×66 09:30 $21.49 → close $21.54 +3.30; ADSK×5 09:30 $261.16 → close $260.66 -2.50; PD×108 09:30 $13.09 → close $13.83 +79.92; RBRK×14 09:30 $98.95 → close $93.05 -82.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.70 | ▼ 09:30 equity $11,156.95 vs yday $11,250.44 (-93.49) | 09:30 open · cash $180.70 (unchanged overnight, no fees) · equity $11,156.95 vs prior close $11,250.44 (-93.49) · 8 name(s) re-marked at the open (per-name table). SEDG×43 yday $31.41 → 09:30 $31.15 -11.18; ZYME×49 yday $28.27 → 09:30 $28.06 -10.29; TH×74 yday $18.55 → 09:30 $18.12 -31.45; HAFN×170 yday $8.47 → 09:30 $8.53 +10.20; S×66 yday $21.54 → 09:30 $21.45 -5.94; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; PD×108 yday $13.83 → 09:30 $13.58 -27.00; RBRK×14 yday $93.05 → 09:30 $92.83 -3.08 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 43 | $31.15 | $2.14 | $-79.51 | $1,518.01 | ▼ -79.51 after sell → book $11,154.81; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 49 | $28.06 | $2.16 | $-45.94 | $2,890.79 | ▼ -45.94 after sell → book $11,152.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 74 | $18.12 | $2.23 | $-69.20 | $4,229.80 | ▼ -69.20 after sell → book $11,150.41; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 170 | $8.53 | $2.54 | $+25.56 | $5,677.36 | ▲ +25.56 after sell → book $11,147.87; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 66 | $21.45 | $2.21 | $-7.04 | $7,090.85 | ▼ -7.04 after sell → book $11,145.66; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $8,377.38 | ▼ -21.28 after sell → book $11,143.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 108 | $13.58 | $2.34 | $+48.26 | $9,841.68 | ▲ +48.26 after sell → book $11,141.30; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 14 | $92.83 | $2.05 | $-89.76 | $11,139.24 | ▼ -89.76 after sell → book $11,139.24; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,156.95 (session +0.00) | 16:00 close · cash $11,139.24 · no lots left · equity $11,139.24. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | 09:30 open · cash $11,139.24 · no holdings · equity $11,139.24 vs prior close $11,139.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,139.24 (session +0.00) | 16:00 close · cash $11,139.24 · no lots left · equity $11,139.24. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | 09:30 open · cash $11,139.24 · no holdings · equity $11,139.24 vs prior close $11,139.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,139.24 (session +0.00) | 16:00 close · cash $11,139.24 · no lots left · equity $11,139.24. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | 09:30 open · cash $11,139.24 · no holdings · equity $11,139.24 vs prior close $11,139.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $8,705.69 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $2784.81 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 346 | $8.03 | $4.46 | — | $5,922.84 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $2784.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 184 | $15.09 | $2.54 | — | $3,143.74 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+6.1; leftover $2784.81 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 52 | $52.88 | $2.15 | — | $391.84 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2784.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.84 | ▲ close $11,281.67 vs 09:30 $11,139.24 (session +153.58) | 16:00 close · cash $391.84 · equity $11,281.67 vs 09:30 $11,139.24 (+142.43; session marks +153.58) · 4 name(s) marked open→close (per-name table). DELL×5 09:30 $486.31 → close $516.39 +150.40; VSTM×346 09:30 $8.03 → close $7.98 -17.30; MEI×184 09:30 $15.09 → close $15.32 +42.32; ATRC×52 09:30 $52.88 → close $52.46 -21.84 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.84 | ▼ 09:30 equity $11,225.72 vs yday $11,281.67 (-55.95) | 09:30 open · cash $391.84 (unchanged overnight, no fees) · equity $11,225.72 vs prior close $11,281.67 (-55.95) · 4 name(s) re-marked at the open (per-name table). DELL×5 yday $516.39 → 09:30 $513.78 -13.05; VSTM×346 yday $7.98 → 09:30 $7.91 -24.22; MEI×184 yday $15.32 → 09:30 $15.34 +3.68; ATRC×52 yday $52.46 → 09:30 $52.03 -22.36 | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 346 | $7.91 | $4.54 | $-50.53 | $3,124.15 | ▼ -50.53 after sell → book $11,221.17; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 184 | $15.34 | $2.60 | $+40.86 | $5,944.12 | ▲ +40.86 after sell → book $11,218.58; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 52 | $52.03 | $2.18 | $-48.52 | $8,647.50 | ▼ -48.52 after sell → book $11,216.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $7,467.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1235.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $6,255.24 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1235.36 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $5,154.42 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1235.36 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `KYIV` | 86 | $14.24 | $2.25 | — | $3,927.53 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ⚪; ret5=+5.2; leftover $1235.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $2,752.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1235.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 35 | $34.69 | $2.10 | — | $1,536.43 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $1235.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 37 | $32.65 | $2.10 | — | $326.28 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1235.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $326.28 | ▲ close $11,319.37 vs 09:30 $11,225.72 (session +117.50) | 16:00 close · cash $326.28 · equity $11,319.37 vs 09:30 $11,225.72 (+93.65; session marks +117.50) · 8 name(s) marked open→close (per-name table). DELL×5 09:30 $513.78 → close $524.14 +51.80; LULU×12 09:30 $98.15 → close $100.61 +29.52; MRX×16 09:30 $75.65 → close $78.27 +41.92; MSTR×8 09:30 $137.35 → close $142.80 +43.60; KYIV×86 09:30 $14.24 → close $14.11 -11.18; GWRE×7 09:30 $167.55 → close $162.42 -35.91; BLSH×35 09:30 $34.69 → close $36.00 +45.85; ZETA×37 09:30 $32.65 → close $31.35 -48.10 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $326.28 | ▼ 09:30 equity $11,247.53 vs yday $11,319.37 (-71.84) | 09:30 open · cash $326.28 (unchanged overnight, no fees) · equity $11,247.53 vs prior close $11,319.37 (-71.84) · 8 name(s) re-marked at the open (per-name table). DELL×5 yday $524.14 → 09:30 $521.15 -14.95; LULU×12 yday $100.61 → 09:30 $100.58 -0.36; MRX×16 yday $78.27 → 09:30 $78.84 +9.12; MSTR×8 yday $142.80 → 09:30 $137.62 -41.44; KYIV×86 yday $14.11 → 09:30 $14.14 +2.58; GWRE×7 yday $162.42 → 09:30 $160.52 -13.30; BLSH×35 yday $36.00 → 09:30 $35.90 -3.50; ZETA×37 yday $31.35 → 09:30 $31.08 -9.99 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 5 | $521.15 | $2.04 | $+170.16 | $2,929.99 | ▲ +170.16 after sell → book $11,245.49; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 12 | $100.58 | $2.05 | $+25.09 | $4,134.91 | ▲ +25.09 after sell → book $11,243.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 16 | $78.84 | $2.06 | $+46.94 | $5,394.29 | ▲ +46.94 after sell → book $11,241.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $6,493.21 | ▼ -1.89 after sell → book $11,239.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `KYIV` | 86 | $14.14 | $2.27 | $-13.12 | $7,706.98 | ▼ -13.12 after sell → book $11,237.08; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $8,828.59 | ▼ -53.25 after sell → book $11,235.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 35 | $35.90 | $2.12 | $+38.14 | $10,082.98 | ▲ +38.14 after sell → book $11,232.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 37 | $31.08 | $2.12 | $-62.31 | $11,230.81 | ▼ -62.31 after sell → book $11,230.81; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,247.53 (session +0.00) | 16:00 close · cash $11,230.81 · no lots left · equity $11,230.81. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | 09:30 open · cash $11,230.81 · no holdings · equity $11,230.81 vs prior close $11,230.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,230.81 (session +0.00) | 16:00 close · cash $11,230.81 · no lots left · equity $11,230.81. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | 09:30 open · cash $11,230.81 · no holdings · equity $11,230.81 vs prior close $11,230.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,230.81 (session +0.00) | 16:00 close · cash $11,230.81 · no lots left · equity $11,230.81. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | 09:30 open · cash $11,230.81 · no holdings · equity $11,230.81 vs prior close $11,230.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 18 | $77.33 | $2.04 | — | $9,836.83 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=+2.5; leftover $1403.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 66 | $21.21 | $2.19 | — | $8,434.78 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1403.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 66 | $21.04 | $2.19 | — | $7,043.95 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.5; leftover $1403.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 20 | $69.88 | $2.05 | — | $5,644.30 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.0; leftover $1403.85 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PUMP` | 121 | $11.57 | $2.35 | — | $4,241.98 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.9; leftover $1403.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 138 | $10.11 | $2.40 | — | $2,844.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1403.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 26 | $52.55 | $2.07 | — | $1,476.03 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1403.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 121 | $11.55 | $2.35 | — | $76.13 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1403.85 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.13 | ▲ close $11,359.85 vs 09:30 $11,230.81 (session +146.68) | 16:00 close · cash $76.13 · equity $11,359.85 vs 09:30 $11,230.81 (+129.04; session marks +146.68) · 8 name(s) marked open→close (per-name table). VIST×18 09:30 $77.33 → close $76.27 -19.08; PBR×66 09:30 $21.21 → close $21.20 -0.66; GME×66 09:30 $21.04 → close $21.15 +7.26; INSP×20 09:30 $69.88 → close $73.00 +62.40; PUMP×121 09:30 $11.57 → close $11.45 -14.52; PAGS×138 09:30 $10.11 → close $10.12 +1.38; BAND×26 09:30 $52.55 → close $56.87 +112.32; FUBO×121 09:30 $11.55 → close $11.53 -2.42 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.13 | ▼ 09:30 equity $11,303.64 vs yday $11,359.85 (-56.21) | 09:30 open · cash $76.13 (unchanged overnight, no fees) · equity $11,303.64 vs prior close $11,359.85 (-56.21) · 8 name(s) re-marked at the open (per-name table). VIST×18 yday $76.27 → 09:30 $77.10 +14.94; PBR×66 yday $21.20 → 09:30 $21.23 +1.98; GME×66 yday $21.15 → 09:30 $21.00 -9.90; INSP×20 yday $73.00 → 09:30 $72.14 -17.20; PUMP×121 yday $11.45 → 09:30 $11.17 -33.88; PAGS×138 yday $10.12 → 09:30 $10.00 -16.56; BAND×26 yday $56.87 → 09:30 $56.90 +0.78; FUBO×121 yday $11.53 → 09:30 $11.56 +3.63 | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 18 | $77.10 | $2.07 | $-8.25 | $1,461.86 | ▼ -8.25 after sell → book $11,301.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 66 | $21.23 | $2.21 | $-3.08 | $2,860.83 | ▼ -3.08 after sell → book $11,299.36; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 20 | $72.14 | $2.07 | $+41.08 | $4,301.56 | ▲ +41.08 after sell → book $11,297.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PUMP` | 121 | $11.17 | $2.38 | $-53.14 | $5,650.75 | ▼ -53.14 after sell → book $11,294.91; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 138 | $10.00 | $2.44 | $-20.02 | $7,028.31 | ▼ -20.02 after sell → book $11,292.47; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 26 | $56.90 | $2.09 | $+108.94 | $8,505.62 | ▲ +108.94 after sell → book $11,290.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 121 | $11.56 | $2.38 | $-3.53 | $9,901.99 | ▼ -3.53 after sell → book $11,287.99; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,901.99 | ▲ close $11,328.91 vs 09:30 $11,303.64 (session +40.92) | 16:00 close · cash $9,901.99 · equity $11,328.91 vs 09:30 $11,303.64 (+25.27; session marks +40.92) · 1 name(s) marked open→close (per-name table). GME×66 09:30 $21.00 → close $21.62 +40.92 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,901.99 | ▼ 09:30 equity $11,321.65 vs yday $11,328.91 (-7.26) | 09:30 open · cash $9,901.99 (unchanged overnight, no fees) · equity $11,321.65 vs prior close $11,328.91 (-7.26) · 1 name(s) re-marked at the open (per-name table). GME×66 yday $21.62 → 09:30 $21.51 -7.26 | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 66 | $21.51 | $2.21 | $+26.62 | $11,319.44 | ▲ +26.62 after sell → book $11,319.44; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,319.44 | ▲ close $11,319.44 vs 09:30 $11,321.65 (session +0.00) | 16:00 close · cash $11,319.44 · no lots left · equity $11,319.44. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,319.44 | ▲ 09:30 equity $11,319.44 vs yday $11,319.44 (+0.00) | 09:30 open · cash $11,319.44 · no holdings · equity $11,319.44 vs prior close $11,319.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 79 | $17.87 | $2.23 | — | $9,905.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+6.8; leftover $1414.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $8,505.05 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1414.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 241 | $5.87 | $3.11 | — | $7,087.27 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1414.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 6 | $224.49 | $2.01 | — | $5,738.32 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+5.3; leftover $1414.93 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 52 | $27.09 | $2.15 | — | $4,327.50 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1414.93 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 16 | $87.52 | $2.04 | — | $2,925.14 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.3; leftover $1414.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 20 | $68.79 | $2.05 | — | $1,547.29 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1414.93 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 35 | $39.99 | $2.10 | — | $145.54 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.3; leftover $1414.93 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.54 | ▼ close $11,120.60 vs 09:30 $11,319.44 (session -181.13) | 16:00 close · cash $145.54 · equity $11,120.60 vs 09:30 $11,319.44 (-198.84; session marks -181.13) · 8 name(s) marked open→close (per-name table). TALO×79 09:30 $17.87 → close $17.42 -35.55; VAL×16 09:30 $87.40 → close $82.52 -78.08; RIG×241 09:30 $5.87 → close $5.54 -79.53; ILMN×6 09:30 $224.49 → close $228.93 +26.64; ADPT×52 09:30 $27.09 → close $27.67 +30.16; MRCY×16 09:30 $87.52 → close $87.25 -4.32; TEM×20 09:30 $68.79 → close $69.97 +23.60; SM×35 09:30 $39.99 → close $38.16 -64.05 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.54 | ▲ 09:30 equity $11,247.86 vs yday $11,120.60 (+127.26) | 09:30 open · cash $145.54 (unchanged overnight, no fees) · equity $11,247.86 vs prior close $11,120.60 (+127.26) · 8 name(s) re-marked at the open (per-name table). TALO×79 yday $17.42 → 09:30 $17.19 -18.17; VAL×16 yday $82.52 → 09:30 $83.20 +10.88; RIG×241 yday $5.54 → 09:30 $5.58 +9.64; ILMN×6 yday $228.93 → 09:30 $233.85 +29.52; ADPT×52 yday $27.67 → 09:30 $28.23 +29.12; MRCY×16 yday $87.25 → 09:30 $89.27 +32.32; TEM×20 yday $69.97 → 09:30 $72.70 +54.60; SM×35 yday $38.16 → 09:30 $37.57 -20.65 | — |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 79 | $17.19 | $2.25 | $-58.20 | $1,501.30 | ▼ -58.20 after sell → book $11,245.61; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $2,830.44 | ▼ -71.30 after sell → book $11,243.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 241 | $5.58 | $3.16 | $-76.16 | $4,172.06 | ▼ -76.16 after sell → book $11,240.39; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 6 | $233.85 | $2.03 | $+52.12 | $5,573.13 | ▲ +52.12 after sell → book $11,238.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 52 | $28.23 | $2.17 | $+54.97 | $7,038.93 | ▲ +54.97 after sell → book $11,236.20; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 16 | $89.27 | $2.06 | $+23.90 | $8,465.19 | ▲ +23.90 after sell → book $11,234.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 20 | $72.70 | $2.07 | $+74.08 | $9,917.12 | ▲ +74.08 after sell → book $11,232.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 35 | $37.57 | $2.12 | $-88.91 | $11,229.95 | ▼ -88.91 after sell → book $11,229.95; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 72 | $25.95 | $2.21 | — | $9,359.34 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1871.66 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 100 | $18.56 | $2.29 | — | $7,501.05 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1871.66 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 246 | $7.59 | $3.17 | — | $5,630.74 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1871.66 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 9 | $196.50 | $2.02 | — | $3,860.22 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $1871.66 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 10 | $170.85 | $2.02 | — | $2,149.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1871.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FOSL` | 342 | $5.46 | $4.41 | — | $277.97 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1871.66 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.97 | ▲ close $11,410.10 vs 09:30 $11,247.86 (session +196.27) | 16:00 close · cash $277.97 · equity $11,410.10 vs 09:30 $11,247.86 (+162.24; session marks +196.27) · 6 name(s) marked open→close (per-name table). ARQT×72 09:30 $25.95 → close $26.46 +36.72; AMRX×100 09:30 $18.56 → close $18.28 -28.00; PGEN×246 09:30 $7.59 → close $7.87 +68.88; FTAI×9 09:30 $196.50 → close $195.07 -12.87; SMTC×10 09:30 $170.85 → close $178.19 +73.40; FOSL×342 09:30 $5.46 → close $5.63 +58.14 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.97 | ▲ 09:30 equity $11,443.84 vs yday $11,410.10 (+33.74) | 09:30 open · cash $277.97 (unchanged overnight, no fees) · equity $11,443.84 vs prior close $11,410.10 (+33.74) · 6 name(s) re-marked at the open (per-name table). ARQT×72 yday $26.46 → 09:30 $26.14 -23.04; AMRX×100 yday $18.28 → 09:30 $18.12 -16.00; PGEN×246 yday $7.87 → 09:30 $7.98 +27.06; FTAI×9 yday $195.07 → 09:30 $195.55 +4.32; SMTC×10 yday $178.19 → 09:30 $182.33 +41.40; FOSL×342 yday $5.63 → 09:30 $5.63 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 72 | $26.14 | $2.23 | $+9.24 | $2,157.82 | ▲ +9.24 after sell → book $11,441.61; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 100 | $18.12 | $2.32 | $-48.61 | $3,967.50 | ▼ -48.61 after sell → book $11,439.29; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 246 | $7.98 | $3.23 | $+89.54 | $5,927.35 | ▲ +89.54 after sell → book $11,436.06; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 9 | $195.55 | $2.04 | $-12.61 | $7,685.26 | ▼ -12.61 after sell → book $11,434.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 10 | $182.33 | $2.04 | $+110.74 | $9,506.51 | ▲ +110.74 after sell → book $11,431.97; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FOSL` | 342 | $5.63 | $4.48 | $+49.24 | $11,427.49 | ▲ +49.24 after sell → book $11,427.49; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 110 | $14.79 | $2.32 | — | $9,798.27 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 116 | $14.07 | $2.34 | — | $8,163.81 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 78 | $20.91 | $2.22 | — | $6,530.61 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 36 | $44.70 | $2.10 | — | $4,919.31 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.5; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $3,822.57 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 213 | $7.64 | $2.75 | — | $2,192.50 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 280 | $5.83 | $3.61 | — | $556.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1632.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $556.49 | ▼ close $11,306.21 vs 09:30 $11,443.84 (session -103.94) | 16:00 close · cash $556.49 · equity $11,306.21 vs 09:30 $11,443.84 (-137.63; session marks -103.94) · 7 name(s) marked open→close (per-name table). RARE×110 09:30 $14.79 → close $14.51 -30.80; BHVN×116 09:30 $14.07 → close $13.62 -52.20; TH×78 09:30 $20.91 → close $21.19 +21.84; SYM×36 09:30 $44.70 → close $41.89 -101.16; AMD×2 09:30 $547.37 → close $559.82 +24.90; SHLS×213 09:30 $7.64 → close $7.60 -8.52; BNC×280 09:30 $5.83 → close $5.98 +42.00 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $556.49 | ▲ 09:30 equity $11,594.70 vs yday $11,306.21 (+288.49) | 09:30 open · cash $556.49 (unchanged overnight, no fees) · equity $11,594.70 vs prior close $11,306.21 (+288.49) · 7 name(s) re-marked at the open (per-name table). RARE×110 yday $14.51 → 09:30 $14.58 +7.70; BHVN×116 yday $13.62 → 09:30 $13.90 +32.48; TH×78 yday $21.19 → 09:30 $21.65 +35.88; SYM×36 yday $41.89 → 09:30 $42.42 +19.08; AMD×2 yday $559.82 → 09:30 $583.88 +48.12; SHLS×213 yday $7.60 → 09:30 $7.71 +23.43; BNC×280 yday $5.98 → 09:30 $6.42 +121.80 | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 110 | $14.58 | $2.35 | $-27.77 | $2,157.94 | ▼ -27.77 after sell → book $11,592.35; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 116 | $13.90 | $2.37 | $-24.43 | $3,767.97 | ▼ -24.43 after sell → book $11,589.98; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 78 | $21.65 | $2.25 | $+53.25 | $5,454.42 | ▲ +53.25 after sell → book $11,587.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 36 | $42.42 | $2.12 | $-86.30 | $6,979.42 | ▼ -86.30 after sell → book $11,585.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 2 | $583.88 | $2.02 | $+69.01 | $8,145.16 | ▲ +69.01 after sell → book $11,583.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 213 | $7.71 | $2.80 | $+9.37 | $9,784.60 | ▲ +9.37 after sell → book $11,580.80; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 280 | $6.42 | $3.67 | $+156.52 | $11,577.13 | ▲ +156.52 after sell → book $11,577.13; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,594.70 (session +0.00) | 16:00 close · cash $11,577.13 · no lots left · equity $11,577.13. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | 09:30 open · cash $11,577.13 · no holdings · equity $11,577.13 vs prior close $11,577.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | 16:00 close · cash $11,577.13 · no lots left · equity $11,577.13. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | 09:30 open · cash $11,577.13 · no holdings · equity $11,577.13 vs prior close $11,577.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | 16:00 close · cash $11,577.13 · no lots left · equity $11,577.13. | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | 09:30 open · cash $11,577.13 · no holdings · equity $11,577.13 vs prior close $11,577.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | 16:00 close · cash $11,577.13 · no lots left · equity $11,577.13. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
