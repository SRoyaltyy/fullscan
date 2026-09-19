# Factor mine action — `union_clk_mom_break_peer_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #1 ∩ Theme Radar T−1 oppset

Cash book **+13.82%** ($11,382) · signal-only (no cash/fees) was +20.68%. Starts YES **19/26**. Fills 142 · skips 58 · realized $+1442.19.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $411.06.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ZIM` | 73 | — | $27.25 | +0.00 | $28.14 | +64.97 | +64.97 | +0.00 | +64.97 |
| 2026-08-14 | `ENS` | 10 | — | $196.00 | +0.00 | $203.40 | +74.00 | +74.00 | +0.00 | +74.00 |
| 2026-08-14 | `YSS` | 198 | — | $10.06 | +0.00 | $10.93 | +172.26 | +172.26 | +0.00 | +172.26 |
| 2026-08-14 | `ADUR` | 121 | — | $16.50 | +0.00 | $16.17 | -39.93 | -39.93 | +0.00 | -39.93 |
| 2026-08-14 | `WDC` | 3 | — | $503.50 | +0.00 | $508.80 | +15.90 | +15.90 | +0.00 | +15.90 |
| 2026-08-17 | `ZIM` | 73 | $28.14 | $28.83 | +50.37 | — | +0.00 | +50.37 | +115.34 | — |
| 2026-08-17 | `ENS` | 10 | $203.40 | $205.03 | +16.30 | — | +0.00 | +16.30 | +90.30 | — |
| 2026-08-17 | `YSS` | 198 | $10.93 | $10.36 | -112.86 | — | +0.00 | -112.86 | +59.40 | — |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | — | +0.00 | -53.24 | -93.17 | — |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | — | +0.00 | +50.19 | +66.09 | — |
| 2026-08-17 | `WBS` | 64 | — | $79.00 | +0.00 | $78.69 | -19.84 | -19.84 | +0.00 | -19.84 |
| 2026-08-17 | `ALM` | 315 | — | $16.20 | +0.00 | $16.36 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-18 | `WBS` | 64 | $78.69 | $78.52 | -10.88 | — | +0.00 | -10.88 | -30.72 | — |
| 2026-08-18 | `ALM` | 315 | $16.36 | $15.78 | -182.70 | — | +0.00 | -182.70 | -132.30 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `SUI` | 10 | — | $121.21 | +0.00 | $122.29 | +10.80 | +10.80 | +0.00 | +10.80 |
| 2026-08-20 | `NOMD` | 105 | — | $11.84 | +0.00 | $11.61 | -24.15 | -24.15 | +0.00 | -24.15 |
| 2026-08-20 | `ZIM` | 45 | — | $27.45 | +0.00 | $27.16 | -13.05 | -13.05 | +0.00 | -13.05 |
| 2026-08-20 | `MSTR` | 11 | — | $113.23 | +0.00 | $112.39 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-08-20 | `DNA` | 168 | — | $7.45 | +0.00 | $6.96 | -82.32 | -82.32 | +0.00 | -82.32 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `ATAT` | 36 | — | $34.05 | +0.00 | $34.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-21 | `SUI` | 10 | $122.29 | $122.41 | +1.20 | — | +0.00 | +1.20 | +12.00 | — |
| 2026-08-21 | `NOMD` | 105 | $11.61 | $11.71 | +10.50 | — | +0.00 | +10.50 | -13.65 | — |
| 2026-08-21 | `ZIM` | 45 | $27.16 | $27.50 | +15.30 | — | +0.00 | +15.30 | +2.25 | — |
| 2026-08-21 | `MSTR` | 11 | $112.39 | $119.69 | +80.30 | — | +0.00 | +80.30 | +71.06 | — |
| 2026-08-21 | `DNA` | 168 | $6.96 | $7.09 | +21.84 | — | +0.00 | +21.84 | -60.48 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `ATAT` | 36 | $34.25 | $34.31 | +2.16 | — | +0.00 | +2.16 | +9.36 | — |
| 2026-08-21 | `VIRT` | 33 | — | $60.66 | +0.00 | $67.93 | +239.91 | +239.91 | +0.00 | +239.91 |
| 2026-08-21 | `GMAB` | 61 | — | $33.36 | +0.00 | $33.45 | +5.49 | +5.49 | +0.00 | +5.49 |
| 2026-08-21 | `DE` | 3 | — | $623.26 | +0.00 | $647.47 | +72.63 | +72.63 | +0.00 | +72.63 |
| 2026-08-21 | `CF` | 15 | — | $127.43 | +0.00 | $129.60 | +32.55 | +32.55 | +0.00 | +32.55 |
| 2026-08-21 | `DXYZ` | 58 | — | $34.89 | +0.00 | $34.43 | -26.68 | -26.68 | +0.00 | -26.68 |
| 2026-08-24 | `VIRT` | 33 | $67.93 | $66.80 | -37.29 | — | +0.00 | -37.29 | +202.62 | — |
| 2026-08-24 | `GMAB` | 61 | $33.45 | $32.82 | -38.43 | — | +0.00 | -38.43 | -32.94 | — |
| 2026-08-24 | `DE` | 3 | $647.47 | $653.04 | +16.71 | — | +0.00 | +16.71 | +89.34 | — |
| 2026-08-24 | `CF` | 15 | $129.60 | $129.99 | +5.85 | — | +0.00 | +5.85 | +38.40 | — |
| 2026-08-24 | `DXYZ` | 58 | $34.43 | $33.10 | -77.14 | — | +0.00 | -77.14 | -103.82 | — |
| 2026-08-25 | `DBRG` | 216 | — | $15.98 | +0.00 | $15.97 | -2.16 | -2.16 | +0.00 | -2.16 |
| 2026-08-25 | `VALE` | 229 | — | $15.01 | +0.00 | $15.33 | +73.28 | +73.28 | +0.00 | +73.28 |
| 2026-08-26 | `DBRG` | 216 | $15.97 | $15.97 | +0.00 | — | +0.00 | +0.00 | -2.16 | — |
| 2026-08-26 | `VALE` | 229 | $15.33 | $15.37 | +9.16 | — | +0.00 | +9.16 | +82.44 | — |
| 2026-08-26 | `BZ` | 621 | — | $16.77 | +0.00 | $18.84 | +1285.47 | +1285.47 | +0.00 | +1285.47 |
| 2026-08-27 | `BZ` | 621 | $18.84 | $18.50 | -211.14 | — | +0.00 | -211.14 | +1074.33 | — |
| 2026-08-28 | `EDU` | 24 | — | $57.63 | +0.00 | $58.83 | +28.80 | +28.80 | +0.00 | +28.80 |
| 2026-08-28 | `SEDG` | 43 | — | $32.90 | +0.00 | $31.41 | -64.07 | -64.07 | +0.00 | -64.07 |
| 2026-08-28 | `ZYME` | 49 | — | $28.91 | +0.00 | $28.27 | -31.36 | -31.36 | +0.00 | -31.36 |
| 2026-08-28 | `TH` | 75 | — | $19.00 | +0.00 | $18.55 | -33.75 | -33.75 | +0.00 | -33.75 |
| 2026-08-28 | `HAFN` | 171 | — | $8.35 | +0.00 | $8.47 | +20.52 | +20.52 | +0.00 | +20.52 |
| 2026-08-28 | `S` | 66 | — | $21.49 | +0.00 | $21.54 | +3.30 | +3.30 | +0.00 | +3.30 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `PD` | 109 | — | $13.09 | +0.00 | $13.83 | +80.66 | +80.66 | +0.00 | +80.66 |
| 2026-08-31 | `EDU` | 24 | $58.83 | $58.23 | -14.40 | — | +0.00 | -14.40 | +14.40 | — |
| 2026-08-31 | `SEDG` | 43 | $31.41 | $31.15 | -11.18 | — | +0.00 | -11.18 | -75.25 | — |
| 2026-08-31 | `ZYME` | 49 | $28.27 | $28.06 | -10.29 | — | +0.00 | -10.29 | -41.65 | — |
| 2026-08-31 | `TH` | 75 | $18.55 | $18.12 | -31.88 | — | +0.00 | -31.88 | -65.62 | — |
| 2026-08-31 | `HAFN` | 171 | $8.47 | $8.53 | +10.26 | — | +0.00 | +10.26 | +30.78 | — |
| 2026-08-31 | `S` | 66 | $21.54 | $21.45 | -5.94 | — | +0.00 | -5.94 | -2.64 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `PD` | 109 | $13.83 | $13.58 | -27.25 | — | +0.00 | -27.25 | +53.41 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GBTG` | 170 | — | $9.49 | +0.00 | $9.49 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `SBS` | 311 | — | $5.21 | +0.00 | $5.13 | -24.88 | -24.88 | +0.00 | -24.88 |
| 2026-09-03 | `ETD` | 74 | — | $21.82 | +0.00 | $21.87 | +3.70 | +3.70 | +0.00 | +3.70 |
| 2026-09-03 | `VSTM` | 201 | — | $8.03 | +0.00 | $7.98 | -10.05 | -10.05 | +0.00 | -10.05 |
| 2026-09-03 | `MEI` | 107 | — | $15.09 | +0.00 | $15.32 | +24.61 | +24.61 | +0.00 | +24.61 |
| 2026-09-03 | `ATRC` | 30 | — | $52.88 | +0.00 | $52.46 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-04 | `GBTG` | 170 | $9.49 | $9.49 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | $524.14 | +31.08 | +23.25 | +82.41 | +113.49 |
| 2026-09-04 | `SBS` | 311 | $5.13 | $5.02 | -34.21 | — | +0.00 | -34.21 | -59.09 | — |
| 2026-09-04 | `ETD` | 74 | $21.87 | $21.84 | -2.22 | — | +0.00 | -2.22 | +1.48 | — |
| 2026-09-04 | `VSTM` | 201 | $7.98 | $7.91 | -14.07 | — | +0.00 | -14.07 | -24.12 | — |
| 2026-09-04 | `MEI` | 107 | $15.32 | $15.34 | +2.14 | — | +0.00 | +2.14 | +26.75 | — |
| 2026-09-04 | `ATRC` | 30 | $52.46 | $52.03 | -12.90 | — | +0.00 | -12.90 | -25.50 | — |
| 2026-09-04 | `LULU` | 14 | — | $98.15 | +0.00 | $100.61 | +34.44 | +34.44 | +0.00 | +34.44 |
| 2026-09-04 | `HPE` | 25 | — | $53.85 | +0.00 | $52.00 | -46.25 | -46.25 | +0.00 | -46.25 |
| 2026-09-04 | `BULL` | 142 | — | $9.79 | +0.00 | $9.74 | -7.10 | -7.10 | +0.00 | -7.10 |
| 2026-09-04 | `MRX` | 18 | — | $75.65 | +0.00 | $78.27 | +47.16 | +47.16 | +0.00 | +47.16 |
| 2026-09-04 | `MSTR` | 10 | — | $137.35 | +0.00 | $142.80 | +54.50 | +54.50 | +0.00 | +54.50 |
| 2026-09-04 | `KYIV` | 98 | — | $14.24 | +0.00 | $14.11 | -12.74 | -12.74 | +0.00 | -12.74 |
| 2026-09-04 | `GWRE` | 8 | — | $167.55 | +0.00 | $162.42 | -41.04 | -41.04 | +0.00 | -41.04 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | — | +0.00 | -8.97 | +104.52 | — |
| 2026-09-08 | `LULU` | 14 | $100.61 | $100.58 | -0.42 | — | +0.00 | -0.42 | +34.02 | — |
| 2026-09-08 | `HPE` | 25 | $52.00 | $52.29 | +7.25 | — | +0.00 | +7.25 | -39.00 | — |
| 2026-09-08 | `BULL` | 142 | $9.74 | $9.94 | +27.69 | — | +0.00 | +27.69 | +20.59 | — |
| 2026-09-08 | `MRX` | 18 | $78.27 | $78.84 | +10.26 | — | +0.00 | +10.26 | +57.42 | — |
| 2026-09-08 | `MSTR` | 10 | $142.80 | $137.62 | -51.80 | — | +0.00 | -51.80 | +2.70 | — |
| 2026-09-08 | `KYIV` | 98 | $14.11 | $14.14 | +2.94 | — | +0.00 | +2.94 | -9.80 | — |
| 2026-09-08 | `GWRE` | 8 | $162.42 | $160.52 | -15.20 | — | +0.00 | -15.20 | -56.24 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AVAV` | 9 | — | $145.91 | +0.00 | $146.71 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-09-11 | `SEDG` | 38 | — | $36.78 | +0.00 | $34.68 | -79.80 | -79.80 | +0.00 | -79.80 |
| 2026-09-11 | `OBE` | 112 | — | $12.55 | +0.00 | $12.97 | +47.04 | +47.04 | +0.00 | +47.04 |
| 2026-09-11 | `VIST` | 18 | — | $77.33 | +0.00 | $76.27 | -19.08 | -19.08 | +0.00 | -19.08 |
| 2026-09-11 | `PBR` | 66 | — | $21.21 | +0.00 | $21.20 | -0.66 | -0.66 | +0.00 | -0.66 |
| 2026-09-11 | `GME` | 67 | — | $21.04 | +0.00 | $21.15 | +7.37 | +7.37 | +0.00 | +7.37 |
| 2026-09-11 | `INSP` | 20 | — | $69.88 | +0.00 | $73.00 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-09-11 | `PAGS` | 139 | — | $10.11 | +0.00 | $10.12 | +1.39 | +1.39 | +0.00 | +1.39 |
| 2026-09-14 | `AVAV` | 9 | $146.71 | $145.80 | -8.19 | — | +0.00 | -8.19 | -0.99 | — |
| 2026-09-14 | `SEDG` | 38 | $34.68 | $33.64 | -39.52 | — | +0.00 | -39.52 | -119.32 | — |
| 2026-09-14 | `OBE` | 112 | $12.97 | $13.57 | +67.20 | — | +0.00 | +67.20 | +114.24 | — |
| 2026-09-14 | `VIST` | 18 | $76.27 | $77.10 | +14.94 | — | +0.00 | +14.94 | -4.14 | — |
| 2026-09-14 | `PBR` | 66 | $21.20 | $21.23 | +1.98 | — | +0.00 | +1.98 | +1.32 | — |
| 2026-09-14 | `GME` | 67 | $21.15 | $21.00 | -10.05 | $21.62 | +41.54 | +31.49 | -2.68 | +38.86 |
| 2026-09-14 | `INSP` | 20 | $73.00 | $72.14 | -17.20 | — | +0.00 | -17.20 | +45.20 | — |
| 2026-09-14 | `PAGS` | 139 | $10.12 | $10.00 | -16.68 | — | +0.00 | -16.68 | -15.29 | — |
| 2026-09-15 | `GME` | 67 | $21.62 | $21.51 | -7.37 | — | +0.00 | -7.37 | +31.49 | — |
| 2026-09-16 | `SRRK` | 28 | — | $50.01 | +0.00 | $49.37 | -17.92 | -17.92 | +0.00 | -17.92 |
| 2026-09-16 | `MEOH` | 22 | — | $63.34 | +0.00 | $61.51 | -40.26 | -40.26 | +0.00 | -40.26 |
| 2026-09-16 | `VAL` | 16 | — | $87.40 | +0.00 | $82.52 | -78.08 | -78.08 | +0.00 | -78.08 |
| 2026-09-16 | `RIG` | 241 | — | $5.87 | +0.00 | $5.54 | -79.53 | -79.53 | +0.00 | -79.53 |
| 2026-09-16 | `ILMN` | 6 | — | $224.49 | +0.00 | $228.93 | +26.64 | +26.64 | +0.00 | +26.64 |
| 2026-09-16 | `ADPT` | 52 | — | $27.09 | +0.00 | $27.67 | +30.16 | +30.16 | +0.00 | +30.16 |
| 2026-09-16 | `MRCY` | 16 | — | $87.52 | +0.00 | $87.25 | -4.32 | -4.32 | +0.00 | -4.32 |
| 2026-09-16 | `TEM` | 20 | — | $68.79 | +0.00 | $69.97 | +23.60 | +23.60 | +0.00 | +23.60 |
| 2026-09-17 | `SRRK` | 28 | $49.37 | $49.52 | +4.20 | $49.02 | -14.00 | -9.80 | -13.72 | -27.72 |
| 2026-09-17 | `MEOH` | 22 | $61.51 | $60.83 | -14.96 | — | +0.00 | -14.96 | -55.22 | — |
| 2026-09-17 | `VAL` | 16 | $82.52 | $83.20 | +10.88 | — | +0.00 | +10.88 | -67.20 | — |
| 2026-09-17 | `RIG` | 241 | $5.54 | $5.58 | +9.64 | — | +0.00 | +9.64 | -69.89 | — |
| 2026-09-17 | `ILMN` | 6 | $228.93 | $233.85 | +29.52 | — | +0.00 | +29.52 | +56.16 | — |
| 2026-09-17 | `ADPT` | 52 | $27.67 | $28.23 | +29.12 | — | +0.00 | +29.12 | +59.28 | — |
| 2026-09-17 | `MRCY` | 16 | $87.25 | $89.27 | +32.32 | — | +0.00 | +32.32 | +28.00 | — |
| 2026-09-17 | `TEM` | 20 | $69.97 | $72.70 | +54.60 | — | +0.00 | +54.60 | +78.20 | — |
| 2026-09-17 | `ARQT` | 63 | — | $25.95 | +0.00 | $26.46 | +32.13 | +32.13 | +0.00 | +32.13 |
| 2026-09-17 | `AMRX` | 89 | — | $18.56 | +0.00 | $18.28 | -24.92 | -24.92 | +0.00 | -24.92 |
| 2026-09-17 | `PGEN` | 217 | — | $7.59 | +0.00 | $7.87 | +60.76 | +60.76 | +0.00 | +60.76 |
| 2026-09-17 | `FTAI` | 8 | — | $196.50 | +0.00 | $195.07 | -11.44 | -11.44 | +0.00 | -11.44 |
| 2026-09-17 | `SMTC` | 9 | — | $170.85 | +0.00 | $178.19 | +66.06 | +66.06 | +0.00 | +66.06 |
| 2026-09-17 | `FOSL` | 302 | — | $5.46 | +0.00 | $5.63 | +51.34 | +51.34 | +0.00 | +51.34 |
| 2026-09-18 | `SRRK` | 28 | $49.02 | $48.02 | -28.00 | — | +0.00 | -28.00 | -55.72 | — |
| 2026-09-18 | `ARQT` | 63 | $26.46 | $26.14 | -20.16 | — | +0.00 | -20.16 | +11.97 | — |
| 2026-09-18 | `AMRX` | 89 | $18.28 | $18.12 | -14.24 | — | +0.00 | -14.24 | -39.16 | — |
| 2026-09-18 | `PGEN` | 217 | $7.87 | $7.98 | +23.87 | — | +0.00 | +23.87 | +84.63 | — |
| 2026-09-18 | `FTAI` | 8 | $195.07 | $195.55 | +3.84 | — | +0.00 | +3.84 | -7.60 | — |
| 2026-09-18 | `SMTC` | 9 | $178.19 | $182.33 | +37.26 | — | +0.00 | +37.26 | +103.32 | — |
| 2026-09-18 | `FOSL` | 302 | $5.63 | $5.63 | +0.00 | — | +0.00 | +0.00 | +51.34 | — |
| 2026-09-18 | `RARE` | 96 | — | $14.79 | +0.00 | $14.51 | -26.88 | -26.88 | +0.00 | -26.88 |
| 2026-09-18 | `BHVN` | 101 | — | $14.07 | +0.00 | $13.62 | -45.45 | -45.45 | +0.00 | -45.45 |
| 2026-09-18 | `TH` | 68 | — | $20.91 | +0.00 | $21.19 | +19.04 | +19.04 | +0.00 | +19.04 |
| 2026-09-18 | `SYM` | 31 | — | $44.70 | +0.00 | $41.89 | -87.11 | -87.11 | +0.00 | -87.11 |
| 2026-09-18 | `AMD` | 2 | — | $547.37 | +0.00 | $559.82 | +24.90 | +24.90 | +0.00 | +24.90 |
| 2026-09-18 | `SHLS` | 187 | — | $7.64 | +0.00 | $7.60 | -7.48 | -7.48 | +0.00 | -7.48 |
| 2026-09-18 | `BNC` | 245 | — | $5.83 | +0.00 | $5.98 | +36.75 | +36.75 | +0.00 | +36.75 |
| 2026-09-18 | `ASX` | 35 | — | $40.35 | +0.00 | $41.63 | +44.80 | +44.80 | +0.00 | +44.80 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +287.20 | ZIM, ENS, YSS, ADUR, WDC | — | $540.71 | $10,276.03 | ZIM×73, ENS×10, YSS×198, ADUR×121, WDC×3 |
| 2026-08-17 | +2.25 | $540.71 | ZIM×73, ENS×10, YSS×198, ADUR×121, WDC×3 | $10,226.79 | -49.24 | +30.56 | WBS, ALM | ZIM, ENS, YSS, ADUR, WDC | $50.22 | $10,239.78 | WBS×64, ALM×315 |
| 2026-08-18 | -6.20 | $50.22 | WBS×64, ALM×315 | $10,046.20 | -193.58 | +0.00 | — | WBS, ALM | $10,039.81 | $10,039.81 | — |
| 2026-08-19 | -7.20 | $10,039.81 | — | $10,039.81 | +0.00 | +0.00 | — | — | $10,039.81 | $10,039.81 | — |
| 2026-08-20 | +1.12 | $10,039.81 | — | $10,039.81 | +0.00 | -21.89 | SUI, NOMD, ZIM, MSTR, DNA, IAG, BHP, ATAT | — | $189.24 | $10,000.65 | SUI×10, NOMD×105, ZIM×45, MSTR×11, DNA×168, IAG×63, BHP×13, ATAT×36 |
| 2026-08-21 | +3.25 | $189.24 | SUI×10, NOMD×105, ZIM×45, MSTR×11, DNA×168, IAG×63, BHP×13, ATAT×36 | $10,201.33 | +200.68 | +323.90 | VIRT, GMAB, DE, CF, DXYZ | SUI, NOMD, ZIM, MSTR, DNA, IAG, BHP, ATAT | $331.82 | $10,497.31 | VIRT×33, GMAB×61, DE×3, CF×15, DXYZ×58 |
| 2026-08-24 | -5.17 | $331.82 | VIRT×33, GMAB×61, DE×3, CF×15, DXYZ×58 | $10,367.01 | -130.30 | +0.00 | — | VIRT, GMAB, DE, CF, DXYZ | $10,356.42 | $10,356.42 | — |
| 2026-08-25 | +1.80 | $10,356.42 | — | $10,356.42 | +0.00 | +71.12 | DBRG, VALE | — | $3,461.71 | $10,421.80 | DBRG×216, VALE×229 |
| 2026-08-26 | +2.02 | $3,461.71 | DBRG×216, VALE×229 | $10,430.96 | +9.16 | +1,285.47 | BZ | DBRG, VALE | $2.91 | $11,702.55 | BZ×621 |
| 2026-08-27 | — | $2.91 | BZ×621 | $11,491.41 | -211.14 | +0.00 | — | BZ | $11,483.20 | $11,483.20 | — |
| 2026-08-28 | +0.75 | $11,483.20 | — | $11,483.20 | +0.00 | +1.60 | EDU, SEDG, ZYME, TH, HAFN, S, ADSK, PD | — | $247.45 | $11,467.26 | EDU×24, SEDG×43, ZYME×49, TH×75, HAFN×171, S×66, ADSK×5, PD×109 |
| 2026-08-31 | -5.85 | $247.45 | EDU×24, SEDG×43, ZYME×49, TH×75, HAFN×171, S×66, ADSK×5, PD×109 | $11,361.83 | -105.43 | +0.00 | — | EDU, SEDG, ZYME, TH, HAFN, S, ADSK, PD | $11,344.09 | $11,344.09 | — |
| 2026-09-01 | -6.30 | $11,344.09 | — | $11,344.09 | -0.00 | +0.00 | — | — | $11,344.09 | $11,344.09 | — |
| 2026-09-02 | -3.83 | $11,344.09 | — | $11,344.09 | -0.00 | +0.00 | — | — | $11,344.09 | $11,344.09 | — |
| 2026-09-03 | -0.90 | $11,344.09 | — | $11,344.09 | -0.00 | +71.02 | GBTG, DELL, SBS, ETD, VSTM, MEI, ATRC | — | $204.10 | $11,397.40 | GBTG×170, DELL×3, SBS×311, ETD×74, VSTM×201, MEI×107, ATRC×30 |
| 2026-09-04 | +2.25 | $204.10 | GBTG×170, DELL×3, SBS×311, ETD×74, VSTM×201, MEI×107, ATRC×30 | $11,328.31 | -69.09 | +60.05 | LULU, HPE, BULL, MRX, MSTR, KYIV, GWRE | GBTG, SBS, ETD, VSTM, MEI, ATRC | $174.50 | $11,357.54 | DELL×3, LULU×14, HPE×25, BULL×142, MRX×18, MSTR×10, KYIV×98, GWRE×8 |
| 2026-09-08 | -11.47 | $174.50 | DELL×3, LULU×14, HPE×25, BULL×142, MRX×18, MSTR×10, KYIV×98, GWRE×8 | $11,329.29 | -28.25 | +0.00 | — | DELL, LULU, HPE, BULL, MRX, MSTR, KYIV, GWRE | $11,312.22 | $11,312.22 | — |
| 2026-09-09 | -13.95 | $11,312.22 | — | $11,312.22 | +0.00 | +0.00 | — | — | $11,312.22 | $11,312.22 | — |
| 2026-09-10 | -13.28 | $11,312.22 | — | $11,312.22 | +0.00 | +0.00 | — | — | $11,312.22 | $11,312.22 | — |
| 2026-09-11 | +0.50 | $11,312.22 | — | $11,312.22 | +0.00 | +25.86 | AVAV, SEDG, OBE, VIST, PBR, GME, INSP, PAGS | — | $174.10 | $11,320.76 | AVAV×9, SEDG×38, OBE×112, VIST×18, PBR×66, GME×67, INSP×20, PAGS×139 |
| 2026-09-14 | -11.00 | $174.10 | AVAV×9, SEDG×38, OBE×112, VIST×18, PBR×66, GME×67, INSP×20, PAGS×139 | $11,313.24 | -7.52 | +41.54 | — | AVAV, SEDG, OBE, VIST, PBR, INSP, PAGS | $9,890.93 | $11,339.47 | GME×67 |
| 2026-09-15 | -3.84 | $9,890.93 | GME×67 | $11,332.10 | -7.37 | +0.00 | — | GME | $11,329.89 | $11,329.89 | — |
| 2026-09-16 | +5.30 | $11,329.89 | — | $11,329.89 | -0.00 | -139.71 | SRRK, MEOH, VAL, RIG, ILMN, ADPT, MRCY, TEM | — | $173.80 | $11,172.66 | SRRK×28, MEOH×22, VAL×16, RIG×241, ILMN×6, ADPT×52, MRCY×16, TEM×20 |
| 2026-09-17 | +7.38 | $173.80 | SRRK×28, MEOH×22, VAL×16, RIG×241, ILMN×6, ADPT×52, MRCY×16, TEM×20 | $11,327.98 | +155.32 | +159.93 | ARQT, AMRX, PGEN, FTAI, SMTC, FOSL | MEOH, VAL, RIG, ILMN, ADPT, MRCY, TEM | $218.34 | $11,457.12 | SRRK×28, ARQT×63, AMRX×89, PGEN×217, FTAI×8, SMTC×9, FOSL×302 |
| 2026-09-18 | +4.86 | $218.34 | SRRK×28, ARQT×63, AMRX×89, PGEN×217, FTAI×8, SMTC×9, FOSL×302 | $11,459.69 | +2.57 | -41.43 | RARE, BHVN, TH, SYM, AMD, SHLS, BNC, ASX | SRRK, ARQT, AMRX, PGEN, FTAI, SMTC, FOSL | $411.06 | $11,382.14 | RARE×96, BHVN×101, TH×68, SYM×31, AMD×2, SHLS×187, BNC×245, ASX×35 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ZIM` | 73 | $27.25 | $2.21 | — | $8,008.54 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+1.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENS` | 10 | $196.00 | $2.02 | — | $6,046.52 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 198 | $10.06 | $2.58 | — | $4,052.06 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,053.20 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $540.71 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $540.71 | ▲ close $10,276.03 vs 09:30 $10,000.00 (session +287.20) | 16:00 close · cash $540.71 · equity $10,276.03 vs 09:30 $10,000.00 (+276.03; session marks +287.20) · 5 name(s) marked open→close (per-name table). ZIM×73 09:30 $27.25 → close $28.14 +64.97; ENS×10 09:30 $196.00 → close $203.40 +74.00; YSS×198 09:30 $10.06 → close $10.93 +172.26; ADUR×121 09:30 $16.50 → close $16.17 -39.93; WDC×3 09:30 $503.50 → close $508.80 +15.90 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $540.71 | ▼ 09:30 equity $10,226.79 vs yday $10,276.03 (-49.24) | 09:30 open · cash $540.71 (unchanged overnight, no fees) · equity $10,226.79 vs prior close $10,276.03 (-49.24) · 5 name(s) re-marked at the open (per-name table). ZIM×73 yday $28.14 → 09:30 $28.83 +50.37; ENS×10 yday $203.40 → 09:30 $205.03 +16.30; YSS×198 yday $10.93 → 09:30 $10.36 -112.86; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; WDC×3 yday $508.80 → 09:30 $525.53 +50.19 | — |
| 2026-08-17 09:30 ET | **SELL** | `ZIM` | 73 | $28.83 | $2.24 | $+110.89 | $2,643.06 | ▲ +110.89 after sell → book $10,224.56; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ENS` | 10 | $205.03 | $2.05 | $+86.23 | $4,691.31 | ▲ +86.23 after sell → book $10,222.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 198 | $10.36 | $2.63 | $+54.18 | $6,739.96 | ▲ +54.18 after sell → book $10,219.88; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,640.90 | ▼ -97.91 after sell → book $10,217.49; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $10,215.47 | ▲ +62.07 after sell → book $10,215.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `WBS` | 64 | $79.00 | $2.18 | — | $5,157.29 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; ⚪; ret5=+0.5; leftover $5107.73 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 315 | $16.20 | $4.06 | — | $50.22 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $5107.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.22 | ▲ close $10,239.78 vs 09:30 $10,226.79 (session +30.56) | 16:00 close · cash $50.22 · equity $10,239.78 vs 09:30 $10,226.79 (+12.99; session marks +30.56) · 2 name(s) marked open→close (per-name table). WBS×64 09:30 $79.00 → close $78.69 -19.84; ALM×315 09:30 $16.20 → close $16.36 +50.40 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.22 | ▼ 09:30 equity $10,046.20 vs yday $10,239.78 (-193.58) | 09:30 open · cash $50.22 (unchanged overnight, no fees) · equity $10,046.20 vs prior close $10,239.78 (-193.58) · 2 name(s) re-marked at the open (per-name table). WBS×64 yday $78.69 → 09:30 $78.52 -10.88; ALM×315 yday $16.36 → 09:30 $15.78 -182.70 | — |
| 2026-08-18 09:30 ET | **SELL** | `WBS` | 64 | $78.52 | $2.23 | $-35.13 | $5,073.27 | ▼ -35.13 after sell → book $10,043.97; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 315 | $15.78 | $4.16 | $-140.52 | $10,039.81 | ▼ -140.52 after sell → book $10,039.81; vs 09:30 mark -4.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,039.81 | ▲ close $10,039.81 vs 09:30 $10,046.20 (session +0.00) | 16:00 close · cash $10,039.81 · no lots left · equity $10,039.81. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,039.81 | ▲ 09:30 equity $10,039.81 vs yday $10,039.81 (+0.00) | 09:30 open · cash $10,039.81 · no holdings · equity $10,039.81 vs prior close $10,039.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,039.81 | ▲ close $10,039.81 vs 09:30 $10,039.81 (session +0.00) | 16:00 close · cash $10,039.81 · no lots left · equity $10,039.81. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,039.81 | ▲ 09:30 equity $10,039.81 vs yday $10,039.81 (+0.00) | 09:30 open · cash $10,039.81 · no holdings · equity $10,039.81 vs prior close $10,039.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 10 | $121.21 | $2.02 | — | $8,825.69 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.1; leftover $1254.98 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NOMD` | 105 | $11.84 | $2.31 | — | $7,580.19 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+4.7; leftover $1254.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZIM` | 45 | $27.45 | $2.12 | — | $6,342.81 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+8.5; leftover $1254.98 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 11 | $113.23 | $2.02 | — | $5,095.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1254.98 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 168 | $7.45 | $2.49 | — | $3,841.17 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1254.98 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $2,602.30 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $1,417.14 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.98 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 36 | $34.05 | $2.10 | — | $189.24 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+9.3; leftover $1254.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.24 | ▼ close $10,000.65 vs 09:30 $10,039.81 (session -21.89) | 16:00 close · cash $189.24 · equity $10,000.65 vs 09:30 $10,039.81 (-39.16; session marks -21.89) · 8 name(s) marked open→close (per-name table). SUI×10 09:30 $121.21 → close $122.29 +10.80; NOMD×105 09:30 $11.84 → close $11.61 -24.15; ZIM×45 09:30 $27.45 → close $27.16 -13.05; MSTR×11 09:30 $113.23 → close $112.39 -9.24; DNA×168 09:30 $7.45 → close $6.96 -82.32; IAG×63 09:30 $19.63 → close $20.50 +54.81; BHP×13 09:30 $91.01 → close $93.63 +34.06; ATAT×36 09:30 $34.05 → close $34.25 +7.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.24 | ▲ 09:30 equity $10,201.33 vs yday $10,000.65 (+200.68) | 09:30 open · cash $189.24 (unchanged overnight, no fees) · equity $10,201.33 vs prior close $10,000.65 (+200.68) · 8 name(s) re-marked at the open (per-name table). SUI×10 yday $122.29 → 09:30 $122.41 +1.20; NOMD×105 yday $11.61 → 09:30 $11.71 +10.50; ZIM×45 yday $27.16 → 09:30 $27.50 +15.30; MSTR×11 yday $112.39 → 09:30 $119.69 +80.30; DNA×168 yday $6.96 → 09:30 $7.09 +21.84; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; ATAT×36 yday $34.25 → 09:30 $34.31 +2.16 | — |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 10 | $122.41 | $2.04 | $+7.94 | $1,411.30 | ▲ +7.94 after sell → book $10,199.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NOMD` | 105 | $11.71 | $2.33 | $-18.29 | $2,638.52 | ▼ -18.29 after sell → book $10,196.96; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZIM` | 45 | $27.50 | $2.15 | $-2.02 | $3,873.87 | ▼ -2.02 after sell → book $10,194.81; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 11 | $119.69 | $2.04 | $+66.99 | $5,188.42 | ▲ +66.99 after sell → book $10,192.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 168 | $7.09 | $2.53 | $-65.51 | $6,377.01 | ▼ -65.51 after sell → book $10,190.24; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $7,708.52 | ▲ +92.64 after sell → book $10,188.04; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $8,950.83 | ▲ +57.15 after sell → book $10,185.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 36 | $34.31 | $2.12 | $+5.14 | $10,183.87 | ▲ +5.14 after sell → book $10,183.87; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 33 | $60.66 | $2.09 | — | $8,180.00 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+7.0; leftover $2036.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 61 | $33.36 | $2.17 | — | $6,142.87 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+6.6; leftover $2036.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 3 | $623.26 | $2.00 | — | $4,271.09 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2036.77 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 15 | $127.43 | $2.04 | — | $2,357.60 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2036.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 58 | $34.89 | $2.16 | — | $331.82 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.6; leftover $2036.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $331.82 | ▲ close $10,497.31 vs 09:30 $10,201.33 (session +323.90) | 16:00 close · cash $331.82 · equity $10,497.31 vs 09:30 $10,201.33 (+295.98; session marks +323.90) · 5 name(s) marked open→close (per-name table). VIRT×33 09:30 $60.66 → close $67.93 +239.91; GMAB×61 09:30 $33.36 → close $33.45 +5.49; DE×3 09:30 $623.26 → close $647.47 +72.63; CF×15 09:30 $127.43 → close $129.60 +32.55; DXYZ×58 09:30 $34.89 → close $34.43 -26.68 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $331.82 | ▼ 09:30 equity $10,367.01 vs yday $10,497.31 (-130.30) | 09:30 open · cash $331.82 (unchanged overnight, no fees) · equity $10,367.01 vs prior close $10,497.31 (-130.30) · 5 name(s) re-marked at the open (per-name table). VIRT×33 yday $67.93 → 09:30 $66.80 -37.29; GMAB×61 yday $33.45 → 09:30 $32.82 -38.43; DE×3 yday $647.47 → 09:30 $653.04 +16.71; CF×15 yday $129.60 → 09:30 $129.99 +5.85; DXYZ×58 yday $34.43 → 09:30 $33.10 -77.14 | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 33 | $66.80 | $2.12 | $+198.41 | $2,534.10 | ▲ +198.41 after sell → book $10,364.89; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 61 | $32.82 | $2.20 | $-37.31 | $4,533.92 | ▼ -37.31 after sell → book $10,362.69; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 3 | $653.04 | $2.02 | $+85.32 | $6,491.02 | ▲ +85.32 after sell → book $10,360.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 15 | $129.99 | $2.06 | $+34.30 | $8,438.81 | ▲ +34.30 after sell → book $10,358.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 58 | $33.10 | $2.19 | $-108.17 | $10,356.42 | ▼ -108.17 after sell → book $10,356.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,356.42 | ▲ close $10,356.42 vs 09:30 $10,367.01 (session +0.00) | 16:00 close · cash $10,356.42 · no lots left · equity $10,356.42. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,356.42 | ▲ 09:30 equity $10,356.42 vs yday $10,356.42 (+0.00) | 09:30 open · cash $10,356.42 · no holdings · equity $10,356.42 vs prior close $10,356.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `DBRG` | 216 | $15.98 | $2.79 | — | $6,901.95 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.4; leftover $3452.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 229 | $15.01 | $2.95 | — | $3,461.71 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy; ⚪; ret5=+9.4; leftover $3452.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,461.71 | ▲ close $10,421.80 vs 09:30 $10,356.42 (session +71.12) | 16:00 close · cash $3,461.71 · equity $10,421.80 vs 09:30 $10,356.42 (+65.38; session marks +71.12) · 2 name(s) marked open→close (per-name table). DBRG×216 09:30 $15.98 → close $15.97 -2.16; VALE×229 09:30 $15.01 → close $15.33 +73.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,461.71 | ▲ 09:30 equity $10,430.96 vs yday $10,421.80 (+9.16) | 09:30 open · cash $3,461.71 (unchanged overnight, no fees) · equity $10,430.96 vs prior close $10,421.80 (+9.16) · 2 name(s) re-marked at the open (per-name table). DBRG×216 yday $15.97 → 09:30 $15.97 +0.00; VALE×229 yday $15.33 → 09:30 $15.37 +9.16 | — |
| 2026-08-26 09:30 ET | **SELL** | `DBRG` | 216 | $15.97 | $2.85 | $-7.80 | $6,908.38 | ▼ -7.80 after sell → book $10,428.11; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 229 | $15.37 | $3.02 | $+76.47 | $10,425.09 | ▲ +76.47 after sell → book $10,425.09; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 621 | $16.77 | $8.01 | — | $2.91 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+3.1; leftover $10425.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.91 | ▲ close $11,702.55 vs 09:30 $10,430.96 (session +1,285.47) | 16:00 close · cash $2.91 · equity $11,702.55 vs 09:30 $10,430.96 (+1271.59; session marks +1285.47) · 1 name(s) marked open→close (per-name table). BZ×621 09:30 $16.77 → close $18.84 +1285.47 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.91 | ▼ 09:30 equity $11,491.41 vs yday $11,702.55 (-211.14) | 09:30 open · cash $2.91 (unchanged overnight, no fees) · equity $11,491.41 vs prior close $11,702.55 (-211.14) · 1 name(s) re-marked at the open (per-name table). BZ×621 yday $18.84 → 09:30 $18.50 -211.14 | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 621 | $18.50 | $8.21 | $+1058.11 | $11,483.20 | ▲ +1,058.11 after sell → book $11,483.20; vs 09:30 mark -8.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,483.20 | ▲ close $11,483.20 vs 09:30 $11,491.41 (session +0.00) | 16:00 close · cash $11,483.20 · no lots left · equity $11,483.20. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,483.20 | ▲ 09:30 equity $11,483.20 vs yday $11,483.20 (+0.00) | 09:30 open · cash $11,483.20 · no holdings · equity $11,483.20 vs prior close $11,483.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `EDU` | 24 | $57.63 | $2.06 | — | $10,098.02 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; ret5=+5.5; leftover $1435.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $8,681.20 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1435.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 49 | $28.91 | $2.14 | — | $7,262.47 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $1435.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 75 | $19.00 | $2.21 | — | $5,835.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1435.40 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 171 | $8.35 | $2.50 | — | $4,404.91 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.1; leftover $1435.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 66 | $21.49 | $2.19 | — | $2,984.38 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+8.5; leftover $1435.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $1,676.57 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+7.8; leftover $1435.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 109 | $13.09 | $2.32 | — | $247.45 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+4.2; leftover $1435.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.45 | ▲ close $11,467.26 vs 09:30 $11,483.20 (session +1.60) | 16:00 close · cash $247.45 · equity $11,467.26 vs 09:30 $11,483.20 (-15.94; session marks +1.60) · 8 name(s) marked open→close (per-name table). EDU×24 09:30 $57.63 → close $58.83 +28.80; SEDG×43 09:30 $32.90 → close $31.41 -64.07; ZYME×49 09:30 $28.91 → close $28.27 -31.36; TH×75 09:30 $19.00 → close $18.55 -33.75; HAFN×171 09:30 $8.35 → close $8.47 +20.52; S×66 09:30 $21.49 → close $21.54 +3.30; ADSK×5 09:30 $261.16 → close $260.66 -2.50; PD×109 09:30 $13.09 → close $13.83 +80.66 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.45 | ▼ 09:30 equity $11,361.83 vs yday $11,467.26 (-105.43) | 09:30 open · cash $247.45 (unchanged overnight, no fees) · equity $11,361.83 vs prior close $11,467.26 (-105.43) · 8 name(s) re-marked at the open (per-name table). EDU×24 yday $58.83 → 09:30 $58.23 -14.40; SEDG×43 yday $31.41 → 09:30 $31.15 -11.18; ZYME×49 yday $28.27 → 09:30 $28.06 -10.29; TH×75 yday $18.55 → 09:30 $18.12 -31.88; HAFN×171 yday $8.47 → 09:30 $8.53 +10.26; S×66 yday $21.54 → 09:30 $21.45 -5.94; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; PD×109 yday $13.83 → 09:30 $13.58 -27.25 | — |
| 2026-08-31 09:30 ET | **SELL** | `EDU` | 24 | $58.23 | $2.08 | $+10.25 | $1,642.88 | ▲ +10.25 after sell → book $11,359.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 43 | $31.15 | $2.14 | $-79.51 | $2,980.19 | ▼ -79.51 after sell → book $11,357.61; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 49 | $28.06 | $2.16 | $-45.94 | $4,352.98 | ▼ -45.94 after sell → book $11,355.45; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 75 | $18.12 | $2.24 | $-70.08 | $5,710.11 | ▼ -70.08 after sell → book $11,353.21; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 171 | $8.53 | $2.54 | $+25.73 | $7,166.20 | ▲ +25.73 after sell → book $11,350.67; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 66 | $21.45 | $2.21 | $-7.04 | $8,579.69 | ▼ -7.04 after sell → book $11,348.46; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $9,866.21 | ▼ -21.28 after sell → book $11,346.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 109 | $13.58 | $2.35 | $+48.75 | $11,344.09 | ▲ +48.75 after sell → book $11,344.09; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.09 | ▲ close $11,344.09 vs 09:30 $11,361.83 (session +0.00) | 16:00 close · cash $11,344.09 · no lots left · equity $11,344.09. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,344.09 | ▲ 09:30 equity $11,344.09 vs yday $11,344.09 (-0.00) | 09:30 open · cash $11,344.09 · no holdings · equity $11,344.09 vs prior close $11,344.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.09 | ▲ close $11,344.09 vs 09:30 $11,344.09 (session +0.00) | 16:00 close · cash $11,344.09 · no lots left · equity $11,344.09. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,344.09 | ▲ 09:30 equity $11,344.09 vs yday $11,344.09 (-0.00) | 09:30 open · cash $11,344.09 · no holdings · equity $11,344.09 vs prior close $11,344.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.09 | ▲ close $11,344.09 vs 09:30 $11,344.09 (session +0.00) | 16:00 close · cash $11,344.09 · no lots left · equity $11,344.09. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,344.09 | ▲ 09:30 equity $11,344.09 vs yday $11,344.09 (-0.00) | 09:30 open · cash $11,344.09 · no holdings · equity $11,344.09 vs prior close $11,344.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GBTG` | 170 | $9.49 | $2.50 | — | $9,728.29 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.2; leftover $1620.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $8,267.36 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1620.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SBS` | 311 | $5.21 | $4.01 | — | $6,643.04 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.9; leftover $1620.58 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ETD` | 74 | $21.82 | $2.21 | — | $5,026.14 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.7; leftover $1620.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 201 | $8.03 | $2.60 | — | $3,409.52 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1620.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 107 | $15.09 | $2.31 | — | $1,792.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+6.1; leftover $1620.58 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 30 | $52.88 | $2.08 | — | $204.10 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1620.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.10 | ▲ close $11,397.40 vs 09:30 $11,344.09 (session +71.02) | 16:00 close · cash $204.10 · equity $11,397.40 vs 09:30 $11,344.09 (+53.31; session marks +71.02) · 7 name(s) marked open→close (per-name table). GBTG×170 09:30 $9.49 → close $9.49 +0.00; DELL×3 09:30 $486.31 → close $516.39 +90.24; SBS×311 09:30 $5.21 → close $5.13 -24.88; ETD×74 09:30 $21.82 → close $21.87 +3.70; VSTM×201 09:30 $8.03 → close $7.98 -10.05; MEI×107 09:30 $15.09 → close $15.32 +24.61; ATRC×30 09:30 $52.88 → close $52.46 -12.60 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.10 | ▼ 09:30 equity $11,328.31 vs yday $11,397.40 (-69.09) | 09:30 open · cash $204.10 (unchanged overnight, no fees) · equity $11,328.31 vs prior close $11,397.40 (-69.09) · 7 name(s) re-marked at the open (per-name table). GBTG×170 yday $9.49 → 09:30 $9.49 +0.00; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; SBS×311 yday $5.13 → 09:30 $5.02 -34.21; ETD×74 yday $21.87 → 09:30 $21.84 -2.22; VSTM×201 yday $7.98 → 09:30 $7.91 -14.07; MEI×107 yday $15.32 → 09:30 $15.34 +2.14; ATRC×30 yday $52.46 → 09:30 $52.03 -12.90 | — |
| 2026-09-04 09:30 ET | **SELL** | `GBTG` | 170 | $9.49 | $2.54 | $-5.04 | $1,814.85 | ▼ -5.04 after sell → book $11,325.76; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SBS` | 311 | $5.02 | $4.08 | $-67.18 | $3,372.00 | ▼ -67.18 after sell → book $11,321.69; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ETD` | 74 | $21.84 | $2.24 | $-2.97 | $4,985.92 | ▼ -2.97 after sell → book $11,319.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 201 | $7.91 | $2.64 | $-29.36 | $6,573.19 | ▼ -29.36 after sell → book $11,316.81; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 107 | $15.34 | $2.34 | $+22.10 | $8,212.22 | ▲ +22.10 after sell → book $11,314.46; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 30 | $52.03 | $2.10 | $-29.68 | $9,771.02 | ▼ -29.68 after sell → book $11,312.36; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 14 | $98.15 | $2.03 | — | $8,394.89 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react,oppset; ret5=+5.9; leftover $1395.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 25 | $53.85 | $2.06 | — | $7,046.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.1; leftover $1395.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BULL` | 142 | $9.79 | $2.42 | — | $5,653.98 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.5; leftover $1395.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 18 | $75.65 | $2.04 | — | $4,290.24 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1395.86 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 10 | $137.35 | $2.02 | — | $2,914.72 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1395.86 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `KYIV` | 98 | $14.24 | $2.28 | — | $1,516.91 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ⚪; ret5=+5.2; leftover $1395.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $174.50 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1395.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.50 | ▲ close $11,357.54 vs 09:30 $11,328.31 (session +60.05) | 16:00 close · cash $174.50 · equity $11,357.54 vs 09:30 $11,328.31 (+29.23; session marks +60.05) · 8 name(s) marked open→close (per-name table). DELL×3 09:30 $513.78 → close $524.14 +31.08; LULU×14 09:30 $98.15 → close $100.61 +34.44; HPE×25 09:30 $53.85 → close $52.00 -46.25; BULL×142 09:30 $9.79 → close $9.74 -7.10; MRX×18 09:30 $75.65 → close $78.27 +47.16; MSTR×10 09:30 $137.35 → close $142.80 +54.50; KYIV×98 09:30 $14.24 → close $14.11 -12.74; GWRE×8 09:30 $167.55 → close $162.42 -41.04 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.50 | ▼ 09:30 equity $11,329.29 vs yday $11,357.54 (-28.25) | 09:30 open · cash $174.50 (unchanged overnight, no fees) · equity $11,329.29 vs prior close $11,357.54 (-28.25) · 8 name(s) re-marked at the open (per-name table). DELL×3 yday $524.14 → 09:30 $521.15 -8.97; LULU×14 yday $100.61 → 09:30 $100.58 -0.42; HPE×25 yday $52.00 → 09:30 $52.29 +7.25; BULL×142 yday $9.74 → 09:30 $9.94 +27.69; MRX×18 yday $78.27 → 09:30 $78.84 +10.26; MSTR×10 yday $142.80 → 09:30 $137.62 -51.80; KYIV×98 yday $14.11 → 09:30 $14.14 +2.94; GWRE×8 yday $162.42 → 09:30 $160.52 -15.20 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 3 | $521.15 | $2.02 | $+100.50 | $1,735.93 | ▲ +100.50 after sell → book $11,327.27; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 14 | $100.58 | $2.05 | $+29.93 | $3,141.99 | ▲ +29.93 after sell → book $11,325.21; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 25 | $52.29 | $2.09 | $-43.15 | $4,447.16 | ▼ -43.15 after sell → book $11,323.13; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BULL` | 142 | $9.94 | $2.45 | $+15.72 | $5,855.48 | ▲ +15.72 after sell → book $11,320.68; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 18 | $78.84 | $2.07 | $+53.31 | $7,272.53 | ▲ +53.31 after sell → book $11,318.61; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 10 | $137.62 | $2.04 | $-1.36 | $8,646.69 | ▼ -1.36 after sell → book $11,316.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `KYIV` | 98 | $14.14 | $2.31 | $-14.40 | $10,030.10 | ▼ -14.40 after sell → book $11,314.26; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $11,312.22 | ▼ -60.29 after sell → book $11,312.22; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,312.22 | ▲ close $11,312.22 vs 09:30 $11,329.29 (session +0.00) | 16:00 close · cash $11,312.22 · no lots left · equity $11,312.22. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,312.22 | ▲ 09:30 equity $11,312.22 vs yday $11,312.22 (+0.00) | 09:30 open · cash $11,312.22 · no holdings · equity $11,312.22 vs prior close $11,312.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,312.22 | ▲ close $11,312.22 vs 09:30 $11,312.22 (session +0.00) | 16:00 close · cash $11,312.22 · no lots left · equity $11,312.22. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,312.22 | ▲ 09:30 equity $11,312.22 vs yday $11,312.22 (+0.00) | 09:30 open · cash $11,312.22 · no holdings · equity $11,312.22 vs prior close $11,312.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,312.22 | ▲ close $11,312.22 vs 09:30 $11,312.22 (session +0.00) | 16:00 close · cash $11,312.22 · no lots left · equity $11,312.22. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,312.22 | ▲ 09:30 equity $11,312.22 vs yday $11,312.22 (+0.00) | 09:30 open · cash $11,312.22 · no holdings · equity $11,312.22 vs prior close $11,312.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AVAV` | 9 | $145.91 | $2.02 | — | $9,997.02 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.2; leftover $1414.03 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 38 | $36.78 | $2.10 | — | $8,597.27 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+8.2; leftover $1414.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OBE` | 112 | $12.55 | $2.33 | — | $7,189.35 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; ret5=+5.5; leftover $1414.03 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 18 | $77.33 | $2.04 | — | $5,795.36 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=+2.5; leftover $1414.03 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 66 | $21.21 | $2.19 | — | $4,393.31 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1414.03 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 67 | $21.04 | $2.19 | — | $2,981.44 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.5; leftover $1414.03 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 20 | $69.88 | $2.05 | — | $1,581.79 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.0; leftover $1414.03 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 139 | $10.11 | $2.41 | — | $174.10 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1414.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.10 | ▲ close $11,320.76 vs 09:30 $11,312.22 (session +25.86) | 16:00 close · cash $174.10 · equity $11,320.76 vs 09:30 $11,312.22 (+8.54; session marks +25.86) · 8 name(s) marked open→close (per-name table). AVAV×9 09:30 $145.91 → close $146.71 +7.20; SEDG×38 09:30 $36.78 → close $34.68 -79.80; OBE×112 09:30 $12.55 → close $12.97 +47.04; VIST×18 09:30 $77.33 → close $76.27 -19.08; PBR×66 09:30 $21.21 → close $21.20 -0.66; GME×67 09:30 $21.04 → close $21.15 +7.37; INSP×20 09:30 $69.88 → close $73.00 +62.40; PAGS×139 09:30 $10.11 → close $10.12 +1.39 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.10 | ▼ 09:30 equity $11,313.24 vs yday $11,320.76 (-7.52) | 09:30 open · cash $174.10 (unchanged overnight, no fees) · equity $11,313.24 vs prior close $11,320.76 (-7.52) · 8 name(s) re-marked at the open (per-name table). AVAV×9 yday $146.71 → 09:30 $145.80 -8.19; SEDG×38 yday $34.68 → 09:30 $33.64 -39.52; OBE×112 yday $12.97 → 09:30 $13.57 +67.20; VIST×18 yday $76.27 → 09:30 $77.10 +14.94; PBR×66 yday $21.20 → 09:30 $21.23 +1.98; GME×67 yday $21.15 → 09:30 $21.00 -10.05; INSP×20 yday $73.00 → 09:30 $72.14 -17.20; PAGS×139 yday $10.12 → 09:30 $10.00 -16.68 | — |
| 2026-09-14 09:30 ET | **SELL** | `AVAV` | 9 | $145.80 | $2.04 | $-5.04 | $1,484.26 | ▼ -5.04 after sell → book $11,311.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SEDG` | 38 | $33.64 | $2.12 | $-123.55 | $2,760.46 | ▼ -123.55 after sell → book $11,309.08; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OBE` | 112 | $13.57 | $2.36 | $+109.56 | $4,277.94 | ▲ +109.56 after sell → book $11,306.72; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 18 | $77.10 | $2.07 | $-8.25 | $5,663.67 | ▼ -8.25 after sell → book $11,304.65; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 66 | $21.23 | $2.21 | $-3.08 | $7,062.64 | ▼ -3.08 after sell → book $11,302.44; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 20 | $72.14 | $2.07 | $+41.08 | $8,503.37 | ▲ +41.08 after sell → book $11,300.37; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 139 | $10.00 | $2.44 | $-20.14 | $9,890.93 | ▼ -20.14 after sell → book $11,297.93; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,890.93 | ▲ close $11,339.47 vs 09:30 $11,313.24 (session +41.54) | 16:00 close · cash $9,890.93 · equity $11,339.47 vs 09:30 $11,313.24 (+26.23; session marks +41.54) · 1 name(s) marked open→close (per-name table). GME×67 09:30 $21.00 → close $21.62 +41.54 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,890.93 | ▼ 09:30 equity $11,332.10 vs yday $11,339.47 (-7.37) | 09:30 open · cash $9,890.93 (unchanged overnight, no fees) · equity $11,332.10 vs prior close $11,339.47 (-7.37) · 1 name(s) re-marked at the open (per-name table). GME×67 yday $21.62 → 09:30 $21.51 -7.37 | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 67 | $21.51 | $2.21 | $+27.09 | $11,329.89 | ▲ +27.09 after sell → book $11,329.89; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,329.89 | ▲ close $11,329.89 vs 09:30 $11,332.10 (session +0.00) | 16:00 close · cash $11,329.89 · no lots left · equity $11,329.89. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,329.89 | ▲ 09:30 equity $11,329.89 vs yday $11,329.89 (-0.00) | 09:30 open · cash $11,329.89 · no holdings · equity $11,329.89 vs prior close $11,329.89 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SRRK` | 28 | $50.01 | $2.07 | — | $9,927.53 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1416.24 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MEOH` | 22 | $63.34 | $2.06 | — | $8,532.00 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.4; leftover $1416.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $7,131.56 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1416.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 241 | $5.87 | $3.11 | — | $5,713.78 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1416.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 6 | $224.49 | $2.01 | — | $4,364.83 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+5.3; leftover $1416.24 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 52 | $27.09 | $2.15 | — | $2,954.01 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1416.24 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 16 | $87.52 | $2.04 | — | $1,551.65 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.3; leftover $1416.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 20 | $68.79 | $2.05 | — | $173.80 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1416.24 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.80 | ▼ close $11,172.66 vs 09:30 $11,329.89 (session -139.71) | 16:00 close · cash $173.80 · equity $11,172.66 vs 09:30 $11,329.89 (-157.23; session marks -139.71) · 8 name(s) marked open→close (per-name table). SRRK×28 09:30 $50.01 → close $49.37 -17.92; MEOH×22 09:30 $63.34 → close $61.51 -40.26; VAL×16 09:30 $87.40 → close $82.52 -78.08; RIG×241 09:30 $5.87 → close $5.54 -79.53; ILMN×6 09:30 $224.49 → close $228.93 +26.64; ADPT×52 09:30 $27.09 → close $27.67 +30.16; MRCY×16 09:30 $87.52 → close $87.25 -4.32; TEM×20 09:30 $68.79 → close $69.97 +23.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.80 | ▲ 09:30 equity $11,327.98 vs yday $11,172.66 (+155.32) | 09:30 open · cash $173.80 (unchanged overnight, no fees) · equity $11,327.98 vs prior close $11,172.66 (+155.32) · 8 name(s) re-marked at the open (per-name table). SRRK×28 yday $49.37 → 09:30 $49.52 +4.20; MEOH×22 yday $61.51 → 09:30 $60.83 -14.96; VAL×16 yday $82.52 → 09:30 $83.20 +10.88; RIG×241 yday $5.54 → 09:30 $5.58 +9.64; ILMN×6 yday $228.93 → 09:30 $233.85 +29.52; ADPT×52 yday $27.67 → 09:30 $28.23 +29.12; MRCY×16 yday $87.25 → 09:30 $89.27 +32.32; TEM×20 yday $69.97 → 09:30 $72.70 +54.60 | — |
| 2026-09-17 09:30 ET | **SELL** | `MEOH` | 22 | $60.83 | $2.08 | $-59.35 | $1,509.98 | ▼ -59.35 after sell → book $11,325.90; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $2,839.12 | ▼ -71.30 after sell → book $11,323.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 241 | $5.58 | $3.16 | $-76.16 | $4,180.74 | ▼ -76.16 after sell → book $11,320.68; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 6 | $233.85 | $2.03 | $+52.12 | $5,581.81 | ▲ +52.12 after sell → book $11,318.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 52 | $28.23 | $2.17 | $+54.97 | $7,047.61 | ▲ +54.97 after sell → book $11,316.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 16 | $89.27 | $2.06 | $+23.90 | $8,473.87 | ▲ +23.90 after sell → book $11,314.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 20 | $72.70 | $2.07 | $+74.08 | $9,925.79 | ▲ +74.08 after sell → book $11,312.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 63 | $25.95 | $2.18 | — | $8,288.77 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1654.30 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 89 | $18.56 | $2.26 | — | $6,634.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ret5=+4.8; leftover $1654.30 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 217 | $7.59 | $2.80 | — | $4,984.84 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1654.30 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 8 | $196.50 | $2.01 | — | $3,410.83 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $1654.30 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 9 | $170.85 | $2.02 | — | $1,871.16 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1654.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FOSL` | 302 | $5.46 | $3.90 | — | $218.34 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1654.30 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.34 | ▲ close $11,457.12 vs 09:30 $11,327.98 (session +159.93) | 16:00 close · cash $218.34 · equity $11,457.12 vs 09:30 $11,327.98 (+129.14; session marks +159.93) · 7 name(s) marked open→close (per-name table). SRRK×28 09:30 $49.52 → close $49.02 -14.00; ARQT×63 09:30 $25.95 → close $26.46 +32.13; AMRX×89 09:30 $18.56 → close $18.28 -24.92; PGEN×217 09:30 $7.59 → close $7.87 +60.76; FTAI×8 09:30 $196.50 → close $195.07 -11.44; SMTC×9 09:30 $170.85 → close $178.19 +66.06; FOSL×302 09:30 $5.46 → close $5.63 +51.34 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.34 | ▲ 09:30 equity $11,459.69 vs yday $11,457.12 (+2.57) | 09:30 open · cash $218.34 (unchanged overnight, no fees) · equity $11,459.69 vs prior close $11,457.12 (+2.57) · 7 name(s) re-marked at the open (per-name table). SRRK×28 yday $49.02 → 09:30 $48.02 -28.00; ARQT×63 yday $26.46 → 09:30 $26.14 -20.16; AMRX×89 yday $18.28 → 09:30 $18.12 -14.24; PGEN×217 yday $7.87 → 09:30 $7.98 +23.87; FTAI×8 yday $195.07 → 09:30 $195.55 +3.84; SMTC×9 yday $178.19 → 09:30 $182.33 +37.26; FOSL×302 yday $5.63 → 09:30 $5.63 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 28 | $48.02 | $2.09 | $-59.89 | $1,560.81 | ▼ -59.89 after sell → book $11,457.60; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 63 | $26.14 | $2.20 | $+7.59 | $3,205.43 | ▲ +7.59 after sell → book $11,455.40; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 89 | $18.12 | $2.28 | $-43.70 | $4,815.82 | ▼ -43.70 after sell → book $11,453.11; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 217 | $7.98 | $2.85 | $+78.98 | $6,544.63 | ▲ +78.98 after sell → book $11,450.26; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 8 | $195.55 | $2.04 | $-11.65 | $8,107.00 | ▼ -11.65 after sell → book $11,448.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 9 | $182.33 | $2.04 | $+99.26 | $9,745.92 | ▲ +99.26 after sell → book $11,446.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FOSL` | 302 | $5.63 | $3.96 | $+43.48 | $11,442.23 | ▲ +43.48 after sell → book $11,442.23; vs 09:30 mark -3.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 96 | $14.79 | $2.28 | — | $10,020.11 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 101 | $14.07 | $2.29 | — | $8,596.74 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 68 | $20.91 | $2.19 | — | $7,172.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 31 | $44.70 | $2.08 | — | $5,784.89 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.5; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $4,688.15 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 187 | $7.64 | $2.55 | — | $3,256.92 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 245 | $5.83 | $3.16 | — | $1,825.41 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ASX` | 35 | $40.35 | $2.10 | — | $411.06 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.7; leftover $1430.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $411.06 | ▼ close $11,382.14 vs 09:30 $11,459.69 (session -41.43) | 16:00 close · cash $411.06 · equity $11,382.14 vs 09:30 $11,459.69 (-77.55; session marks -41.43) · 8 name(s) marked open→close (per-name table). RARE×96 09:30 $14.79 → close $14.51 -26.88; BHVN×101 09:30 $14.07 → close $13.62 -45.45; TH×68 09:30 $20.91 → close $21.19 +19.04; SYM×31 09:30 $44.70 → close $41.89 -87.11; AMD×2 09:30 $547.37 → close $559.82 +24.90; SHLS×187 09:30 $7.64 → close $7.60 -7.48; BNC×245 09:30 $5.83 → close $5.98 +36.75; ASX×35 09:30 $40.35 → close $41.63 +44.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BVN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PODD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SWKS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IHS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `KMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BILI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OPFI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `OKLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ASX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TSM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RARE` | 96 | 2026-09-18 @ $14.79 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1430.28 |
| `BHVN` | 101 | 2026-09-18 @ $14.07 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1430.28 |
| `TH` | 68 | 2026-09-18 @ $20.91 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1430.28 |
| `SYM` | 31 | 2026-09-18 @ $44.70 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.5; leftover $1430.28 |
| `AMD` | 2 | 2026-09-18 @ $547.37 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $1430.28 |
| `SHLS` | 187 | 2026-09-18 @ $7.64 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $1430.28 |
| `BNC` | 245 | 2026-09-18 @ $5.83 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1430.28 |
| `ASX` | 35 | 2026-09-18 @ $40.35 | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.7; leftover $1430.28 |
