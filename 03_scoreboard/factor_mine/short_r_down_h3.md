# Factor mine action — `short_r_down_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · downgrade ≤5d

Cash book **+0.00%** ($10,000) · signal-only (no cash/fees) was +0.00%. Starts YES **0/17**. Fills 0 · skips 0 · realized $+0.00.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `flag_R=-1,days_since_R_max=5` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,000.00.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-21 | +3.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-24 | -5.17 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-5.17 |
| 2026-08-25 | +1.80 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-26 | +2.02 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-27 | — | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-28 | +0.75 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-31 | -5.85 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-09-04 | — | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
