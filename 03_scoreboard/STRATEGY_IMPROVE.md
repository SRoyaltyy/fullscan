# Strategy improve — gainer reverse-run

_Generated 2026-09-19T05:22:16-04:00 — research audit, not a wire._

Liquid Finviz top-15 gainers (Change% ≥ 5%, mcap ≥ $100M, adv ≥ 500k) run back through the 09:30 packet that already printed. Same-day Change% only picks the universe.

## When is a strategy improved?

All four bars below must be green on a rolling 10-session window. Book% / fill-reality ideal / general direction-call do **not** count.

### `live_up_not_empty` — Live book is in the market on UP mornings · **FAIL**

- Pass: On a session with morning S > 0, leftover cash ≥ $10k, and not hard-red: flatten_robust prints at least one live BUY (n_priced_buys ≥ 1 or a 09:30 ticket). io 3d must be able to schedule an exit (session calendar extends ≥ 3 sessions past D).
- Fail looks like: S positive and $101k leftover with 0 priced BUYs / `io 3d cannot settle` is a sit, not a strategy call.
- This window: flatten_robust sit=True on an audited UP window is a fail when leftover cash could buy 1 share (see sleeve today.json).

### `fat_day_keep_oc` — KEEP longs are not red from the 09:30 open on fat index days · **MEASURE**

- Pass: On sessions with SPX close-to-close ≥ +0.80%: equal-weight open→close of that morning's KEEP long list (union_hot_n4_h1, else combo_sh longs) ≥ SPX open→close − 0.50 pp. Gap days are scored from the open, not from the prior close.
- Fail looks like: Thursday 09-17: SPX +1.14% close-to-close / +0.08% from the open; hot4 AZTA/RVTY/IT/FIVN −1.06% from the open.
- This window: Needs official SPX + KEEP open→close for each fat day. 09-17 already measured: hot4 −1.06% vs SPX open→close +0.08%.

### `gainer_recall` — Liquid rippers show up on a morning list · **FAIL**

- Pass: Rolling 10 sessions: of each day's liquid top-15 (Change% ≥ 5%, mcap ≥ $100M, adv ≥ 500k), ≥ 25% are a hit, and ≥ 1 of the day's top-5 is a hit on at least 6 of those sessions. Hit = in stock-book 1d BUY ∪ flatten/KEEP tickets ∪ news |net| ≥ 1 ∪ a usable catalyst dossier.
- Fail looks like: Same-day Change% is the universe only. A name that ripped with every camera dark and no list seat is a miss.
- This window: mean recall 0% vs 25% bar; top-5 hit on 0/3 sessions

### `catalyst_targets_move` — Dossier seats are real company events, not stuck override captains · **FAIL**

- Pass: Usable dossiers ≥ 2 that morning, the 8 targets are not identical for 3 straight sessions, and ≥ 1 target is in that day's news |net| ≥ 2 or that day's liquid top-15 gainers.
- Fail looks like: 09-16/17/18: same 8 oil/coal override captains (NE RIG SLB BKR KGS WHD CNR BTU), then OpenClaw+DeepSeek empty STEP1 → 0/8.
- This window: usable [0, 0, 0]; stuck_captains=True; target∩gainer [0, 0, 0]

## What does **not** count as improved

- Book% on combo_sh_5050 (short grind + one-week hot burst)
- Fill-reality ideal 09:30 butterfly
- General predict direction-call hit rate
- Paper Webull n_would with $0 cash

## Why real catalysts miss the book

Three stacked filters, in order:

1. **Wrong eight seats.** `catalyst_daily.select_targets` fills OVERRIDE map-heat captains first. Oil & Gas Drilling / Equipment / Thermal Coal consumed all 8 slots (NE RIG SLB BKR KGS WHD CNR BTU) on 09-16, 09-17, and 09-18. News `action_top` and the day's actual rippers never get a dossier.
2. **Those seats then go empty.** OpenClaw + DeepSeek return empty on CATALYST STEP1/STEP2 → `n_ok=0`. Usable-dossier boost to news actions is zero, so the company route has nothing to adjudicate.
3. **The book is a different pile.** News actions stay on the oil E&P cluster (COP/EOG/RRC…). Judge prints sector ETFs (XLE/IGV), not SDGR/GNRC. Flatten / hot4 pick healthcare size-book names. Live flatten then sits (`io 3d cannot settle`) so even the wrong names are not bought.

Dossiers also run **after** the stock book in preopen ALL, so a healthy STEP1 still cannot pick that morning's BUY list.

## Reverse-run

### 2026-09-16 · recall 0% (0/15) · top-5 hits 0 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP1 NE

KEEP hot4: `AVAH`, `BLFS`, `IQV`, `RDNT`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `BBNX` | +15.3% | Healthcare |  | never_targeted |  |  |  |  |
| 2 | `EMAT` | +14.8% | Basic Materials |  | never_targeted |  |  |  |  |
| 3 | `HLP` | +13.7% | Basic Materials |  | never_targeted |  |  |  |  |
| 4 | `DVLT` | +12.6% | Technology |  | never_targeted |  |  |  |  |
| 5 | `BRUN` | +11.7% | Technology |  | never_targeted |  |  |  |  |
| 6 | `AXTI` | +11.4% | Technology |  | never_targeted |  |  |  |  |
| 7 | `ARQT` | +11.2% | Healthcare |  | never_targeted |  |  |  |  |
| 8 | `FPS` | +11.1% | Industrials |  | never_targeted |  |  |  |  |
| 9 | `SMTC` | +11.0% | Technology |  | never_targeted |  |  |  |  |
| 10 | `SABR` | +11.0% | Technology |  | never_targeted |  |  |  |  |
| 11 | `CIFR` | +10.8% | Technology |  | never_targeted |  |  |  |  |
| 12 | `EROC` | +10.6% | Industrials |  | never_targeted |  |  |  |  |
| 13 | `AIB` | +10.0% | Technology |  | never_targeted |  |  |  |  |
| 14 | `CYPH` | +9.9% | Healthcare |  | never_targeted |  |  |  |  |
| 15 | `BYND` | +9.6% | Consumer Defensive |  | never_targeted |  |  |  |  |

### 2026-09-17 · recall 0% (0/15) · top-5 hits 0 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP2 NE

KEEP hot4: `AZTA`, `RVTY`, `IT`, `FIVN`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `DCX` | +37.5% | Consumer Cyclical |  | never_targeted |  |  |  |  |
| 2 | `SDGR` | +26.4% | Healthcare |  | never_targeted |  |  |  |  |
| 3 | `TLSA` | +26.0% | Healthcare |  | never_targeted |  |  |  |  |
| 4 | `CYPH` | +25.8% | Healthcare |  | never_targeted |  |  |  |  |
| 5 | `USDE` | +24.4% | Financial |  | never_targeted |  |  |  |  |
| 6 | `INDP` | +22.4% | Healthcare |  | never_targeted |  |  |  |  |
| 7 | `LVWR` | +19.3% | Consumer Cyclical |  | never_targeted |  |  |  |  |
| 8 | `GNRC` | +18.3% | Industrials |  | never_targeted |  |  |  |  |
| 9 | `SWRD` | +17.7% | Financial |  | never_targeted |  |  |  |  |
| 10 | `VICR` | +17.7% | Technology |  | never_targeted |  |  |  |  |
| 11 | `VITL` | +16.6% | Consumer Defensive |  | never_targeted |  |  |  |  |
| 12 | `EYPT` | +15.7% | Healthcare |  | never_targeted |  |  |  |  |
| 13 | `BHVN` | +15.6% | Healthcare |  | never_targeted |  |  |  |  |
| 14 | `ABSI` | +15.3% | Healthcare |  | never_targeted |  |  |  |  |
| 15 | `SECZ` | +14.9% | Technology |  | never_targeted |  |  |  |  |

### 2026-09-18 · recall 0% (0/15) · top-5 hits 0 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP2 NE

KEEP hot4: `ARQT`, `ILMN`, `FTRE`, `SDGR`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `TJGC` | +53.0% | Communication Services |  | never_targeted |  |  |  |  |
| 2 | `USDE` | +32.2% | Financial |  | never_targeted |  |  |  |  |
| 3 | `GEMI` | +31.1% | Financial |  | never_targeted |  |  |  |  |
| 4 | `FWDI` | +24.2% | Financial |  | never_targeted |  |  |  |  |
| 5 | `SECZ` | +21.6% | Technology |  | never_targeted |  |  |  |  |
| 6 | `FEAM` | +21.4% | Basic Materials |  | never_targeted |  |  |  |  |
| 7 | `DFDV` | +21.2% | Financial |  | never_targeted |  |  |  |  |
| 8 | `CYPH` | +17.3% | Healthcare |  | never_targeted |  |  |  |  |
| 9 | `CAN` | +16.8% | Technology |  | never_targeted |  |  |  |  |
| 10 | `MSTR` | +16.4% | Technology |  | never_targeted |  |  |  |  |
| 11 | `BKKT` | +15.4% | Technology |  | never_targeted |  |  |  |  |
| 12 | `BTDR` | +15.4% | Technology |  | never_targeted |  |  |  |  |
| 13 | `AMTX` | +14.9% | Basic Materials |  | never_targeted |  |  |  |  |
| 14 | `LVWR` | +14.8% | Consumer Cyclical |  | never_targeted |  |  |  |  |
| 15 | `ORBS` | +14.7% | Consumer Cyclical |  | never_targeted |  |  |  |  |

## How to extend the reverse-run

```
python -m src.gainer_reverse_audit --dates YYYY-MM-DD,YYYY-MM-DD --write
```

Optional: `python -m src.ticker_lookback_run --tickers SDGR,GNRC` paints the 12 09:30 boxes on one name. `python -m src.gainer_lookback_action --write` is the full-history BUY/SELL catch board (stale through 09-08 until restamped).

Live `flatten_robust`, hard-red sit, and Webull paper stay sit.

