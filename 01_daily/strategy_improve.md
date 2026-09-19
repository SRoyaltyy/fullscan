# Strategy improve — gainer reverse-run

_Generated 2026-09-19T07:47:11-04:00 — research audit, not a wire._

Liquid Finviz top-15 gainers (Change% ≥ 5%, mcap ≥ $100M, adv ≥ 500k) run back through the 09:30 packet that already printed. Same-day Change% only picks the universe.

## When is a strategy improved?

All four bars below must be green on a rolling 10-session window. Book% / fill-reality ideal / general direction-call do **not** count.

### `live_up_not_empty` — Live book is in the market on UP mornings · **FAIL**

- Pass: On a session with morning S > 0, leftover cash ≥ $10k, and not hard-red: flatten_robust prints at least one live BUY (n_priced_buys ≥ 1 or a 09:30 ticket). io 3d must be able to schedule an exit (session calendar extends ≥ 3 sessions past D).
- Fail looks like: S positive and $101k leftover with 0 priced BUYs / `io 3d cannot settle` is a sit, not a strategy call.
- This window: flatten_robust sit=True on an audited UP window is a fail when leftover cash could buy 1 share (see sleeve today.json).

### `fat_day_keep_oc` — KEEP longs are not red from the 09:30 open on fat index days · **PASS**

- Pass: On sessions with SPX close-to-close ≥ +0.80%: equal-weight open→close of that morning's KEEP long list (union_hot_n4_h1, else combo_sh longs) ≥ SPX open→close − 0.50 pp. Gap days are scored from the open, not from the prior close.
- Fail looks like: Thursday 09-17: SPX +1.14% close-to-close / +0.08% from the open; hot4 AZTA/RVTY/IT/FIVN −1.06% from the open.
- This window: 1/1 fat days: KEEP OC ≥ index OC − 0.50pp. C2C fat days are often the overnight gap; 09:30 longs only get the leftover OC. 2026-09-17 c2c=+1.14 gap=+1.05 idx_oc=+0.08 keep_oc=+5.49

### `gainer_recall` — Liquid rippers show up on a morning list · **FAIL**

- Pass: Rolling 10 sessions: of each day's liquid top-15 (Change% ≥ 5%, mcap ≥ $100M, adv ≥ 500k), ≥ 25% are a hit, and ≥ 1 of the day's top-5 is a hit on at least 6 of those sessions. Hit = in stock-book 1d BUY ∪ flatten/KEEP tickets ∪ news |net| ≥ 1 ∪ a usable catalyst dossier.
- Fail looks like: Same-day Change% is the universe only. A name that ripped with every camera dark and no list seat is a miss.
- This window: mean recall 22% vs 25% bar; top-5 hit on 3/3 sessions

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

## Why long-only books miss highly positive days

Two stacked reasons, in order:

1. **The fat print is the overnight gap.** A 09:30 long buys after the gap. Thursday 09-17 SPX +1.14% close-to-close was +1.05% before the open and +0.08% from 09:30→16:00. `union_hot_n4_h1` is hold=1 — it never carries last night's lot into that gap. `union_hot_n4_holdup` keeps S>0 lots through the next 09:30 so the *next* gap is in the book. `union_e_fresh_h3` already holds 3.
2. **KEEP hot4 is leftover tape, not index beta.** On the 09-03 grind fat day (SPY +0.69% from the open) hot4 GPRO/REAX/CNH/MMED was **−5.28%**. The same morning `union_e_fresh_h3` (AVGO/CIEN/FIVE…) was **+4.07%** and `union_news_pack_net2_h1` (AVGO/DELL/HPE) was **+7.37%**. Event / news longs participated. Hot-score micro names faded.

Morning S does not call fat grind days (09-03 S=−0.9). S=+7 on 09-17 was after the gap was already printed.

## How to find overnight bangers

Find the bang *before* 09:30: buy D from the prior Finviz earnings calendar (AMC today / BMO next). Unfiltered liquid calendar is a coin flip. Mega-cap ($50B+) is the one-way slice. Index-like nights are yesterday's liquid winners (holdup / yday_gainer), not the earnings lottery. earn_react / e_fresh are after the print.

Two different nights, two different lists:

1. **Print night** — AMC today / BMO next from the prior export. `overnight_mega_h1` keeps mcap ≥ $50B. The full liquid calendar has more −5% dumps than +5% bangs. `earn_react` / `e_fresh` fire *after* the print — they miss the bang.
2. **Index-like night** — yesterday's liquid winners (`yday_gainer_h1` / `union_hot_n4_holdup` when S>0). Thursday 09-17's +1.05% SPX gap was this kind of night: scheduled AMC leftover was ALMU/LEN, not mega-cap beta.

Lookback 2026-08-14 → last closed (24 nights; D close → D+1 open EW):

- scheduled liquid `overnight`: -0.95% on 20 nights with names
- scheduled mega `overnight_mega` ($50B+): +1.13% on 12 nights
- yesterday's liquid 25: +1.75%
- post-print `earn_react`: -0.04%

Audited window nights:

- 2026-09-15→2026-09-16: overnight n=1 gap=+4.28%; mega n=0 gap=—; yday25 gap=-0.19%; earn_react gap=+2.20%
- 2026-09-16→2026-09-17: overnight n=2 gap=-6.58%; mega n=0 gap=—; yday25 gap=+1.30%; earn_react gap=+0.89%
- 2026-09-17→2026-09-18: overnight n=0 gap=—; mega n=0 gap=—; yday25 gap=+0.84%; earn_react gap=-0.50%

## Reverse-run

### 2026-09-16 · recall 27% (4/15) · top-5 hits 2 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP1 NE

KEEP hot4: `INDP`, `SWKS`, `GPRO`, `INSP`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `BBNX` | +15.3% | Healthcare | Y | captured |  | Y |  |  |
| 2 | `EMAT` | +14.8% | Basic Materials |  | never_targeted |  |  |  |  |
| 3 | `HLP` | +13.7% | Basic Materials | Y | captured |  | Y |  |  |
| 4 | `DVLT` | +12.6% | Technology |  | never_targeted |  |  |  |  |
| 5 | `BRUN` | +11.7% | Technology |  | never_targeted |  |  |  |  |
| 6 | `AXTI` | +11.4% | Technology |  | never_targeted |  |  |  |  |
| 7 | `ARQT` | +11.2% | Healthcare | Y | captured |  | Y |  |  |
| 8 | `FPS` | +11.1% | Industrials | Y | captured |  | Y |  |  |
| 9 | `SMTC` | +11.0% | Technology |  | never_targeted |  |  |  |  |
| 10 | `SABR` | +11.0% | Technology |  | never_targeted |  |  |  |  |
| 11 | `CIFR` | +10.8% | Technology |  | never_targeted |  |  |  |  |
| 12 | `EROC` | +10.6% | Industrials |  | never_targeted |  |  |  |  |
| 13 | `AIB` | +10.0% | Technology |  | never_targeted |  |  |  |  |
| 14 | `CYPH` | +9.9% | Healthcare |  | never_targeted |  |  |  |  |
| 15 | `BYND` | +9.6% | Consumer Defensive |  | never_targeted |  |  |  |  |

### 2026-09-17 · recall 13% (2/15) · top-5 hits 1 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP2 NE

KEEP hot4: `INDP`, `GPRO`, `INSP`, `TJGC`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `DCX` | +37.5% | Consumer Cyclical |  | never_targeted |  |  |  |  |
| 2 | `SDGR` | +26.4% | Healthcare | Y | captured |  | Y |  |  |
| 3 | `TLSA` | +26.0% | Healthcare |  | never_targeted |  |  |  |  |
| 4 | `CYPH` | +25.8% | Healthcare |  | never_targeted |  |  |  |  |
| 5 | `USDE` | +24.4% | Financial |  | never_targeted |  |  |  |  |
| 6 | `INDP` | +22.4% | Healthcare | Y | captured |  | Y |  |  |
| 7 | `LVWR` | +19.3% | Consumer Cyclical |  | never_targeted |  |  |  |  |
| 8 | `GNRC` | +18.3% | Industrials |  | never_targeted |  |  |  |  |
| 9 | `SWRD` | +17.7% | Financial |  | never_targeted |  |  |  |  |
| 10 | `VICR` | +17.7% | Technology |  | never_targeted |  |  |  |  |
| 11 | `VITL` | +16.6% | Consumer Defensive |  | never_targeted |  |  |  |  |
| 12 | `EYPT` | +15.7% | Healthcare |  | never_targeted |  |  |  |  |
| 13 | `BHVN` | +15.6% | Healthcare |  | never_targeted |  |  |  |  |
| 14 | `ABSI` | +15.3% | Healthcare |  | never_targeted |  |  |  |  |
| 15 | `SECZ` | +14.9% | Technology |  | never_targeted |  |  |  |  |

### 2026-09-18 · recall 27% (4/15) · top-5 hits 2 · dossiers 0/8 · flatten **sit**

Catalyst targets: `NE`, `RIG`, `SLB`, `BKR`, `KGS`, `WHD`, `CNR`, `BTU`

Dossier error: OpenClaw and DeepSeek both returned empty for CATALYST STEP2 NE

KEEP hot4: `INDP`, `BNC`, `GPRO`, `INSP`

| # | Ticker | Δ | Sector | Hit | Why | Book | KEEP | News | Cat |
|---:|---|---:|---|---|---|---|---|---|---|
| 1 | `TJGC` | +53.0% | Communication Services |  | never_targeted |  |  |  |  |
| 2 | `USDE` | +32.2% | Financial | Y | captured |  | Y |  |  |
| 3 | `GEMI` | +31.1% | Financial |  | never_targeted |  |  |  |  |
| 4 | `FWDI` | +24.2% | Financial |  | never_targeted |  |  |  |  |
| 5 | `SECZ` | +21.6% | Technology | Y | captured |  | Y |  |  |
| 6 | `FEAM` | +21.4% | Basic Materials |  | never_targeted |  |  |  |  |
| 7 | `DFDV` | +21.2% | Financial |  | never_targeted |  |  |  |  |
| 8 | `CYPH` | +17.3% | Healthcare | Y | captured |  | Y |  |  |
| 9 | `CAN` | +16.8% | Technology |  | never_targeted |  |  |  |  |
| 10 | `MSTR` | +16.4% | Technology |  | never_targeted |  |  |  |  |
| 11 | `BKKT` | +15.4% | Technology |  | never_targeted |  |  |  |  |
| 12 | `BTDR` | +15.4% | Technology |  | never_targeted |  |  |  |  |
| 13 | `AMTX` | +14.9% | Basic Materials |  | never_targeted |  |  |  |  |
| 14 | `LVWR` | +14.8% | Consumer Cyclical | Y | captured |  | Y |  |  |
| 15 | `ORBS` | +14.7% | Consumer Cyclical |  | never_targeted |  |  |  |  |

## How to extend the reverse-run

```
python -m src.gainer_reverse_audit --dates YYYY-MM-DD,YYYY-MM-DD --write
```

Optional: `python -m src.ticker_lookback_run --tickers SDGR,GNRC` paints the 12 09:30 boxes on one name. `python -m src.gainer_lookback_action --write` is the full-history BUY/SELL catch board (stale through 09-08 until restamped).

Live `flatten_robust`, hard-red sit, and Webull paper stay sit.

