# How fullscan works, explained like you're 14

This document explains the whole repository in plain language, but it does not
skip the actual mechanics. Every number, weight and threshold below was copied
from the code (file paths are given so you can check). Every case study uses
real files that are committed in this repo.

If you only read one paragraph, read the next one.

---

## 0. The one-paragraph version

Every weekday morning before the US stock market opens, a set of robots
(Python scripts on GitHub Actions plus a small always-on server) wake up and
do three jobs. **Job 1:** guess whether the whole US market (the S&P 500) will
close up, down, or flat today, and by roughly how much. **Job 2:** make the
same guess for each of the 11 sector ETFs (energy, tech, healthcare, and so on).
**Job 3:** rank about 2,000 liquid stocks and print a BUY list and a SELL list
for several holding periods (1 day, 3 days, 1 week, 2 weeks, 1 month). After
the market closes, the robots check what actually happened, grade themselves,
write a short "lesson" about what went wrong, and feed those lessons into
tomorrow's guesses. There are also several side labs (paper trading, pattern
mining, a spreadsheet clone) that test whether any of this actually makes
money. So far the honest answer is: **the grading and learning machinery works;
the predictions themselves are roughly coin-flip accurate.**

---

## 1. What the system is trying to do, and the three things it produces

Think of it like a weather service for stocks.

- A real weather service says "70% chance of rain tomorrow." This system says
  "the market is DOWN/mild today with confidence 0.65."
- A weather service also says "good day for the beach" for different
  *kinds* of people. This system says "today is hostile for small unprofitable
  high-beta stocks, favorable for large profitable low-beta ones."
- Then, for each individual stock, it combines "what kind of stock is this"
  with "is today good for that kind" plus stock-specific news and chart data,
  and prints a ranked list.

The three daily outputs:

| Output | Where it lands | Made by |
|---|---|---|
| **General market prediction** (up/down/flat + mild/notable/severe + confidence) | `01_daily/general/<date>_predict.md` | `src/run_predict.py` + `src/compute_scores.py` |
| **11 sector predictions** | `01_daily/sectors/<date>/<sector>_predict.md` | `src/run_sector_predict.py` + `src/compute_sector_scores.py` |
| **Stock book** (BUY/SELL per horizon, plus a "decision lattice" of why) | `01_daily/<date>_stock_book.md`, `data/stock_book/<date>_stock_book.json` | `src/stock_book.py`, `src/decision_lattice.py`, `src/green_pile.py` |

---

## 2. The daily clock

All times are New York time. This is what `src/run_preopen_all.py` and
`src/run_postclose_all.py` actually run, in order.

**Night before (about 10:00 PM):** "Map Heat Captain Research." For every
industry group, an LLM (Grok, with DeepSeek as backup) researches the two most
important "captain" stocks and writes a cited baseline for tomorrow. This is
stored in `01_daily/map_heat/`.

**5:55 AM — Pre-Open ALL** (the main morning run, fired by a systemd timer on
an Alibaba Cloud server, with GitHub Actions as backup):

1. **Labels** (`src/segments.py`): read the Finviz export of ~6,000 stocks and
   stamp every stock with tags like `size:small`, `beta:high`, `ext:extended`.
2. **Weather** (`src/weather.py`): decide, for each tag, whether today is
   favorable / neutral / hostile / unknown.
3. **Join** (`src/join.py`): labels × weather = a first ranked list.
4. **AB checklist** (`src/ab_checklist.py`, `src/ab_enrich.py`): 29 yes/no
   chart-and-fundamentals checks per stock.
5. **News parse** (`src/news_parse.py`): pull the last 48 hours of headlines
   (up to 400) from the database.
6. LLM essays, each with a hard time limit so one hung call cannot eat the
   morning: **event scanner**, **event catcher**, **news actions**,
   **news judge**, then the **general market predict**, then **all 11 sector
   predicts** (2400 s budget), then the **map-heat morning refresh**.
7. Weather and join are re-run so they can see the fresh essays.
8. **Stock book** (`src/run_stock_book_all.py` → `src/stock_book.py`) ranks the
   universe and writes BUY/SELL.
9. The live BUY/SELL strip is published to the dashboard, then paper trading,
   sleeve-combine and the "flatten" card run.
10. **Catalyst dossiers** (`src/catalyst_daily.py`) run last, after the book is
    on disk, because on 2026-09-02 eight dossiers ate the whole morning and the
    ranker never started.
11. A QC report is written. A Grok "text review" reads every output as text and
    fails the packet if a file is a stub, a timeout, or a carry-forward.

The hard cutoff is **09:25 AM**. After that, LLM essays are skipped so nothing
is "predicted" after the market is already trading.

**5:00 PM onwards — Post-Close ALL:** general **outcome** (grade the call),
**horizon grade**, general **reflect** (write the lesson), the same three for
each **sector**, the **sector board**, the **news-actions grader**, the
**HIT board**, the **learn cycle**, and finally the night captain research
for tomorrow.

**Sundays 11 AM / 1st of the month (designed, currently switched off):**
candidate lessons that fired at least twice get promoted to standing rules,
and the whole memory is re-distilled into a short `04_consolidated_memory.md`.
Both schedules were disabled on 2026-08-29; promotion now happens inside the
nightly learn cycle instead (see Section 7.2), and the consolidated memory
was last rewritten on 2026-08-01.

---

## 3. Where the raw information comes from

The system is careful to separate two channels, because an LLM is bad at
arithmetic and good at reading.

**Channel 1 — pre-fetched numbers (the LLM may not change these).**
`src/fetch_channel1.py` pulls from FRED (the Federal Reserve's data API) and
Yahoo Finance: VIX and VIX3M (fear gauges), the 10-year and 30-year Treasury
yields, real yields, high-yield credit spreads, the dollar index (DXY), oil
(CL=F and BZ=F), S&P and Nasdaq futures (ES=F, NQ=F), how Asian and European
markets did overnight, the CNN Fear & Greed index, and a pre-computed 5-day
correlation between the 10-year yield and the S&P 500.

**Channel 2 — live research (the LLM's job).** The LLM must search the web
for: overnight geopolitical events, Fed speeches in the last 24-48 h, big
earnings, sovereign credit actions, China data, and "anything else." If it
finds nothing in a category it must literally write "checked, nothing
material" — it is not allowed to just skip a category.

**The Finviz Elite export** (`data/exports/finviz_<date>.csv`) is the
backbone of the stock layer: one row per stock with market cap, average
volume, beta, short float, RSI, moving averages, performance over week/month/
quarter, earnings date, EPS surprise, analyst rating, and a one-line company
description. Everything in Sections 6.1–6.3 is computed from these columns.

**Prices** live in `data/prices/ohlc.parquet` (~3 million daily bars) and
are what the graders and paper traders use.

**News** comes from Postgres tables filled by the `collectors/` scripts (RSS,
NewsAPI, Reddit, SEC filings) — see `schema.sql`.

---

## 4. Engine 1: the general market predictor

### 4.1 The idea

Imagine a judge scoring nine different pieces of evidence about today, each on
its own small scale, then a calculator adds them up with fixed weights. The
judge is the LLM; the calculator is `src/compute_scores.py`. The LLM is
**forbidden** from computing the total itself (the prompt in
`00_grounding/master_rubric.md` says so explicitly). This split exists so the
arithmetic is auditable and so the LLM cannot "talk itself into" a total.

### 4.2 The nine factors

Copied from `WEIGHTS` and `BOUNDS` in `src/compute_scores.py` (lines 13–25)
and the bands in `00_grounding/master_rubric.md`:

| Factor | What it measures | Allowed range | Weight | Plain-English band |
|---|---|---|---|---|
| **B0_ASIA** | How Asia closed overnight (Nikkei, Hang Seng, Shanghai, Kospi, ASX average) | −2 to +0.5 | ×2.0 | worse than −2%: −2 · −1 to −2%: −1 · −0.5 to −1%: −0.5 · within ±0.5%: 0 · better than +0.5%: +0.5 |
| **B0_EUROPE** | Same for Europe (session still running, so "partial") | −2 to +0.5 | ×2.0 | same bands |
| **B1_CATALYSTS** | Overnight news: wars, Fed surprises, mega-cap earnings, downgrades | −3 to +3 | ×3.0 | −3 severe risk-off shock … 0 neutral … +3 severe risk-on |
| **B2_BONDS** | What 10y/30y yields did, read through the current regime | −2 to +2 | ×2.0 | judged |
| **B3_FEDPATH** | Did rate-hike/cut odds shift hawkish (bad) or dovish (good) | −2 to +2 | ×2.0 | judged; also outputs a high/low confidence flag |
| **B4_VIX** | Fear gauge level and shape | −2 to +0.5 | ×1.5 | VIX>30: −1.5 · 25–30: −1 · 20–25: −0.5 · 15–20: 0 · <15: +0.5; extra −0.5 if VIX/VIX3M > 1.0 ("backwardation" = acute stress) |
| **B5_SENTIMENT** | Fear & Greed index (flagged as yesterday's close) | −1 to +1 | ×1.0 | Extreme Fear −1 · Fear −0.5 · Neutral 0 · Greed +0.5 · Extreme Greed +1 |
| **B6_FUTURES** | ES/NQ premarket move | −0.5 to +0.5 | ×0.5 | confirmation only, deliberately tiny |
| **B7_OIL_DOLLAR** | Oil spike + dollar surge is bad for stocks | −1 to +1 | ×1.0 | judged |

Notice the asymmetry: Asia/Europe/VIX can go much more negative than
positive. That is on purpose: the tool was originally a "crash detector."

Maximum possible |total| is about 30 at multiplier 1.0 (the comment on line 30
says so); an "ordinary all-slightly-green day" is calibrated *not* to reach
"severe."

### 4.3 Regime and multiplier

Before scoring, the LLM must declare a **regime**:

- *"Good news = good news"*: strong economy data makes stocks go up.
- *"Bad news = good news"*: weak data makes stocks go up because it means the
  Fed will cut rates.

If it cannot tell from the last CPI/PCE/jobs print, it uses Channel 1's
5-day yield-vs-S&P correlation: same direction → good=good, opposite →
bad=good. On 2026-09-10 the correlation was −0.969, so the regime was
"bad news = good news."

Then it picks an **uncertainty multiplier** between 0.5 and 2.0 from the VIX
and economic-policy-uncertainty levels. The pipeline clamps it to
[0.5, 2.0] no matter what the LLM says (`MULT_MIN, MULT_MAX = 0.5, 2.0`).

### 4.4 The divergence rule

Futures are "thin and synthetic," so the rubric distrusts them when real
markets disagree. Code (`compute_scores.py` lines 112–119):

```
leading = B1×3 + B2×2 + (B0_ASIA + B0_EUROPE)×2
if leading <= -8.0 and B6_FUTURES >= 0:
    flag "LEADING/LAGGING DIVERGENCE"
    remove B6's contribution from the total (cap it to zero)
```

So if Asia, bonds and overnight news are all screaming "down" but futures are
flat or green, the system trusts the screaming and ignores the futures.
Section 9.1 shows a day where this fired and was right about direction but
wrong about size.

### 4.5 From total to a prediction

```
total = Σ (component × weight)           # minus the B6 cap if divergence
total = total × multiplier

direction:  total > +1.0 → up
            total < −1.0 → down
            otherwise    → flat            # DIRECTION_EPS = 1.0

magnitude (only if not flat):
            |total| ≥ 12 → severe
            |total| ≥ 7  → notable
            |total| ≥ 3  → mild
            else         → flat
```

**Worked example — 2026-09-10** (`01_daily/general/2026-09-10_predict.md`):

| Factor | Score | × Weight | = |
|---|---:|---:|---:|
| Asia | −0.5 | 2.0 | −1.0 |
| Europe | 0.0 | 2.0 | 0.0 |
| Catalysts (Iran attacked 10 ships, Brent >$102) | −3.0 | 3.0 | −9.0 |
| Bonds | −0.5 | 2.0 | −1.0 |
| Fed path (hawkish Warsh, ~55–60% Sep hike odds) | −1.0 | 2.0 | −2.0 |
| VIX (16.52 but backwardation 1.079) | −0.5 | 1.5 | −0.75 |
| Sentiment (data missing) | 0.0 | 1.0 | 0.0 |
| Futures | 0.0 | 0.5 | 0.0 |
| Oil & dollar | −1.0 | 1.0 | −1.0 |
| **Sum** | | | **−14.75** |
| × multiplier 0.9 | | | **−13.275** |

−13.275 < −1 → DOWN. |−13.275| ≥ 12 → SEVERE. Confidence 0.65.
Leading sum = −9 −1 −1 = −11 ≤ −8 and futures 0 ≥ 0 → divergence flagged.

### 4.6 How it gets graded

After the close, `src/run_outcome.py` fetches the S&P 500's daily bar
(`fetch_channel1.fetch_actual_close`) and computes the % change of **today's
close versus yesterday's close** (`_pct(close, prev_close)`). The scoreboard
row for 2026-09-11 shows `actual_open 7636.75, actual_close 7656.98, +0.86%`
— note that +0.86% is measured from the previous close, not from the open
(open→close would only be +0.26%). That matters: the system predicts at 9 AM,
after futures already moved overnight, but it is graded on the whole
close-to-close day. Then (`compute_scores.actual_band`, lines 143–162):

```
actual direction: > +0.1% up · < −0.1% down · else flat
actual band:      ≥2% severe · ≥1% notable · ≥0.3% mild · else flat
direction_hit  = predicted direction == actual direction
magnitude_hit  = predicted band == actual band
```

Each of the nine factors is also individually marked hit/miss/neutral by
whether its sign matched the day's sign. Everything is appended to
`03_scoreboard/scoreboard.json` (284 rows as of this writing).

### 4.7 Forced memory

Before predicting, `run_predict.py` injects the last 10 runs with their
hit/miss, the rolling accuracy, every active lesson, and the consolidated
memory. The **first line of output must be**
`MEMORY_CONFIRM: Reviewed prior runs from …; rolling accuracy X%; key
standing lesson: …`. If the model cannot write that line, it must output
`MEMORY CONTEXT MISSING` and the run aborts. This is how the system forces
itself to actually read its own history.

---

## 5. Engine 2: the eleven sector predictors

Same architecture, different components (`src/compute_sector_scores.py`):

| Component | Meaning | Range | Weight |
|---|---|---|---|
| **S0_SHARED_MACRO** | The general market's macro read, as it applies to this sector | −2..+2 | ×2.0 |
| **S1_SECTOR_FACTORS** | Sector-specific drivers (oil price for Energy, yields for REITs, China data for Materials…) | −3..+3 | ×3.0 |
| **S2_BREADTH** | How many stocks in the sector are participating | −2..+2 | ×2.0 |
| **S3_FLOWS_POSITIONING** | Fund flows, crowding, positioning | −2..+2 | ×1.5 |
| **S4_ETF_TAPE** | What the sector ETF itself did recently | −1..+1 | ×0.5 |

Same direction threshold (±1.0), same magnitude bands (3 / 7 / 12), same
multiplier clamp. The divergence rule is two-sided here: leading =
S1×3 + S0×2 + S2×2; if |leading| ≥ 6 and the ETF tape points the *other*
way, the tape's contribution is removed.

Each sector has its own grounding file (`00_grounding/sectors/energy.md`,
`technology.md`, …) with a SPINE (the factors that dominate S1), SECONDARY
factors, a MACRO MAP (how risk-on/off, dollar and real yields hit *this*
sector), a search priority list and a DO-NOT list. The system prompt is
`sector_method.md` + that file + the full taxonomy label list.

After Python computes the total, two deterministic gates in
`map_heat_research.decision_gate` can still overrule it:

- **Sector RS veto:** if the call is "up" but the Finviz sector tape is red
  on both the day and the week (or "down" with both green), force
  flat/flat and cap confidence at 0.55.
- **Calendar size gate:** on a high-impact macro day (or Technology on a
  mega-cap earnings day), a "notable"/"severe" call is capped to "mild" with
  confidence ≤ 0.65.

The outcome (`run_sector_outcome.py`) is graded on the sector ETF's
**absolute** close-vs-prior-close % move with the same 0.1% / 0.3% / 1% / 2%
bands. The ETF's move *relative to SPY* is recorded, and the essays talk
about it a lot, but `grade()` ignores it — Section 9.7 shows why that
distinction decides hits and misses.

Current scorecard from `03_scoreboard/scoreboard.json` (193 graded sector
runs): **44.6% direction, 32.1% magnitude.** Best: Consumer Cyclical 61%
(11/18), Healthcare 60% (9/15). Worst: Communication Services 28% (5/18),
Industrials 28% (5/18).

---

## 6. Engine 3: the stock book (the big one)

This is the part that turns "the market is DOWN/mild" into "buy CVS, sell
QTRX." It is built as a chain of small deterministic steps. The picture:

```
Finviz export ──► LABELS ──┐
                           ├──► JOIN (labels × weather) ──► s_join ─┐
General + sector ─► WEATHER┘                                        │
                                                                    │
AB checklist (29 chart/fundamental checks) ─────────► s_ab ─────────┤
Peer relative strength ─────────────────────────────► s_peer ───────┤
News actions + judge + Finviz digest ───────────────► s_news ───────┼─► weighted score per horizon
General predict × stock beta ───────────────────────► s_general ────┤        + s_heat + s_opp
Sector predict (+ event tilt) ──────────────────────► s_sector ─────┤
Map-heat captains / industry residual ──────────────► s_heat ───────┘
                                                                    │
                       GREEN PILE (all-green names) ◄───────────────┤
                       DECISION LATTICE (permission to trade) ◄─────┘
                                                                    │
                                        BUY list / SELL list per horizon
```

### 6.1 Step A — Labels: "what is this stock?"

`src/segments.py` reads the Finviz row and assigns one value per **family**.
Bin edges live in `00_grounding/segments.json`. The important ones:

| Family | Source column | Values (edges) |
|---|---|---|
| size | Market Cap | micro <$300M · small $300M–2B · mid $2–10B · large $10–200B · mega >$200B |
| beta | Beta | low <0.8 · mid 0.8–1.3 · high >1.3 |
| short | Short Float % | low <5 · mid 5–15 · high 15–25 · extreme >25 |
| liq | Price × Avg Volume | low <$10M/day · mid $10–100M · high >$100M |
| rvol | Relative Volume | quiet <0.8 · normal 0.8–1.5 · hot >1.5 |
| vol | ATR ÷ Price × 100 | low <2% · mid 2–5% · high >5% |
| profit | Profit Margin | no <0 · thin 0–10% · yes >10% |
| lev | Debt/Equity | low <0.3 · mid 0.3–1 · high >1 · neg_equity if negative |
| style | Sales growth, EPS growth, fwd P/E | growth if sales ≥15% or EPS growth ≥20%; value if fwd P/E ≤15 and sales <10%; else blend |
| mom | 50-day & 200-day SMA distance | uptrend if both positive · downtrend if both negative · else mixed |
| ext | Week perf, month perf, RSI | extreme if week ≥+100% or 40% above 50-SMA · extended if week ≥+40% or RSI ≥75 · washed if month ≤−25% or RSI ≤30 · else neutral |
| range | 52-week high/low | Fibonacci zone of (P−L52)/(H52−L52): deep_low <0.236 · low · mid · high · top >0.786 · breakout if at/above the 52w high |
| earn | Earnings Date | today · this_week (≤7 days) · later · past |
| earnsurp | EPS Surprise % | big_miss <−20 · miss −20..−5 · inline · beat 5..20 · big_beat >20 |
| analyst | Analyst Recom (1–5) | strong_buy <2 · buy 2–2.5 · hold 2.5–3.5 · sell >3.5 |
| themes | regex over Industry + description | nuclear_smr, optics_transceiver, data_center_power, hbm_memory, copper_metals, ai_capex, defense, semiconductor_equip, glp1_obesity, biotech, regional_bank, crypto, solar_renewable, housing, oil_gas |

Output: `data/universe/<date>_membership.csv` (one row per ticker) and a
human report `01_daily/<date>_universe.md`.

### 6.2 Step B — Weather: "is today good for that kind of stock?"

`src/weather.py` turns the day's macro signals into a stance
(**favorable / neutral / hostile / unknown**) for every label value. It uses
no LLM directly; it reads the LLM's outputs plus Channel 1. Thresholds from
`00_grounding/weather_rules.json`:

- **risk** = "on" if the general predict total ≥ +4.0, "off" if ≤ −4.0, else
  "mixed." If mixed, the news judge's `risk_tilt` can break the tie.
- **yields** = rising if the 10-year moved > +0.02 (2 bps) in a day, falling
  if < −0.02, else flat. (FRED data preferred over the LLM's bond score.)
- **dollar** = strong if DXY ≥ +0.15% on the day, soft if ≤ −0.15%.
- **VIX** = spiking if VIX/VIX3M ≥ 1.10, or VIX up ≥1.5 points, or VIX ≥ 25;
  falling if ratio ≤ 0.90 or VIX down ≥1.5; else calm.
- **oil** = rising if WTI ≥ +1% on the day, falling if ≤ −1%.
- **sector stance** = favorable if that sector's predict score ≥ +3.0, hostile
  if ≤ −3.0 (or, if no essay, from the Finviz median week performance with
  ±1.5% edges). The news judge can overwrite a sector to hostile/favorable.
- **events**: bullish/bearish counts for events with impact ≥ 3; China events
  need impact ≥ 4.

Then rules like these (all in `build_stances`, lines 384–606):

- risk-on **and** soft dollar → `size:micro` and `size:small` favorable.
  risk-off **or** strong dollar → they are hostile ("small caps de-rate first").
- risk-off → `size:large`/`mega` favorable, `profit:no` hostile (high
  confidence), `profit:yes` favorable, `beta:low` favorable, `beta:high`
  hostile, `ext:washed` hostile ("falling knives stay sharp"),
  `ext:extended`/`extreme` hostile, `range:top`/`breakout` hostile.
- risk-on **and** futures positive → a "trend day": `mom:uptrend` favorable,
  `range:top`/`breakout` favorable.
- yields falling → `style:growth` favorable; rising → `style:value` favorable,
  `style:growth` hostile, `lev:high` hostile.
- risk-on **and** Fear & Greed ≥ 75 → `profit:no` favorable ("junk rallies in
  froth").
- `short:high`/`extreme` are **multipliers, not directions**: favorable in
  risk-on (squeeze fuel), hostile in risk-off (stress amplifier).

Weather also emits **gates** (things that veto or flag regardless of stance):
earnings today = veto, `ext:extreme` in risk-off = veto, `liq:low` = flag.

Output: `01_daily/weather/<date>_weather.json` and a human `.md` with
emoji tables (🌤️ favorable, ⛅ neutral, 🌧️ hostile, ❔ unknown).

### 6.3 Step C — Join: labels × weather = one number per stock

`src/join.py` with `00_grounding/join_rules.json`. For each stock:

1. **Weather votes.** Each label value's stance maps to a vote
   (favorable +1, hostile −1, else 0) times the family weight:
   sector 2.5, mom 1.0, ext 0.9, size 0.8, beta 0.8, themes 0.8, profit 0.7,
   range 0.7, geo 0.6, short 0.5, vol 0.5, lev 0.5, style 0.5, index 0.0.
   Multi-tag families (themes, index) vote as the *mean* of their tags; a
   theme tag with no weather still gets a small +0.25×0.8 prior for being a
   theme at all.
2. **Label priors** (fire even when weather is unknown, so the list is never
   flat): mom uptrend +0.50 / downtrend −0.50; ext washed +0.40 / extended
   −0.25 / extreme −0.55; range deep_low +0.30 / breakout +0.20 / top −0.10;
   profit yes +0.25 / no −0.30; RSI oversold +0.35 / overbought −0.35;
   above 20-SMA +0.15 / below −0.15; quarter momentum up +0.20 / down −0.20.
3. **Intrinsic votes**: earnings surprise (weight 0.6: big_beat +1, beat +0.5,
   miss −0.5, big_miss −1), analyst (0.5), PEG (0.35), institutional
   ownership (0.25), sales growth (0.35), ROE (0.25).
4. **Gates**: `earn:today` → veto; `ext:extreme` in risk-off → veto;
   `liq:low` → score halved; high short + risk-on → flagged
   "squeeze_candidate."
5. **Z-score** the total across the universe and clip to ±2.5. That becomes
   `score_norm`, which the stock book later squashes with `tanh` into
   `s_join` in (−1, +1).

Output: `data/join/<date>_ranked.csv` and `01_daily/<date>_match.md`
("stocks wearing today's lucky badges, minus those wearing cursed ones").

### 6.4 The other five ingredients

**s_ab — the AB checklist** (`src/ab_checklist.py`, real implementation
pinned to commit `01d6380c…`). 29 features, each scored −1 / 0 / +1, summed.
Part A is chart-based from the price store, Part B is Finviz fundamentals:

- A01 RSI ≤30 (+1) or ≥70 (−1) · A02 RSI crossed up through 30 (+1) ·
  A03 crossed up/down through 50 (±1) · A04 crossed 70 either way (−1,
  "entering overbought = caution") · A05 last-two-session green body >
  red body (+1) · A06 same for volume · A07 relative volume ≥1.5 (+1) or
  <0.5 (−1) · A08 Bollinger position ≤−0.8 (+1) or ≥+0.8 (−1) · A09 above
  50-SMA · A10 20/50/80 SMA stack bullish (+1) or bearish (−1) · A11 the
  lowest lows of three 21-day sections are rising or flat (+1), or spread
  >25% apart (−1) · A12/A13 green body vs wick, red body vs wick ·
  A15 "tape recovery" (2-day body ratio >1.4, red wicks >1.15× green wicks,
  biggest green body > biggest red body).
- B01/B02 EPS and revenue surprise sign · B04 income >0 · B05 margin >0 ·
  B06 profitable · B08 analyst target price rose vs prior export ·
  B09 analyst recom ≤2.5 (+1) or ≥3.5 (−1) · B10/B11 insider buying and its
  change · B12 institutional buying · B13 short float ≥20% (+1, squeeze
  candidate) · B17/B18 last-two surprises both positive/negative.

`src/ab_enrich.py` adds four context columns P01–P04 (peer leading the week,
peers advancing, industry advancing, sector supportive) and a
`context_label` such as `LEAD,peers↑,ind↑` or `LAG`. The book uses
`s_ab = tanh(score / 8)`.

**s_peer — peer relative strength** (`src/peer_rs.py`). Each stock has up to
10 correlated peers (`data/peers/correlations.csv`).
`rs_week = stock's week % − median(peers' week %)`. The book uses
`s_peer = tanh(rs_week / 8)`. Positive = leading its own peer group.

**s_news** (`stock_book.py` lines 850–856). Three sources are merged into one
`net` per ticker: the LLM **news actions** (buy/sell with a weight), the
**news judge** (added only if not already inside the actions, to avoid
double counting), and a regex over the **Finviz daily digest** headline
(words like beat/upgrade/raises → +1.6; miss/downgrade/cut → −1.6).
`s_news = tanh(net / 5)`.

Where those inputs come from, in order:

1. **News parse** (`src/news_parse.py`) is *not* an LLM. It pulls the last
   48 h of headlines from the Postgres `news` table (RSS, NewsAPI, Reddit
   collectors) and uses regexes to class each one as noise / single-name /
   sector-relevant / macro-relevant with a polarity (+, −, mixed, neutral),
   then buckets them by sector and macro theme. Its output is a summary of
   the news *landscape*, not a per-ticker score.
2. **News judge** (`src/run_news_judge.py`, `src/judge_apply.py`) is the LLM
   reading that landscape plus the Finviz digest. Its parsed output has a
   `risk_tilt` (on/off/none), `sector_tilts` (bullish/bearish/mixed/
   hawkish per sector), per-ticker scores (kept if ≥ 0.5), and a `B1_INJECT`
   paragraph the general predictor is given. Example 2026-09-09: risk_tilt
   off; Energy/Healthcare/Technology bullish, Utilities bearish; XLK +5.5,
   XLU −7.0.
3. **News actions** (`src/news_actions.py`) turn "event families" into
   edges: each event → bucket, side, weight, list of tickers (industries
   expanded to tickers through the Finviz universe), rolled into a per-ticker
   `net` and side. These are graded by `src/news_grade.py`: entry at the
   next open, window 14 trading days. Scoreboard: 1,053 suggestions, 1-day
   close win rate **54.1%**, 5-day **61.1%**, but "ever profitable within the
   window" 98.6% — a very loose measure the repo's own hypothesis file
   (`news_global_1d_weak.md`) flags as "barely better than a coin at one
   day."
4. **Catalyst dossiers** (`src/catalyst_daily.py`, `collectors/
   catalyst_analysis.py`) run for at most **8** tickers a day (mega-cap
   earnings first, then captains with conflicts, then the biggest |net|
   actions). Each is a multi-step LLM grid: every catalyst in a taxonomy is
   marked HIT/MISS with a weight and confidence; `Net = Σ(weight ×
   confidence/100)` for positives minus the same for negatives; Net ≥ 20
   Strong Bullish, ≥ 8 Bullish, ≥ −8 Neutral, ≥ −20 Bearish, else Strong
   Bearish; `conviction = min(100, 2×|Net|)`. A dossier feeds the actions
   book at ±3 (strong) scaled by conviction, and gives the lattice its
   strongest company evidence (0.80–1.0). In practice many dossiers fail to
   parse and land as error stubs (see Section 9.4).
5. **Events** (`src/run_events.py`, `run_events_catcher.py`) are the LLM's
   calendar: each event has a category, timing (past/today/upcoming),
   `expected_direction`, `impact` 1–5, regions and sectors. Weather counts
   events with impact ≥ 3 (China ≥ 4); the stock book adds
   ±0.08 × impact per named sector, clipped at ±0.20.

**s_general** = (general predict direction as ±confidence, floored at 0.15)
× an accuracy gate × the stock's **beta load** (high beta 1.0, mid 0.5,
low 0.15, unknown 0.4). A high-beta stock feels the market call more.

**s_sector** = the same ±confidence bias from that sector's predict × its
accuracy gate, plus an **event-scanner tilt** (impact ≥3 events add
±0.08×impact per named sector, clipped to ±0.20 so an event overlay cannot
flip an essay). For horizons beyond 1 day, the predict's `HORIZON_3D/1W/2W/1M`
lines are used instead of the 1-day call.

**Accuracy gates** (`_accuracy_gates`, lines 375–397): if a predictor's
rolling direction hit rate is below 45% on ≥3 runs, its bias is multiplied
by 0.5; below 55% → 0.85; else 1.0. So a predictor that keeps missing
literally gets less say in the stock book.

**s_heat** — map-heat "captains" (from `src/map_heat_research.py`) and the
industry's 1-week performance *residual* vs its parent sector. Scaled by a
learned `heat_scale` (currently 0.25, `book_policy.json`). How the map is
built (`src/map_heat.py`): the two largest stocks in every industry (S&P 500
members, plus Russell 2000 names with ≥ $5M/day dollar volume) are its
**captains**. Each industry's 1-day/1-week % is compared to its parent
sector; a residual ≥ 3 pp with |week| ≥ 2% is an **OVERRIDE** (child moving
against parent) or a **SPLIT**. Top/bottom 8 industries by week are
hot/cold. The night before, Grok researches every industry's captains in
chunks of 8; at 5:55 AM only hot/cold/override captains and earnings
captains (≤ 28) are refreshed. If the morning refresh produced ≥ 20 cards,
`s_heat` uses its per-ticker boosts; otherwise it falls back to ±0.20 tape
boosts on OVERRIDE captains.

### 6.5 Mixing them: the weighted score

Six weights per horizon, tuned inside a box (`stock_book.py` lines 36–43;
`00_grounding/book_policy.json` may nudge each by at most ±0.12):

| Horizon | join | sector | general | news | AB | peer |
|---|---|---|---|---|---|---|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 |

Read the pattern: **news matters most for 1 day and not at all for 1 month;
labels/weather and sector matter more the longer you hold; chart setup (AB)
and peer leadership matter a lot at every horizon; the general market call
is only 8%.** If a whole family is missing today (no AB file, say), its
weight is redistributed proportionally to the ones present
(`effective_weights`).

```
core_h  = w_join·s_join + w_sector·s_sector_h + w_general·s_general_h
        + w_news·s_news + w_ab·s_ab + w_peer·s_peer + s_heat
score_h = core_h + s_opp            # BUY-side tilt only
          + 0.08 if "rebound" flag  # REBOUND_BOOST
          − 0.10 if it was already on yesterday's BUY list and nothing fresh
                                    # PERSIST_PENALTY (breaks clones)
```

`s_opp` is an "opportunity" nudge for liquid mid/small names with room to run:
size small +0.16 / mid +0.32 / large −0.05 / mega −0.22; 52-week zone deep_low
+0.16 / low +0.12 / mid +0.08 / top −0.12; +0.08 for small/mid not extended;
+0.12 for small/mid that just beat earnings. Capped at **0.20** and zeroed
when the sector essay is hard-red (≤ −0.25), because an earlier version
"added +0.60–0.68 and bought Healthcare into a −0.50 sector call." SELL ranks
on `core_h` only, so mega-cap penalties cannot leak into the short list.

Two protective clips: a mid/small stock with real stock-specific evidence
(AB >0.10 or peer >0.20 or an earnings beat) cannot have `s_join` pushed
below −0.15 by a sector stamp ("ADBE → all Tech hostile must not bury a
mid-cap beating peers"); a join-vetoed name has its `s_join` multiplied by 0.2.

### 6.6 The green pile

`src/green_pile.py`. Instead of a beauty contest, first ask: is this stock's
tape *clean on every axis*? A name is **green** when
`s_join ≥ 0.05` and `s_general ≥ 0.05` and `s_ab ≥ 0.05` and
`s_peer ≥ 0.05`, sector and news are not red (> −0.05; missing is fine),
and Finviz relative volume is not in (0, 0.7) ("dead"). If at least **8**
liquid (≥$400M, not micro) green names exist, BUY is filled *only from the
pile*, ranked by `green_rank = mean(s_join, s_ab, s_peer)` with no weights
and no opportunity tilt. Otherwise the weighted walk above is used.

Note the consequence: because `s_general ≥ 0.05` is required, **on any day
the general predict is flat or down, the pile is empty by construction.** On
2026-09-11 (general = flat) the book says "Pile: 0 liquid all-green names
(need ≥ 8)." That is most days.

### 6.7 The decision lattice: permission before ranking

`src/decision_lattice.py` (added 2026-08-31) sits on top for the 1-day book.
Its complaint about the weighted score: "it blends unrelated evidence into one
score" so a great chart can average away a terrible market. The lattice
instead asks six questions in order, each answered RED / YELLOW / GREEN:

```
MARKET → PARENT (sector) → CHILD (industry/theme) → COMPANY → SETUP → FLOW
```

**Market** (`evaluate_market`, lines 158–289). Re-reads the *raw* nine-factor
scoreboard (not the accuracy-gated 8% version). Groups factors into seven
"pillars"; a pillar is red if its weighted points ≤ −0.20.

| State | Condition | Max longs | Position size | Lanes allowed |
|---|---|---|---|---|
| **HARD_RED** | direction down **and** total ≤ −3 **and** bad points ≤ −4 **and** weather risk off **and** ≥3 red pillars | 10 | ×0.25 | catalyst_exception, probable |
| **RED** | direction down and (risk off or total ≤ −1) | 8 | ×0.35 | group_leader, catalyst, probable |
| **GREEN** (hard) | direction up, total ≥ +3, good points ≥ +4, risk on, ≥3 green pillars | 15 | ×1.0 | standard, group_leader, catalyst |
| **GREEN** | direction up and risk not off | 15 | ×1.0 | same |
| **YELLOW** | everything else | 8 | ×0.60 | standard, group_leader, catalyst |

**Parent** (`_parent_eval`). Measured Finviz sector tape (1-day and 1-week %)
is kept *separate* from the LLM essay. Tape is good if week ≥ +2% with day
≥ 0, or day ≥ +0.5% with week ≥ 0; bad mirrored. Essay tone uses ±0.15. If
tape and essay disagree, the parent is YELLOW ("kept YELLOW"), not averaged.

**Child** (`_child_eval`). The industry's own 1d/1w tape (same edges) and its
**residual** vs the parent sector: ≥ +3 pp = relatively good, ≤ −3 pp =
relatively bad.

**Company** (`_company_eval`). One de-duplicated "direct event" decision.
Priority order: a usable **catalyst dossier** (strength 0.80–1.0) → a
**Finviz digest** headline that is directional and about *this* company
(strength 0.72 if high-materiality and same-day, 0.48 if high but stale,
0.42 if fresh, 0.30 otherwise; +0.10 if the news judge agrees, +0.06 if news
actions agree) → judge-only or actions-only evidence is capped at YELLOW
(≤0.40). The company is only GREEN/RED if strength ≥ **0.65**
(`DIRECT_EVENT_MIN`). **Price confirmation** = today's change or gap ≥ 0.5%
in the same direction (or ≥0.2% with relvol ≥1.5).

**Setup** (`_setup_eval`). Intrinsic AB (`tanh(score_base/8)`) plus a label
structure score (uptrend +0.5, washed +0.25, extended −0.25, extreme −0.5,
profitable +0.25, unprofitable −0.25, beat +0.25, big beat +0.5, miss −0.25,
big miss −0.5). RED if AB ≤ −0.05 or structure ≤ −0.75; GREEN if AB ≥ 0.10
and structure ≥ 0; else YELLOW.

**Flow** (`_flow_eval`). Peer RS plus today's price action and relative
volume. RED if relvol is dead after the open (0 < rvol < 0.7) or peers
lagging with price down; GREEN if peers ≥ 0.05 and price not down, or price
up ≥0.5% with peers not lagging, or relvol ≥ 1.5.

**Lanes** (`finalize_decisions`, lines 1116–1142). A stock is allowed onto
the BUY side only if it qualifies for a lane the market state allows:

- **catalyst**: direct company event, company GREEN, strength ≥0.65, setup
  not red, flow not red, no lookback vetoes. On HARD_RED it must also be
  price-confirmed and strength ≥0.70 (`catalyst_exception`).
- **group_leader**: industry good both absolutely and relative to parent,
  setup GREEN, flow/company not red. On a RED market it also needs today's
  price up ≥0.5%.
- **standard**: parent/child/company not red, setup GREEN, flow not red.
- **probable**: "most-probable long on a hostile tape": setup not red, no
  vetoes, and at least one *clock* — fresh company news (≥0.48), a strong
  child industry outperforming (1w ≥ +2% or residual ≥ +3) *with* a
  name-level signal, or a lookback 🔵/⚪ mark.

Inside a lane, names are ordered by

```
bull_rank = 10 (if eligible) + lane bonus (catalyst_exception 4, catalyst 3,
            group_leader 2, standard 1, probable 0.8)
          + 2×company_strength + group_strength + setup_strength + flow_strength
          + 0.20×(#green domains) − 0.35×(#red domains)
          + 0.80 if 🔵 blue + 0.50 if ⚪ white + 0.30 if Cond green
          − 1.50 if 🚨 alarm + 2.50 if fresh company news clock
```

A stock is **bear-eligible** (SELL/AVOID) if ≥2 of parent/child/company/setup/
flow are red, or child red with flow not green, or market red with setup red —
unless it has a strong positive direct catalyst (≥0.70), which vetoes the short.

The 🔵 / 🚨 / ⚪ marks come from the **ticker lookback** color engine
(Section 8.5): 🔵 = the stock's factor colors improved vs the prior session,
🚨 = purely worse, ⚪ = zero red cells.

### 6.8 Building the final list

`_book_side` (lines 1427–1535) walks the ranked, eligible names and applies
caps: skip micro caps and anything under **$400M**; at most **4 per sector**,
**3 per industry**, **4 large/mega** total (the rest must be small/mid);
stop at `top_n` (25 by default, or the market state's max slots for 1d).
SELL never includes a name that is on BUY. A **stand-down** empties BUY
entirely when the same-day general is down, weather is risk-off, bias ≤ −0.25
and zero usable company dossiers exist.

Tradeable universe gates before any of this: market cap ≥ **$80M**, average
volume ≥ **500k shares**, ATR% above a floor. On 2026-09-11 that left 2,059
names.

---

## 7. The learning loop: how it tries to get better

### 7.1 Outcome → Reflect → Lesson

Each afternoon `run_outcome.py` grades the morning and an LLM writes a
"post-market autopsy" listing what actually drove the day, with cited
sources (each URL is re-fetched to verify; unverifiable paywalled sources are
flagged, not deleted). Then `run_reflect.py` classifies any miss:

- **A — missing evidence** (something knowable at 9 AM was not looked at),
- **B — misweighted** (looked at it, weighed it wrong),
- **C — miscalibrated** (right direction, wrong size/confidence),
- **D — ops failure** (a file was missing; not a reasoning error).

It must run five checks: does an existing lesson already match? would the
proposed fix have helped on the last similar days (backward test)? does it
conflict with an active lesson? were applied lessons helpful or harmful? and
it must write a **falsifier** ("if X happens on 2 of the next 3 such days,
revise this lesson"). The result is a structured block
(`LESSON_BEGIN … LESSON_END` with TRIGGER_PATTERN, CURRENT_BEHAVIOR,
CORRECTED_BEHAVIOR, FALSIFIER, EVIDENCE) saved to
`02_lessons/candidate/<date>_lesson.md`.

### 7.2 Promotion and distillation

`src/promote_lessons.py` groups candidate lessons whose TRIGGER_PATTERN
text overlaps enough (Jaccard word-set similarity ≥ **0.5**) and promotes a
group into `02_lessons/active/` as a standing rule when it has **≥ 2**
complete market lessons (categories A/B/C) or **≥ 1** complete ops lesson
(category D). A lesson with a missing TRIGGER, CORRECTED_BEHAVIOR or
FALSIFIER is never promoted.

That is the *designed* rule. What actually runs is different, and it is
worth knowing: the Sunday workflow (`weekly_consolidation.yml`) and the
monthly distillation were **switched off on 2026-08-29** ("not in live
pipeline"). Instead the nightly `learn_cycle.py` calls the same clustering
code with `min_market=1` (`_promote_complete_candidates`), so **any single
complete candidate is promoted to active the same night**. That is why the
active folder has 201 files and the candidate folder 220: the "must recur
twice" filter is not being applied in practice. `src/distill_memory.py` (monthly)
rewrites `04_consolidated_memory.md` in under 1,500 words. The report card
already warns: "active-lesson pile is large — risk of narrow, contradictory
standing rules (overfitting); consider a cull."

### 7.3 Learn cycle and hypotheses

`src/learn_cycle.py` mines every win and loss into
`02_lessons/hypotheses/general_win_<date>_<dir>.md` /
`general_loss_<date>_<pred>_vs_<actual>.md` (and per-sector), and rewrites
`03_scoreboard/LEARNINGS.md` and `00_grounding/mutable_policy.md`, which the
next morning's prompt reads.

### 7.4 The stock book learns its own weights

`src/book_learn.py` is a walk-forward tuner: for each past book, look up what
the top-10 BUY names actually returned over each horizon minus the
liquid-universe median, and search for weights that would have done better.
Guardrails in `book_policy.json`:

- at least **5** realized dates,
- mean improvement ≥ **0.05 pp** (`eps_improve 0.0005`),
- the new weights must win on ≥ **60%** of dates,
- only move **half-way** toward the winner (`half_step 0.5`),
- never drift more than **±0.12** from the code defaults.

The history in `book_policy.json` (versions 1–13, 2026-08-22 → 09-11) shows
what this produced: **every horizon is "hold"** — the best candidate weights
won on only 21–43% of dates, never 60%. The weights in force are still the
code defaults. The risk-off entry scale stayed at 0.5 because "book loses
−0.37% on risk-off days." That is the guardrails doing their job: no
adoption without evidence.

`src/book_reflect.py` scans big movers the book missed and classes each as
**blind** (no input saw it), **outweighed** (an input saw it but was
outvoted), or **gated out** (a veto removed it), and maintains
`02_lessons/hypotheses/book_missing_inputs.md`.

### 7.5 Accuracy gates

As described in 6.4, a predictor whose rolling hit rate falls under 45% has
its influence on the stock book halved automatically. This is the only fully
automatic "trust less" mechanism in the system.

### 7.6 Did the lessons actually help? (`src/lesson_efficacy.py`)

For every active lesson, take the 7 graded runs of its topic *before* it was
promoted and the 7 *after* (need at least 4 on each side), and compare the
direction hit rate. Δ > +5 pp = improved, Δ < −5 pp = worse, else flat. The
last report (`03_scoreboard/LESSON_EFFICACY.md`, 2026-09-01):

> Active lessons: 123 · judged: 47 · improved: **3** · flat: 2 · worse:
> **42** · mean delta: **−0.269**

The file is careful to say this is correlation, not proof (the market got
harder in late August for every topic at once), but 42 of 47 judged lessons
being followed by *worse* accuracy is the single most important number in
the learning loop. "Worse" lessons are flagged as retirement candidates for
the monthly distill — which, as noted in 7.2, is currently switched off.
`distill_memory.py` is also where unpromoted candidates older than 60 days
would be moved to `02_lessons/archive/`.

---

## 8. The side labs

These do not feed the morning book; they test whether anything here makes
money.

### 8.1 Paper trading (`src/paper_trade.py`, `03_scoreboard/BOOK_PAPER.md`)

Rules: long only, top 10 names from the 1d BUY list, buy at the 4:00 PM
close, hold one week, 10% of equity per trade, Futubull commission model
(`00_grounding/futubull_fees.json`). **Day gate:** only trade when the
morning general score ≥ +1.0. Result 2026-08-13 → 2026-09-08: $100,000 →
**$104,122 (+4.12%)**, max drawdown 5.71%, **29 trades, 58.6% winners, 225
trades skipped by the gate.** The gate closed the book on 11 of 19 sessions.

### 8.2 The book's own backtest (`01_daily/<date>_stock_book_backtest.md`)

20 books graded, entry at the first close on/after the signal, exit N trading
days later:

| Horizon | BUY hit% | SELL hit% | avg book P&L |
|---|---|---|---|
| 1d | 43.3% | 57.5% | +0.12% |
| 3d | 40.3% | 62.0% | +0.30% |
| 1w | 40.3% | 59.8% | +0.07% |
| 2w | 37.5% | 70.2% | +0.69% |
| 1m | 27.9% | 74.9% | +0.56% |

The SELL side is right far more often than the BUY side; BUY accuracy *falls*
with holding period. That is a real, uncomfortable finding recorded by the
system itself.

### 8.3 Factor mine, boring winners, sleeve merge

**Factor mine** (`src/factor_mine.py`, `03_scoreboard/FACTOR_MINE.md`) is a
brute-force recipe tester. A "recipe" is: a universe (the book's BUY lists,
the flatten wish-list, the "probable" lane, yesterday's gainers, or a hot
OHLC screen), a hold (1/3/5 days), optional *require* gates (e.g. `vol good`,
`last bar green`, `earnings within 1 day`), optional *forbid* gates (almost
all forbid 🚨 and news🔴), and a side (long/short). It replays **235**
leak-free recipes on a $10,000 cash ledger with whole shares, Futubull fees,
09:30 fills and a "sit out when the market score ≤ −3" rule, 2026-08-13 →
09-10 (20 sessions). The two cleanest single recipes it found:

- `short_news_r_h3` — **short** any book name whose news cell is red, hold
  3 days: 59% win, +14.99% book, started on 17 of 20 days.
- `union_hot_n4_h1` — long the top 4 by "hot score", hold 1 day: 55% win,
  +17.40% book, but only started 10 of 20 days.

Most long-only recipes were flat-to-negative; the "coil" (quiet-then-pop)
ideas lost money (`union_coil_off_h1` −9.28%, `coil_h3_exit_alarm` −4.75%),
and the flatten wish-list only got to start on 6 of 20 days. `src/factor_mine_combo.py` then
mixes recipes on one shared cash pile: the best combos pair the
news-red **short** with a long sleeve — `combo_sh_5050_shared` (short-news-red
+ hot-4) made **+35.9%** with 61% wins, `combo_jse_333_shared` +32.2%. The
lesson the file itself draws: the short side is the ballast that makes the
longs survivable.

**Boring winners** (`src/boring_winners_lab.py`, `BORING_WINNERS.md`) takes
the same 1d BUY list and overlays the "stacks" the feature mine found
predictive: `hot+ab+peer` (70.6% hit, +3.14 mean, n=51), `steady+blue`
(52% hit, +9.54 mean, n=1,394), `blue+white`, plain `blue` (57.7%, n=3,387).
It keeps BUY names with a named stack, drops ones that printed `fade`, adds
at most 5 extras that pass the liquidity floor, and shorts only
`book SELL ∩ fade` (38.2% hit, −0.72 mean — i.e. those names really do fall).
Result over 19 priced days: the overlay's long side lost **−4.40%
cumulative** (mean −0.23/day) versus the plain stock-book BUY list's −2.94%
and a mine-only 25-seat fill's −1.06%; the short overlay made **+9.63%** over
7 days. The file's own verdict: the overlay "has not beaten the book on this
window."

**Sleeve merge / three-sleeve combine** (`SLEEVE_MERGE.md`,
`THREE_SLEEVE_COMBINE.md`) route between the three live books by the morning
general score S: S ≥ +1 → the *mover* sleeve (09:30 open, 1-day hold, top 10);
−3 ≤ S < +1 → the `.io` dashboard book (close fill, 2-week sizing);
S < −3 → no new 1-day risk, hold existing, shorts only. The merged
flatten-switch book made **+8.62%** (72 trades, 45.8% win) but *trailed* the
plain `.io` 2w book (+10.27%) and failed its own "15% per fortnight" target
(second fortnight −2.0%). The mover paper book alone did **+12.7%** with
51.7% wins, and the file is explicit that "the day gate (S ≥ +1.0) is the
whole product: it closed 10 sessions and blocked 3 days whose ungated top-10
basket was negative."

### 8.4 Excel bot (`excel_bot/`)

A zero-LLM Python replica of the owner's original `Simple View--Calculation.xlsx`
spreadsheet: it recomputes the spreadsheet's colour cells for ~3,600 tickers
from Yahoo OHLCV and fires seven strategies (L1–L5 long "green cluster,"
S1–S2 short "red cluster"). The README is candid: "wins are tail-driven: the
median trade is ~0, profit comes from a few +20–40% runners," and only the
mid-cap hold-2-day variants beat their backtest live.

### 8.5 Ticker lookback colours (`src/ticker_lookback.py`)

For any stock and any date, paint what the pipeline *knew at 9:30 AM* as a
row of red/yellow/green cells (join, vol, AB, peer, buy, sector, gen, news,
digest, judge, catal, heat) and show the next 1/3/5-day return. Marks:
🔵 when no cell got worse and at least one got better (or factor points
red=1/yellow=2/green=3 jumped ≥3), 🚨 when purely worse, ⚪ when zero red
cells. The market-wide mine of ~29,000 printed days found the cleanest signal
was **🚨 on a still-green row** (`first_crack`) as a fade; that "`turn`
(blue on a red row) did not clear the bar market-wide. The useful blue is a
blue tag on a mixed 3-day stretch"; and that "the same 🔵 / 🚨 / ⚪ flips
meaning on green vs red mass" — the tag alone is not predictive, the colour
stretch around it is what matters.

---

## 9. Case studies (all from committed files)

### 9.1 The market call that was right about direction and wrong about size — 2026-09-10

Section 4.5 showed the arithmetic: nine factors → −14.75 × 0.9 = **−13.275 →
DOWN / SEVERE**, confidence 0.65. The story: Iran attacked ten ships near
Hormuz after the US sank five tankers, Brent went above $102, a hawkish Fed
chair had hike odds at 55–60%, VIX was in backwardation, and PPI was due at
8:30 with CPI the next day. Futures were flat, so the divergence rule fired.

What happened (`2026-09-10_outcome.md`): S&P opened 7594.74, closed 7591.70,
**−0.58%** — DOWN, but only **MILD**. Direction ✅, magnitude ❌. The market
"gapped down and ground sideways" all day. Gold *fell* 1.25% on a war
escalation day, which the autopsy flagged as the day's biggest oddity.

The reflection (`2026-09-10_reflect.md`) classified it **Category B,
misweighted**, and found the embarrassing part: the model's own narrative
said "DOWN / MILD" while its emitted scores mapped to SEVERE, and two active
lessons that say "cap magnitude at MILD when futures don't confirm ≥0.5%"
were *cited in RULES_APPLIED but not enforced*. The backward test showed the
cap would have been right on 09-08 (−0.58%), 09-09 (−0.48%) and 09-01
(−0.71%) too. New candidate lesson: when B1 = −3 for a fresh oil/kinetic
shock but |futures| < 0.5%, force the band to MILD and multiplier ≤ 0.9.
Falsifier: if SPX instead closes ≥1% down on 2 of the next 3 such days,
revise. This is the learning loop working exactly as designed — and also
showing its weakness: the lesson existed already and was ignored.

### 9.2 The flat call that missed — 2026-09-11

Components: Asia −1.0, Europe +0.5, catalysts +1.0, bonds −0.5, Fed −0.5,
VIX −0.5, sentiment 0, futures +0.5, oil/dollar +1.0 → total **+0.5** → FLAT
(inside ±1). Actual: **+0.86%** (open 7636.75 → close 7656.98), a broad rally
that broke a four-day losing streak. Miss on both axes. The scoreboard's
divergence note is blunt: "the failure was band-mapping, not divergence
detection." The lattice that morning read the same scoreboard as **YELLOW**
(good +5.2 vs bad −4.8, risk off, 3 red pillars), allowing 8 long slots at
×0.60 size.

### 9.3 The stock book on a HARD_RED morning — 2026-09-02, CVS

`data/stock_book/2026-09-02_stock_book.json`, first BUY row:

| Ingredient | Value | Where it came from |
|---|---:|---|
| s_join | +0.99 | labels × weather, z-scored, tanh |
| s_ab | +0.93 | AB checklist (raw ≈ +13 of 29 → tanh(13/8)); context `LEAD, peers↑, ind↑` |
| s_ab_intrinsic | +0.85 | Part A/B without the P01–P04 context |
| s_peer | +0.51 | rs_week ≈ +4.5 pp vs its 10 peers |
| s_news | +0.31 | Finviz digest: "CVS beats Q2, raises 2026 EPS and cash-flow guidance" |
| s_general | −0.07 | general predict DOWN (−3.825, conf 0.52) × gate × beta load |
| s_sector | 0.00 | Healthcare essay missing that day (9/11 sectors landed) |
| relvol / change / gap | 1.08 / +3.93% / +1.15% | Finviz |
| lookback | 🔵 blue, Cond good | colour row improved vs prior session |

The lattice: market **HARD_RED** (general −3.825 ≤ −3, bad points ≤ −4, risk
off, ≥3 red pillars); parent Healthcare YELLOW; child Healthcare Plans GREEN
(+2.4% 1d, +3.6% vs parent); company YELLOW (digest strength 0.48 — high
materiality but stale/undated, below the 0.65 bar); setup GREEN; flow GREEN.
No lane but **probable** is open on HARD_RED, and CVS qualified through two
clocks: child outperformance and the 🔵 mark. `bull_rank` = 16.05, first in
the book, tagged "size ×0.25."

What happened: the general call was **wrong** — SPX closed **+0.46%** (SPY
open 762.45 → close 765.16). Of the ten 1d BUYs (CVS, CVE, CNQ, COR, BG, PBF,
ADM, OXY, EOG, CVX), **eight rose open→close that day** (COR +1.74%, BG
+2.21%, EOG +1.07%); CVS itself fell −0.60%. But the book's own backtest
counts that date as **1/10 BUY hits**, because it enters at the *close* and
exits at the next close, and nine of the ten fell on 09-03 while SPY rose
+1.41%. Same picks, two honest timing conventions, opposite verdicts. The
paper trader did not trade at all that day: the gate requires a general
score ≥ +1.0 and it was −3.825.

### 9.4 A catalyst lane in action — 2026-09-11, ORCL

Oracle's headline in the Finviz digest was same-day and high-materiality
("Q1 revenue up 30%, cloud infrastructure up 121%, raises FY27 guidance to
≥$90B revenue"). That is strength 0.72 ≥ 0.65 → company **GREEN**, direct,
fresh. Parent Technology GREEN, but child Software-Infrastructure **RED**
(−3.1% 1w, −4.4% vs parent). Setup and flow GREEN. On a YELLOW market the
catalyst lane is allowed and does not require the child to be green, so ORCL
was ranked #1 as `BUY CATALYST` with the lookback 🔵 and Cond green. The
other fourteen bull names that morning were all `group_leader` picks from the
hottest industries on the Finviz board (Electrical Equipment +13.6% 1w,
Semiconductor Equipment, Computer Hardware, Electronic Components). Price
data in the repo ends 2026-09-09, so this book is not yet graded here.

One detail worth noticing: ORCL was picked for a **catalyst dossier** on
09-09 and 09-10 (it is in the mega-cap earnings set), but both files in
`data/catalyst/` are error stubs — `"Step 1 parse failure"` and `"Step 4
parse failure"` — with no `net_signal` or conviction. So the "catalyst"
lane here was earned entirely from the Finviz digest headline (strength
0.72), not from the dossier engine that was designed to provide it. The
digest path is the one that actually works day to day.

### 9.7 A sector call that was right relative and wrong absolute — Energy, 2026-09-11

`01_daily/sectors/2026-09-11/energy_predict.md`: S0 = 0, S1 = −1 (crude
offered −2.5–3.4% after a record close and a crowded-long run), S2 = 0,
S3 = −0.5, S4 = 0, multiplier 0.9.

```
total = 2(0) + 3(−1) + 2(0) + 1.5(−0.5) + 0.5(0) = −3.75
       × 0.9 = −3.375  →  < −1 → DOWN;  |3.375| ≥ 3 → MILD
leading = 3(−1) + 2(0) + 2(0) = −3.0  (not ≤ −6, so no divergence)
```

(The essay's prose summed the components without weights to −1.35 and
claimed divergence was flagged; the Python footer is what counts, and it says
−3.375 and no flag.)

Outcome (`energy_outcome.md`): XLE **+0.32%** (64.89 → 65.14) while SPY rose
+0.85%, so XLE *lagged* by −0.53% — exactly the fade the essay expected. But
grading is on the absolute move: +0.32% > +0.1% → actual UP, ≥ 0.3% → MILD.
**Direction miss, magnitude hit.** The reflection filed it as Category A:
the model treated green futures as a headwind for Energy instead of a beta
tailwind, and the corrected behaviour is "score equity beta as an absolute
tailwind, let the barrel set the *relative* call only." This is the sector
engine's most common failure shape: relative reasoning graded on an absolute
scale.

### 9.5 The learning tuner that refused to learn

`book_policy.json` v3 (2026-08-27): "1d: hold — wins only 43% of dates
(< 60%). 3d: hold — 29%. 1w: hold — 20%." v13 (2026-09-11): "1d 33%, 3d 21%,
1w 33%, 2w 43%." Thirteen consecutive versions, zero adopted weight changes.
The one thing it did decide, on v12, was to *keep* the 0.5 risk-off entry
scale because the book "loses −0.37% on risk-off days." A learner that
changes nothing is boring, but it is behaving correctly: the evidence bar
(60% of dates) was never cleared.

### 9.6 The day the pipeline itself failed — 2026-09-08

`docs/autopsy-2026-09-05-08.md`. After Labor Day the morning run produced no
usable packet: the DeepSeek account returned "402 Insufficient Balance" so
every essay was empty, the news database timed out (silently reported as
"empty parse"), the map-heat refresh burned 21 minutes and hit the 09:25
cutoff, six competing GitHub runners raced to commit the same files, two
runs hung for hours and could not be cancelled, and GitHub Actions showed
**green** for a run whose own QC said `all_ok=False`. The stock book still
printed — with news, general and sector inputs all missing — and marked
itself `learn_grade=true`, meaning a book that never saw any essays was going
to be graded as if it had. Most of the timeouts and gates described in
Section 2 exist because of this day.

---

## 10. The honest scorecard

Numbers pulled from `03_scoreboard/scoreboard.json` and the report card:

- **General market direction: 48.3% (14/29 graded), magnitude 41.4%.**
  Last 10: 40% direction. The report card's naive baselines: "always guess
  up" 52%, "same as yesterday" 54%. Its own verdict: "engine is BEHIND the
  best naive baseline."
- **Sectors: 44.6% direction over 193 graded runs, 32.1% magnitude.**
- **Calibration:** when the model said 0.5–0.6 confident it was right 40% of
  the time (overconfident); at 0.6–0.7, 57% (fine); at 0.7–0.8, 100% on 11
  runs (underconfident).
- **Stock book BUY 1d hit 43%, SELL 1d hit 58%;** BUY decays to 28% at 1 month
  while SELL climbs to 75%.
- **Paper trading +4.12% in ~4 weeks with a 58.6% win rate on 29 trades**,
  but 225 skipped by the gate — a small sample dominated by a few winners.
- **News actions: 54.1% win at the 1-day close (n=950)**, rising to ~61% at
  3–5 days; the "98.6% ever profitable within 14 days" headline is a loose
  measure.
- **Book weight learner: 13 versions, 0 adoptions.**
- **Lesson efficacy: of 47 judged lessons, 3 improved their topic, 42 were
  followed by worse accuracy** (mean −27 pp; correlation, not proof).
- **Lessons: 201 active, 220 candidates**, flagged as overfitting risk. The
  "must recur twice" promotion filter is not applied by the nightly learn
  cycle (Section 7.2), and the 09-10 case shows lessons being cited and not
  enforced.
- **Operational:** the most detailed document in the repo is an autopsy of
  a morning the pipeline produced nothing while reporting success.

What genuinely works: the separation of LLM judgment from Python arithmetic,
the forced memory line, the deterministic grading, the guardrailed weight
tuner, the input-health renormalisation, the QC that treats "Actions green"
as meaningless, and the fact that every single decision leaves a file
explaining why. What does not yet work: the predictions beating a coin.

---

## 11. Glossary

- **ATR** — average true range; how much a stock typically moves per day.
- **Backwardation (VIX/VIX3M > 1)** — near-term fear higher than 3-month fear; acute stress.
- **Beta** — how much a stock moves when the market moves 1%.
- **bps** — basis points; 1 bp = 0.01%.
- **Captain** — one of the two most liquid, representative stocks in an industry group.
- **Channel 1 / Channel 2** — pre-fetched numbers vs live LLM research.
- **Divergence** — real overnight markets say one thing, US futures say another.
- **ES / NQ** — S&P 500 and Nasdaq-100 futures.
- **FRED** — the St. Louis Fed's economic data API.
- **Green pile** — names whose join, general, AB and peer scores are all ≥ +0.05 with no red sector/news.
- **Lane** — the reason a stock is allowed on BUY: standard, group_leader, catalyst, catalyst_exception, probable.
- **Lookback marks** — 🔵 improved, 🚨 worse, ⚪ zero red cells, computed from the stock's own factor colour row.
- **Relative volume (relvol)** — today's volume ÷ normal volume; < 0.7 after the open is "dead."
- **Residual** — an industry's 1-week % minus its sector's 1-week %.
- **Risk-on / risk-off** — general score ≥ +4 / ≤ −4.
- **RS** — relative strength vs peers or vs the market.
- **tanh** — a squash function that maps any number into (−1, +1); used so no single input can dominate.
- **Weather** — favorable/neutral/hostile/unknown stance per label value per day.
- **Z-score** — how many standard deviations above/below the average.
