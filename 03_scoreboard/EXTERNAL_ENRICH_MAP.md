# External enrich map — ops only

Kid: We already have report cards. This page says how to tape extra stickers
on them without peeking at today's answers, and without touching the live
shopping list.

**Research only.** Does not change live `flatten_robust`. No new vendor
column overwrites a Finviz Elite header. No sidecar prediction may color a
09:30 box until it clears the same leak / fill / fee bar as factor-mine.

| Layer | Kid one-liner | Status | Cyrus goal |
|---|---|---|---|
| Theme Radar `avoid_veto` | Don't buy the expensive stickers — they fade whether the class is happy or sad. | Optional column. Both-tape **YES** on prior `Forward P/E` ≥ 35. | **Avoid** |
| `elevate_bump` | Pull the kid who did the homework and still sat in the back. | Optional column. **Failed** both-tape on this window — keep research. | **Elevate** |
| Magic Formula / CANSLIM | Cheap + good at using money, or growing and leading — from columns we already have. | Offline flags. Cheap ≠ auto long. | Expand / combine |
| vectorbt harness | A calculator that tries many shopping lists, but still pays the 09:30 school-bell price and our fees. | Proposed wrapper. Do not use vectorbt's own fills. | **Expand only** |
| OpenBB / MarketDataApp | Only fill empty boxes. Don't redraw the report card. | Thin gaps only. Parquet under `data/`. | **Expand only** |
| qlib / FinRL / AlphaSift / Vibe-Trading | Practice teams in another room until they pass the same test as our sleeves. | Offline sidecars. Bar not cleared. | **Expand only** |

---

## Hard leak / fill / fee rules (every layer)

Kid: At the 09:30 school bell you only know yesterday's homework plus this
morning's packet. You cannot look at today's report card to pick. You pay
our broker's fees. You can only spend leftover lunch money and only sell
sandwiches you already have.

Copy these from the live research miners. Do not invent a second clock.

| Rule | What it means | Source of truth |
|---|---|---|
| Clock | Inputs knowable at **09:30 ET** on session `D`. | `src/factor_mine.py`, `src/ticker_lookback.py` |
| Finviz vintage | Elite export allowed as a 09:30 *input* on `D` is always the **prior session**. Same-day `data/exports/finviz_D.csv` is tape / outcome. | `feature_export_date()` → `gainer_capture.prior_session` |
| Banned gates | Same-day `Change`, `Change from Open`, `Gap`, `Relative Volume`, `Volume`, `Open`/`High`/`Low`/`Close` of `D`, printed book, later-export headlines. | `src/factor_mine.py` `INPUT_FIELDS` docstring |
| AB / candles | A-side tape uses completed bars with `date < D`. At 09:30 on `D` the factor never sees `D`'s OHLC. | `src/candle_factor.py`, AB `pair_day_a` / `pair_day_b` |
| E / R | Same-day analyst `R` off. Same-day earnings `E` only if stamped **≤ 09:30**. | `finviz_events.asof_snapshot` |
| Fill | **09:30 open**, whole shares. Skip if leftover cannot buy 1 share, or **no open**. Early exit fills at that later session's 09:30 open. | `src/factor_mine_book.py` `BOOK_RULES` (`fill: "open"`) |
| Fees | Futubull US-stock schedule. | `00_grounding/futubull_fees.json` via `src/paper_trade.py` `load_fees` / `order_fees` |
| Cash machine | Leftover cash + lots actually held. Sell first. Hard-red morning `S ≤ −3`: sit, no new buys. | `src/factor_mine_book.py`, live `flatten_robust` |
| Audit | Independent fill-replay must PASS. | `FACTOR_MINE_ACTION.md` / `factor_mine_book` audit |
| Live | Research sleeves may say `live_untouched = flatten_robust`. They must not edit that policy. | `src/test_factor_mine.py`, `src/sleeve_merge.py` `LIVE_POLICY` |

Join keys used everywhere below:

| Key | Normalize | Lives on |
|---|---|---|
| `Ticker` | `str.upper().strip()` | Finviz, AB, join, universe, stock book |
| `asof_date` / session `D` | `YYYY-MM-DD` | AB `asof_date`, weather `date`, ranked join filename |
| price bar | `(date, ticker)` | `data/prices/ohlc.parquet` columns `date,ticker,open,high,low,close,volume` |

---

## 1. Magic Formula / CANSLIM — existing Finviz Elite + AB join

Kid: Two old report-card stickers. Magic Formula = cheap and good at using
its toys. CANSLIM = growing, near a new high, with grown-ups buying.
We already printed every number. Do not call a new website.

### Files

| Path | Role |
|---|---|
| `data/exports/finviz_YYYY-MM-DD.csv` | Weekday Elite archive. **150** exact headers (2026-09-04). |
| `data/finviz/latest.csv` | Fallback if no dated archive (`src/segments.py`). |
| `excel_bot/data/finviz_with_descriptions.csv` | Older full dump with `Finviz_Description` (graft by `Ticker` only). |
| `data/ab_checklist/{D}_ab_checklist.csv` | Intrinsic A + B1. Join key `Ticker` + `asof_date`. |
| `data/ab_checklist/{D}_ab_checklist_enriched.csv` | Preferred. Adds P01–P04 + `score_enriched`. |
| `data/universe/{D}_membership.csv` | Label bins already minted from Elite (`roe`, `sales_g`, `earnsurp`, `instown`, `range`, …). |
| `data/join/{D}_ranked.csv` | Labels × weather. Join `Ticker`. |
| `01_daily/weather/{D}_weather.json` | CANSLIM **M** (market). `signals.general_direction`, `signals.risk`. |
| `src/finviz_style_flags.py` | Tiny offline flagger (this PR). One CSV in, flags out. |

Loader already used by the book: `src/stock_book.py` `_load_ab_enriched` tries
enriched then base, dedupes `Ticker`, keeps `score_enriched` else `score`,
and `score_base` / P01–P04 when present.

### Exact Elite headers (do not rename)

There is **no** `Earnings Yield` column and **no** IBD Relative Strength
rating. RSI is `Relative Strength Index (14)` — different thing.

**Magic Formula proxies** (Greenblatt EY + ROC, not a new fundamental):

| Recipe input | Exact header | Unit on the 2026-09-04 export | Notes |
|---|---|---|---|
| Earnings-yield numerator | `Income` | $ millions TTM | Not EBIT. Closest signed earnings dollars. |
| Earnings-yield denominator | `Enterprise Value` | $ millions | Blank on many banks (JPM). |
| EY fallback | `EV/EBITDA` | multiple | Use `1 / EV/EBITDA` only when `Income`/`Enterprise Value` missing and EBITDA > 0. |
| EY last resort | `P/E` | multiple | `1 / P/E` if P/E > 0. Worse proxy (net, not EV). |
| Return on capital | `Return on Invested Capital` | percent (`72.08%`) | Closest ROC header. |
| Confirm / sort | `Return on Equity`, `Return on Assets`, `Operating Margin`, `Profit Margin` | percent | Do not replace ROIC. |
| Size gate | `Market Cap` | $ millions | Liquid floor already `100` in `ticker_lookback.RANDOM_MIN_MCAP_M`. |
| Classic exclude | `Sector` | text | Greenblatt often drops `Financial` + `Utilities` (EV blank / leverage). Optional flag, not a live gate. |

Worked AAPL 2026-09-04: `Income=128930` / `Enterprise Value=4811900.59` →
EY ≈ 2.68%. `Return on Invested Capital=72.08%`.

**CANSLIM proxies** (O'Neil letters, Elite columns only):

| Letter | Kid | Exact headers | Default flag (research, not IBD) |
|---|---|---|---|
| **C** current quarter | Did this report beat and grow? | `EPS Growth Quarter Over Quarter`, `EPS Surprise` | QoQ ≥ **18** and surprise **> 0** |
| **A** annual | Has the year been growing? | `EPS Growth This Year`, `EPS Growth Past 3 Years`, `EPS Year Over Year TTM` | this year ≥ **25** **or** past-3y ≥ **25** |
| **N** new high | Is it near a new high? | `52-Week High`, `All-Time High` | `52-Week High` **> −15** (Finviz stores **% below the high**, so `-4.75%` = 4.75% under the high; `≥ 0` is a breakout). Same decode as `src/segments.py` range. |
| **S** supply / demand | Are people actually trading it? | `Relative Volume`, `Average Volume`, `Volume`, `Shares Float`, `Float %`, `Short Float` | **Prior-session** `Relative Volume` ≥ **1.0** and `Average Volume` ≥ **500** (thousands of shares — same unit as `RANDOM_MIN_AVG_VOL_K`). Same-day RelVol on `D` is a leak. |
| **L** leader | Is it leading its cousins? | `Performance (Quarter)`, `Performance (Month)` | Quarter **> 0**. Stronger with AB `P01_peer_lead_week == 1`. |
| **I** institutions | Are the big kids adding? | `Institutional Ownership`, `Institutional Transactions` | ownership ≥ **20** and transactions **≥ 0** |
| **M** market | Is the weather OK? | *not a Finviz stock column* | `01_daily/weather/{D}_weather.json` → `signals.general_direction == "up"` and `signals.risk != "off"`. If the file is missing, **M = unknown** (do not invent). |

Sales / surprise helpers already on the export (do not fetch):
`Sales Year Over Year TTM`, `Sales Growth Quarter Over Quarter`,
`Revenue Surprise`, `EPS Growth Next Year`, `EPS Growth Past 5 Years`.

### AB join (name the columns)

Kid: AB is the tape + homework checklist we already run. Glue it by ticker.
Do not rebuild A-side from Finviz Change%.

```
finviz_YYYY-MM-DD.csv.Ticker
    ==  {D}_ab_checklist_enriched.csv.Ticker
asof_date on the AB file must be the Finviz file date (or the prior
session when this row is a 09:30 input on D+1).
```

B-side already **is** Elite (do not re-scrape):

| AB column | Elite header |
|---|---|
| `val_B01_eps_surprise` / `status_B01_eps_surprise` | `EPS Surprise` |
| `val_B02_revenue_surprise` | `Revenue Surprise` |
| `val_B03_sales` | `Sales` |
| `val_B04_income` | `Income` |
| `val_B05_profit_margin` / `status_B06_profitable` | `Profit Margin` / `Income` |
| `val_B07_target_price` / `val_B08_target_price_delta` | `Target Price` vs prior export |
| `val_B09_analyst_recom` | `Analyst Recom` |
| `val_B10_insider_transactions` / `val_B11_insider_tx_delta` | `Insider Transactions` |
| `val_B12_institutional_transactions` | `Institutional Transactions` |
| `val_B13_short_float` | `Short Float` |
| `val_B14_earnings_date` | `Earnings Date` |
| `val_B17_eps_surprise_pair` / `val_B18_rev_surprise_pair` | last two exports, same headers |

Enrichment (peer / industry / sector — still on-disk):

| Column | Meaning | Source |
|---|---|---|
| `P01_peer_lead_week` | +1 lead / −1 lag | `data/peers/correlations.csv` + Elite `Performance (Week)` ≤ asof |
| `P02_peers_advancing` | peer-median week | same |
| `P03_industry_advancing` | industry median week | Elite `Industry` |
| `P04_sector_supportive` | board Dir=up | nearest `01_daily/sectors/<board_date>/_BOARD.md` with `board_date <= asof` |
| `score_base` | intrinsic A+B | checklist |
| `score_enriched` | base + P01–P04 | `src/ab_enrich.py` |
| `score_context` | P-only | do not double-count as `s_peer` |

Stock-book already merges AB on `Ticker` and treats P01–P04 as **context**,
not a second peer vote (`src/stock_book.py`).

Optional CANSLIM **L** boost: `P01_peer_lead_week == 1` **and**
`Performance (Quarter) > 0`.

### Offline flag script

```
python -m src.finviz_style_flags \
  --csv data/exports/finviz_2026-09-04.csv \
  --ab data/ab_checklist/2026-09-04_ab_checklist_enriched.csv \
  --out data/style_flags/2026-09-04_style_flags.csv
```

Writes `mf_flag`, `mf_combo_rank`, `ey`, `roic`, `canslim_flag`, letter
bits, and (if `--ab`) `ab_score`, `P01_peer_lead_week`. Stdlib only.
Does not call flatten, Yahoo, or OpenBB. Magic Formula drops
`Sector` in {Financial, Utilities} and `Market Cap` < 100 ($ millions)
unless `--keep-fin-util`.

### Leak rules for this layer

- A 09:30 recipe on `D` reads `finviz_{prior}.csv` and
  `{prior}_ab_checklist*.csv`, not `finviz_D.csv`.
- `Relative Volume` / `Volume` / `Change` on the **same-day** export are
  outcomes. The flag script on a dated file is a **snapshot of that file**,
  not a live gate. Wire it through `feature_export_date` before any sweep.
- Do not treat `status_A07_rvol` from an AB file whose `asof_date == D` as
  a 09:30 input — A07 uses that asof session's volume.
- `News Title` / `Daily Digest` on the same-day export are later scrapes.
  Factor-mine already prefers the morning news box, else the **prior**
  headline (`prior_news_tone`).
- Membership / join labels (`roe:good`, `sales_g:fast`, `range:breakout`)
  are the same Elite numbers binned. Prefer those columns if you only need
  a bin, not a second decode.

---

## 2. vectorbt harness — factor / regime sweeps, our PIT fills / fees

Kid: vectorbt is a fast abacus. It does not get to invent the price we
paid or skip the fee jar. Our kid still buys at the 09:30 bell.

**Not in `requirements.txt`.** Keep it a research extra
(`pip install vectorbt` in a sidecar venv). Do not import it from
`sleeve_merge_live` / `futubull_exec`.

### What it is allowed to sweep

| Knob | Already named | File |
|---|---|---|
| Universe lists | `union`, `flatten`, `probable`, `yday_gainer`, `ohlc_hot` | `src/factor_mine.py` `_UNI_KID` |
| Cameras / gates | vol, news, ab, join, blue, white, alarm, last_green, … | same, `_gate_kid` |
| Hold | 1 / 3 / 5 sessions (entry morning counts as 1) | `hold_window()` |
| Size / sell / S-boost | leftover, rank_w, topheavy, half; list / time / cut_loser / trail | `src/factor_mine_book.py` |
| Regime | morning `S` from general predict; hard-red `S ≤ −3` sit | `factor_mine_book.morning_s` ← `sleeve_merge.load_payload()["regime"]` |
| Weather stance | `01_daily/weather/{D}_weather.json` `stances` / `signals.risk` | `src/weather.py` |
| MF / CANSLIM bits | flags from §1, **prior** vintage only | `data/style_flags/{prior}_style_flags.csv` |

### What it must not replace

vectorbt `Portfolio.from_signals` defaults to **close-to-close, no
Futubull fees, fractional shares, no leftover cash machine**. That would
lie next to `FACTOR_MINE.md`.

Harness contract (proposed `src/vectorbt_harness.py`, not written):

1. Build a signal panel from 09:30-knowable columns only.
2. **Fills:** `open` from `data/prices/ohlc.parquet` keyed
   `(date, ticker)`. No open → skip (do not silently use close).
3. **Fees:** `paper_trade.order_fees(shares, px, side, load_fees())`.
4. **Shares:** integers. `paper_account.fractional_shares = false`.
5. **Cash:** $10k start, leftover split, sell first — call
   `factor_mine_book` / `replay_ledger` rather than vectorbt cash.
6. **Marks:** per-name 09:30 open; overnight cash does not change.
7. **Audit:** reuse the factor-mine fill-replay. FAIL = do not publish.
8. **Live:** assert `LIVE_POLICY == "flatten_robust"` and do not write it.

### Paths / join keys

| Path | Key | Use |
|---|---|---|
| `data/prices/ohlc.parquet` | `date` + `ticker` | 09:30 `open`, later `close` for *grading only* |
| `data/factor_mine/panel.json` | `date` + `ticker` | existing 09:30 camera panel |
| `03_scoreboard/feature_asof_panel.parquet` | name-day | cameras + prior Finviz (do not rebuild) |
| `data/join/{D}_ranked.csv` | `Ticker` | regime × label score |
| `01_daily/weather/{D}_weather.json` | `date` | M / risk-off |
| `00_grounding/futubull_fees.json` | — | fee schedule |
| `data/vectorbt/sweeps/{name}.parquet` | `date,ticker,recipe` | **output cache** (new) |
| `03_scoreboard/VECTORBT_SWEEP.md` | — | human table, same columns as `FACTOR_MINE.md` |

### Leak rules

- Grade a hold-3 buy on `2026-08-17` on sessions 8-17 / 8-18 / 8-19 only
  (`hold_window`). Tomorrow's weather is never an input.
- Regime filters read `D`'s **morning** predict / weather, not `D`'s close
  outcome (`01_daily/general/{D}_outcome.md` is post-close).
- Do not pass same-day `Change%` into `from_signals` as a size or gate.
- If vectorbt wants a "price" matrix, give it **open** for trades and keep
  close in a separate mark column. Mixing them is a leak.

---

## 3. OpenBB / MarketDataApp — thin gaps only, parquet under `data/`

Kid: If a box on the worksheet is empty, you may ask the librarian for that
one missing number. You may not throw away the worksheet and start over.

Finviz Elite + FRED Channel 1 + `price_store` already cover almost
everything the book ranks. Vendor calls are **gap fills**, not a second
universe.

### What counts as a thin gap (from this repo)

| Gap | Evidence | Allowed fill | Not allowed |
|---|---|---|---|
| Missing OHLC name-days | `price_store` bootstrap / update miss; factor-mine "no open" skip | Daily OHLCV into `data/prices/ohlc.parquet` **or** a side cache, then merge by `(date,ticker)` | Intraday of session `D` as a 09:30 input |
| Missing `Finviz_Description` | Automated views drop it; `segments.py` grafts from `data/finviz/latest.csv` | Static description text only | Same-day `News Title` rewrite |
| Silent news layer | `BOOK_GAPS.md` `s_news` 100% silent on 2026-08-27 | Morning-packet headlines already in `01_daily/news/` | Vendor news stamped after 09:30 on `D` |
| Missing general / weather file | `BOOK_GAPS` "file missing — not silence"; weather degrades to `unknown` | Do **not** invent stance. Optional FRED reprint if Channel 1 file is absent (`01_daily/_channel1/{D}_predict.json`) | Live web scrape as a stance |
| Insider history holes | `data/insider/history/monthly_panel.parquet` (`ab_merge_extras.py`) | Monthly Form-4 panel, `asof <= D` | Same-day Form-4 as a 09:30 gate unless the morning packet already has it |
| Optionable only | Elite `Optionable` = Yes/No | Snapshot OI/IV **prior** close, if ever needed | Same-day option tape |
| Peer map missing | `data/peers/correlations.csv` | Offline corr rebuild from `ohlc.parquet` closes **< D` | Vendor "similar stocks" list |

Channel 1 (VIX, DGS10, DXY, oil, Fear & Greed) is already pre-fetched into
`01_daily/_channel1/` and `src/weather.py`. **Do not replace FRED with
OpenBB** when that file exists.

### Cache layout (parquet, under `data/`)

Kid: Put the librarian's photocopy in a dated folder so tomorrow we know
which day we asked.

```
data/external/
  openbb/{dataset}/asof=YYYY-MM-DD/part.parquet
  mda/{dataset}/asof=YYYY-MM-DD/part.parquet
  manifest.json          # dataset, vendor, pulled_at, asof, n, join_keys
```

Required parquet columns:

| Column | Rule |
|---|---|
| `ticker` | upper strip (or `symbol` renamed on read) |
| `asof_date` | vendor knowledge date, `YYYY-MM-DD` |
| `dataset` | e.g. `ohlc_gap`, `description`, `options_oi` |
| `vendor` | `openbb` / `mda` |
| payload columns | **new names only** (`ext_*`). Never a column named `Change`, `Price`, `EPS Surprise`, … |

Join: `ticker` + `asof_date`. For a 09:30 row on session `D`,
`asof_date < D` (same function as `feature_export_date`). If the vendor
stamp is missing, drop the row — do not assume "latest".

Read path: `src/external_cache.py` (proposed). Write path is a cron /
manual pull, not `run_preopen_all`.

### Leak rules

- Cache key is **asof**, not "downloaded_at". A Sunday pull of Friday's
  bar is `asof=Friday`.
- Never left-join vendor "latest quote" onto a historical `D`.
- If Elite already has the header, the vendor value is a **qc diff**, not
  a replacement. Log mismatches; keep Finviz.
- Thin-gap OHLCV may append into `data/prices/ohlc.parquet` only through
  `src/price_store.py` (same `date,ticker,open,high,low,close,volume`
  schema, `drop_duplicates keep=last`). Do not invent a second price store.

---

## 4. qlib / FinRL / AlphaSift / Vibe-Trading — offline sidecars until the bar clears

Kid: Extra study groups can practice in the library. They do not get to
rewrite tomorrow's real shopping list until they pass the same test we
already use.

**Bar (all must pass):**

1. Inputs ⊆ 09:30-knowable set (`INPUT_FIELDS` + prior Elite + prior AB +
   morning packet + weather/predict dated `D`).
2. No same-day `Change` / `Gap` / `RelVol` / `D` OHLC / later book.
3. Fills = 09:30 open, whole shares, Futubull fees, leftover, sell first,
   hard-red sit.
4. Cash+holdings fill-replay **PASS**.
5. `live_untouched == "flatten_robust"` in the payload.
6. No import from `src/sleeve_merge_live.py`, `src/futubull_exec.py`, or
   systemd preopen.
7. License / dependency review recorded (these stacks pull large graphs).
   Until that note exists, they stay out of `requirements.txt`.

None of these four are in the repo today. Treat them as **foreign
processes** that read our parquet and write predictions we may join later.

### Isolation

```
sidecars/qlib/            # vendor tree or submodule — not imported by src.*
sidecars/finrl/
sidecars/alphasift/
sidecars/vibe_trading/

data/sidecars/{name}/{asof}/preds.parquet
  columns: ticker, asof_date, sidecar, score, horizon, model_id
03_scoreboard/SIDECAR_{NAME}.md   # research table only
```

Join onto a 09:30 row on `D` with `asof_date < D` **or** `asof_date == D`
only when the sidecar's own manifest swears the features were the morning
packet (same vintage as `feature_asof_panel.parquet`). If the manifest is
missing, treat as leak and drop.

### What each sidecar is for (ops, not endorsement)

| Sidecar | Kid | Allowed job | Stop condition |
|---|---|---|---|
| **qlib** | A homework machine that ranks names from old report cards. | Offline alpha dump → `data/sidecars/qlib/` | Any feature with `D` close / volume |
| **FinRL** | A robot that practices buying and selling in a video game. | Train on our PIT open+fee blotter, never on raw gym close | Reward using close-to-close without fees |
| **AlphaSift** | A sieve that throws out noisy homework questions. | Feature prune on `feature_asof` / factor-mine panel | Sift using same-day gainer labels as inputs |
| **Vibe-Trading** | A mood reader. | Narrative tags **after** the morning news box exists | Live tweet/websocket into preopen |

Promotion path (same as any factor-mine recipe): show up in
`03_scoreboard/FACTOR_MINE.md` with audit PASS → stay research → only a
human changes `LIVE_POLICY`. This map does not promote anyone.

---

## 0. Avoid / Elevate / Expand — ops frame

Kid: Three jobs. (1) Stop buying rotting apples. (2) Rescue the good apple
we ranked "meh". (3) Invent new recipes in the practice kitchen — do not
serve them yet.

Full blotter + panel: [`OVERLAY_AUTOPSY.md`](OVERLAY_AUTOPSY.md) ·
`python -m src.overlay_autopsy --write`.

Cyrus bar for any overlay column:

| Goal | Question | Promote only when |
|---|---|---|
| **Avoid** | Would this have killed bad buys we took? | Both-tape excess **< 0** (underperforms SPY-up **and** SPY-down, each n≥20) |
| **Elevate** | Would this have rescued high-conviction names we ranked mediocre? | Both-tape excess **> 0**, and the set is **not** half the universe |
| **Expand** | Is this a formula we never wired? | Stays research. vectorbt / OpenBB / sidecars live here only |

Theme Radar (Elite headers, prior vintage):

| Factor | Exact header / math | Role |
|---|---|---|
| high Forward P/E | `Forward P/E` ≥ 35 on **prior** `data/exports/finviz_{prior}.csv` | **Avoid** — faded both tapes (n=4827, xs −0.09, up −0.03, down −0.19) |
| d_RSI | `Relative Strength Index (14)`[prior] − same[prior-prior] | Input only. Combined with d_mcap **failed** both-tape |
| d_Market Cap | 100 × (`Market Cap`[prior] / `Market Cap`[prior-prior] − 1) | Same. 08-14 jumps look like unit/CA noise |
| cheap FPE | 0 < `Forward P/E` ≤ 15 | **Not** an elevate. Small both-tape xs (+0.15) on 35% of the panel = "not expensive", not a rescue |

Optional columns (emitted by `src/finviz_style_flags.py`, **not** live gates):

| column | rule | both-tape this window | promote? |
|---|---|---|---|
| `avoid_veto` / `radar_high_fpe` | prior `Forward P/E` ≥ 35 | YES (avoid) | Optional sticker only. Human review. |
| `radar_hot` | d_RSI≥5 **and** d_mcap≥3% | NO | Do not OR into the veto |
| `elevate_bump` | `canslim_flag` + not high-FPE + (P01=1 or AB≥8) | NO (n=112, up xs ~0) | Keep research |
| `radar_cheap_fpe` / `mf_flag` | cheap / Magic Formula | cheap YES as a *tiny* mix; MF NO | **Never auto-long** |

Join: `Ticker` + `feature_export_date(D)` = prior session. Same leak clock
as § Hard rules. Same-day `Change` / `Gap` / RelVol stay outcomes.

**Autopsy sources (no new scrape):**

| Source | Path | What we used |
|---|---|---|
| Paper closed lots | `data/paper/roundtrips.csv` sleeves `1d_top/size`, `3d_top/size` | 175 losers after Futubull fees |
| Book-gap worst buys | `data/stock_book/{D}_book_gaps.json` `worst_buys` | 30 names, 1w fwd |
| Book-gap missed | same, `missed_movers` | outweighed=20 (elevate-shaped), gated_out=25 (expand), blind=11 |
| Liquid panel | prior Elite × D `Change from Open` | 50,445 name-days, 2026-08-13 → 2026-09-05 |
| flatten_live | `03_scoreboard/factor_mine/flatten_live_h*.md` | **thin-n** (7 start days) — not scored |

Worked avoid hits among worst buys: `BTBT` FPE 152, `INDI` FPE 212
(08-14). Missed: `ACMR`/`ERO` (08-27) had join/AB ~+1 and cheap-to-mid
FPE — the FPE veto would **not** have saved those; they are join-hot
losers, not Theme Radar. Cost: `REAX` +853% 1w was outweighed **and**
FPE 65 — the surviving avoid would have skipped a rocket.

Do not promote `elevate_bump` until a later window clears both tapes.
Do not feed vectorbt/OpenBB/sidecars into avoid/elevate.

---

## Wiring cheat-sheet

```
prior Elite CSV ──┐
prior-prior CSV ──┼─ src/finviz_style_flags.py ── avoid_veto / elevate_bump
prior AB CSV ─────┘         │
                            │ ticker
09:30 cameras / join / weather / ohlc.open
                            │
                            ├─ overlay autopsy (both-tape bar) ── 03_scoreboard/OVERLAY_AUTOPSY.md
                            ├─ factor_mine_book (fills + fees + audit)
                            ├─ vectorbt harness (EXPAND only)
                            └─ sidecar preds (EXPAND only)

OpenBB/MDA ── data/external/.../asof=prior/part.parquet
              (ext_* columns only; fill holes; never overwrite Elite)
```

Live production path stays:

`flatten_robust` ← `src/sleeve_merge.py` `LIVE_POLICY` ← 3d size-book +
flatten clock. **Not on this map.**

---

## Do not

- Edit `LIVE_POLICY` or `flatten_robust` from this work.
- Rename Finviz headers (`Return on Invested Capital` ≠ `ROIC`).
- Join on company name. `Ticker` only.
- Use `data/exports/finviz_D.csv` as a 09:30 feature on `D`.
- Let vectorbt / qlib / FinRL default backtesters publish next to
  `FACTOR_MINE.md` without the Futubull 09:30 book.
- Cache vendor "latest" without an `asof_date`.
- Treat this document as a live gate. Flags are stickers, not tickets.
- Promote an overlay that failed both-tape or sits on thin-n.
- Treat cheap `Forward P/E` / Magic Formula as an elevate (cheap ≠ auto long).
- Wire vectorbt / OpenBB / sidecars into avoid or elevate.
