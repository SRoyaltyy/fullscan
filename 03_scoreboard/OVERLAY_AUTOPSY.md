# Overlay autopsy — avoid / elevate / expand

Kid: Stop buying the rotting apples. Pull the good apples we left on the bottom of the barrel. New recipes stay in the practice kitchen.

_Generated 2026-09-06T13:06:25.730363-04:00 · research only · live `flatten_robust` untouched._

Leak clock: features = **prior** Elite + prior AB + morning weather. 1d panel outcome = same-day `Change from Open` (never a gate). Book-gap fwd is the committed 1w gap scan. Paper fwd is closed round-trip after fees.

## Cyrus bar

| Goal | Kid | What we score |
|---|---|---|
| **Avoid** | Don't buy the ones that go bad. | Optional `avoid_veto` |
| **Elevate** | Rescue names we ranked 'meh' that then won. | Optional `elevate_bump` |
| **Expand** | New formulas we never wired. | vectorbt / OpenBB / sidecars only |

Theme Radar: **high `Forward P/E` fades both tapes on the 1d open→close clock**. Cheap / Magic Formula is **not** an auto long. This panel is **`theme_radar_1d`** — it does **not** auto-apply to `flatten_h5` or live `flatten_robust`. The 5d FPE re-mine **failed both-tape** (Sign_up 40%). Do not wire.

## Ranked findings

1. **Avoid that cleared both tapes:** `radar_high_fpe` / `avoid_veto` (n=4827, xs=-0.09, up xs=-0.03, down xs=-0.19). High `Forward P/E` ≥ 35 on the **prior** Elite file. Optional column only — not a live gate.
2. **`radar_rsi_up` (d_RSI≥5) is a veto *candidate*, not buy-rank fuel** (n=3468, both-tape=NO, up xs=-0.20, down xs=+0.33). Do not OR into `avoid_veto` unless both-tape stays YES on a later window.
3. **`radar_mcap_up` (d_Market Cap≥3%) is a veto *candidate*, not buy-rank fuel** (n=5584, both-tape=NO, up xs=+0.01, down xs=+0.10). Same rule: do not OR into the surviving high-FPE veto yet.
4. **`radar_hot` (d_RSI≥5 and d_mcap≥3%) failed both-tape** (n=2311, down xs=+0.35). Do not OR it into `avoid_veto`.
5. **`elevate_bump` did not clear both-tape** (n=142, up xs=-0.12, down n=24 xs=+1.00). Keep as a research sticker. Down-tape n is borderline (≥20).
6. **Cheap FPE is not a rescue overlay.** `radar_cheap_fpe` has a small both-tape xs (+0.15, n=17897, ~35% of the panel). That is 'not expensive', not 'high-conviction we ranked mediocre'. Theme Radar: cheap ≠ auto long.
7. **Magic Formula `mf_flag` failed both-tape** (up xs=-0.07, down xs=+0.12). Do not elevate on EY+ROIC alone.
8. **Bad buys:** 175 paper closed losers in 1d_top, 1d_size, 3d_top, 3d_size + 30 book-gap worst buys. Worst names (VERI −25%, AEVA −21%, RXT −21%, ACMR −14%) were join-positive or silent-AB, not CANSLIM. Two of the worst (BTBT FPE 152, INDI FPE 212) would have printed `avoid_veto`.
9. **Missed rockets:** outweighed=20 (elevate-shaped) · gated_out=25 (micro gate — expand, not a rank miss) · blind=11. REAX +853% was outweighed **and** high-FPE — the surviving avoid would have skipped a winner. That cost is why this stays optional.
10. **Elevate: nothing cleared both-tape as a long overlay.** `elevate_bump` / `canslim_flag` / `mf_flag` die on up tapes. Join `total_score` is the existing ranker — using it to bump is circular. On Theme Radar miss names it was already high (copper 48/95 top-quintile) and they still lost. Do **not** bump.
11. **Theme Radar miss baskets** (graded high then lost; 09:30 prior Elite): high-FPE fired every day on `GEV` (19/19, med FPE 39) and `CCJ` (19/19, med 55). `GLW` 8/19. Copper 0/95 (ERO med FPE 7.6 — cheap, still lost). Gold contrast `GDX/GLD/NEM/AEM` high-FPE 0/76 — fade veto would not have blocked the 8/12 hit. Paper/book losers in-basket: CEG, VST (08-14 / 08-19 / 09-03), ERO book-gap 08-27 −11%. CEG/VST/ERO were mid/cheap FPE — the surviving avoid would not have saved those buys.
12. **Short side was weak early** (lookback 🔵/🚨/fade empty on early books). First leak-free patch = fade vetoes (high FPE; d_RSI / d_mcap as candidates), **not** a short book and **not** a buy-rank bump.
13. **Expand only:** vectorbt, Zipline, OpenBB/MDA, qlib/FinRL/AlphaSift/Vibe-Trading. flatten_live blotters are thin (7 start days) — not a second autopsy sample. AB `status_*` fail-any hits ~93% of stock name-days — too wide. Do not invent scrapes.
14. **Sift autopsy fold:** FPE veto caught ~2/15 worst buys (BTBT, INDI). GEV is a natural Avoid (19/19). Copper was join-high then lost. ERO = cheap spike we bought, not a missed expensive theme. REAX tax keeps the veto optional. Soft 🚨∧fade / heat-sit n=1 / CANSLIM A∧S stay log-only — do not promote. See §5 and `EXTERNAL_ENRICH_MAP.md`.
15. **Strategy-native:** `radar_high_fpe` / optional FPE≥35 are the **1d Theme Radar clock** (`theme_radar_1d`, open→close). They do **not** auto-apply to `flatten_h5` or live `flatten_robust`. The matching-hold 5d re-mine **failed both-tape**. Reject elevates unless scored on the matching hold. See §6.
16. **Thin-n / data caveats:** 08-14 d_RSI often missing (no prior-prior RSI). Some d_mcap prints look like unit/corporate-action jumps (APPS +270%). Lookback 🔵/🚨/fade columns are empty on early books. 08-27 morning weather is unknown in this run. OKLO/SMR/GDX/GLD often have blank `Forward P/E` — honest thin, no new scrape.
17. **Fee-aware KEEP re-mine:** `radar_high_fpe` fails Futubull $ peer-excess on `theme_radar_1d` (xs $+0.09; up-tape $+0.35). **`flatten_h5` × FPE-avoid = FAIL** — local 5d board IC_up −0.033 Sign_up **40%** (2/5) n=5; IC_down −0.131 Sign_down 100% n=10 (up-tape flips). Do not wire. d_RSI / d_mcap 5d inconclusive. Leftover h1/h3 THIN. Live-shaped veto never fired. See [`OVERLAY_HORIZON_BT.md`](OVERLAY_HORIZON_BT.md). Elevate stays closed. No live wire.

Panel: **50445** liquid name-days · sessions 2026-08-13 → 2026-09-05 · base 1d mean **-0.13** · hit **42.2%**.

Both-tape = same-sign excess on realized SPY-up **and** SPY-down days, each cell n≥20. Otherwise **thin-n** — do not promote.

## 1. Avoid — rule scoreboard (liquid panel)

| rule | n | hit | mean 1d | xs vs base | up n / xs | down n / xs | both-tape |
|---|---:|---:|---:|---:|---:|---:|---|
| `avoid_veto` | 4827 | 41.5% | -0.22 | -0.09 | 1764 / -0.03 | 1269 / -0.19 | YES |
| `radar_high_fpe` | 4827 | 41.5% | -0.22 | -0.09 | 1764 / -0.03 | 1269 / -0.19 | YES |
| `radar_rsi_up` | 3468 | 46.5% | -0.08 | +0.05 | 1357 / -0.20 | 1135 / +0.33 | NO |
| `radar_mcap_up` | 5584 | 45.3% | -0.19 | -0.06 | 1423 / +0.01 | 1374 / +0.10 | NO |
| `radar_hot` | 2311 | 45.7% | -0.12 | +0.01 | 801 / -0.14 | 788 / +0.35 | NO |

High `Forward P/E` must fade **both** tapes to stay an avoid. `radar_rsi_up` / `radar_mcap_up` are Theme Radar veto *candidates* (not buy-rank fuel). Combined `radar_hot` failed both-tape — do not OR it into `avoid_veto`. If `radar_cheap_fpe` or `mf_flag` prints a *positive* both-tape elevate, ignore it — cheap ≠ auto long.

### Bad buys we actually took

Paper `1d_top, 1d_size, 3d_top, 3d_size` closed losers (fwd < 0, n=175) plus book-gap worst buys (1w, n=30). Showing the worst 15 by fwd.

| date | ticker | fwd | src | class | FPE | d_RSI | d_mcap | MF | CS | avoid | elev | AB | join | peer | 🔵 | 🚨 | fade | morn |
|---|---|---:|---|---|---:|---:|---:|:-:|:-:|:-:|:-:|---:|---:|---:|:-:|:-:|:-:|---|
| 2026-08-14 | `VERI` | -25.30 | book_gaps | bought | — | — | -34.30 |  |  |  |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-14 | `AEVA` | -21.43 | book_gaps | bought | — | — | +65.04 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `RXT` | -21.14 | book_gaps | bought | +30.96 | — | +175.63 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `WOLF` | -18.97 | book_gaps | bought | — | — | +18.42 |  |  |  |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-27 | `ACMR` | -13.75 | book_gaps | bought | +24.93 | +1.39 | +1.05 |  |  |  |  | +0.76 | +0.95 | +0.52 |  |  |  | unknown |
| 2026-08-14 | `TLN` | -13.31 | book_gaps | bought | +11.15 | — | +3.44 |  |  |  |  | +0.00 | +0.37 | +0.00 |  |  |  | up |
| 2026-08-17 | `NB` | -13.31 | paper:3d_size | bought | — | +1.61 | +1.73 |  |  |  |  | — | +0.17 | — |  |  |  | up |
| 2026-08-14 | `BTBT` | -13.28 | paper:3d_size | bought | +152.50 | — | +0.98 |  |  | Y |  | — | +0.55 | — |  |  |  | up |
| 2026-08-14 | `TLN` | -12.98 | paper:1d_top | bought | +11.15 | — | +3.44 |  |  |  |  | — | +0.37 | — |  |  |  | up |
| 2026-08-14 | `APPS` | -12.62 | book_gaps | bought | +13.36 | — | +270.14 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `BAND` | -11.95 | book_gaps | bought | +28.25 | — | +123.25 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `INDI` | -11.89 | book_gaps | bought | +211.98 | — | +12.75 |  |  | Y |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-14 | `TLN` | -11.69 | paper:3d_top | bought | +11.15 | — | +3.44 |  |  |  |  | — | +0.37 | — |  |  |  | up |
| 2026-08-27 | `ERO` | -11.30 | book_gaps | bought | +8.18 | +2.11 | +1.29 |  |  |  |  | +0.98 | +0.99 | +0.79 |  |  |  | unknown |
| 2026-08-14 | `NRG` | -10.40 | book_gaps | bought | +11.25 | — | -26.12 |  |  |  |  | +0.00 | +0.23 | +0.00 |  |  |  | up |

## 2. Elevate — rule scoreboard (liquid panel)

| rule | n | hit | mean 1d | xs vs base | up n / xs | down n / xs | both-tape |
|---|---:|---:|---:|---:|---:|---:|---|
| `elevate_bump` | 142 | 46.5% | +0.07 | +0.20 | 71 / -0.12 | 24 / +1.00 | NO |
| `canslim_flag` | 268 | 46.6% | -0.21 | -0.08 | 101 / -0.35 | 44 / +0.19 | NO |
| `mf_flag` | 822 | 47.0% | +0.02 | +0.15 | 303 / -0.07 | 218 / +0.12 | NO |
| `radar_cheap_fpe` | 17897 | 44.3% | +0.02 | +0.15 | 6660 / +0.08 | 4693 / +0.11 | YES |

`elevate_bump` = CANSLIM **and** not Theme-Radar avoid **and** (AB `P01=1` or `ab_score`≥8.0). `mf_flag` / cheap FPE alone never bump.

### Missed rockets (not top-ranked / not bought, then won big)

Book-gap missed movers with 1w fwd ≥ 10%. **outweighed** = elevate candidates (signals existed, rank buried them). **gated_out** = micro/mcap gate — expand, not a ranker miss. **blind** = every input silent.

| date | ticker | fwd | src | class | FPE | d_RSI | d_mcap | MF | CS | avoid | elev | AB | join | peer | 🔵 | 🚨 | fade | morn |
|---|---|---:|---|---|---:|---:|---:|:-:|:-:|:-:|:-:|---:|---:|---:|:-:|:-:|:-:|---|
| 2026-08-19 | `REAX` | +853.05 | book_gaps | outweighed | +64.97 | +3.40 | +5.65 |  |  | Y |  | +0.56 | -0.53 | +0.82 |  |  |  | down |
| 2026-08-19 | `ASST` | +45.96 | book_gaps | outweighed | +23.32 | +0.67 | +1.25 |  |  |  |  | -0.64 | -0.58 | +0.98 |  |  |  | down |
| 2026-08-19 | `ANF` | +40.11 | book_gaps | outweighed | +9.08 | +1.38 | +1.05 |  |  |  |  | +0.24 | -0.08 | -0.36 |  |  |  | down |
| 2026-08-19 | `TRON` | +32.72 | book_gaps | outweighed | — | -1.26 | -1.33 |  |  |  |  | +0.00 | -0.46 | +0.23 |  |  |  | down |
| 2026-08-19 | `CRML` | +29.50 | book_gaps | outweighed | — | -2.26 | -3.09 |  |  |  |  | -0.64 | -0.54 | -0.73 |  |  |  | down |
| 2026-08-19 | `TMC` | +26.20 | book_gaps | outweighed | — | -0.05 | -0.26 |  |  |  |  | -0.76 | -0.65 | -0.94 |  |  |  | down |
| 2026-08-21 | `CRM` | +22.39 | book_gaps | outweighed | +13.26 | +0.05 | +0.01 |  |  |  |  | +0.93 | -0.64 | +0.29 |  |  |  | up |
| 2026-08-27 | `MMED` | +22.19 | book_gaps | outweighed | +28.08 | -6.80 | -3.41 |  |  |  |  | +0.55 | -0.15 | +0.00 |  |  |  | unknown |
| 2026-08-27 | `FMC` | +21.76 | book_gaps | outweighed | +6.83 | +4.04 | +2.81 |  |  |  |  | +0.55 | +0.96 | +0.08 |  |  |  | unknown |
| 2026-08-27 | `CNH` | +21.08 | book_gaps | outweighed | +18.16 | -1.41 | -0.66 |  |  |  |  | +0.96 | +0.94 | +0.87 |  |  |  | unknown |
| 2026-08-21 | `RZLV` | +20.73 | book_gaps | outweighed | — | -1.48 | -1.82 |  |  |  |  | -0.55 | -0.98 | -0.82 |  |  |  | up |
| 2026-08-27 | `SMMT` | +19.54 | book_gaps | outweighed | — | +2.17 | +2.14 |  |  |  |  | -0.46 | -0.95 | +0.81 |  |  |  | unknown |
| 2026-08-21 | `ASST` | +19.32 | book_gaps | outweighed | +28.26 | +4.75 | +7.36 |  |  |  |  | -0.46 | +0.14 | +1.00 |  |  |  | up |
| 2026-08-27 | `SLBT` | +19.32 | book_gaps | outweighed | — | +2.61 | +4.89 |  |  |  |  | -0.55 | -0.90 | +0.00 |  |  |  | unknown |
| 2026-08-27 | `SID` | +18.87 | book_gaps | outweighed | — | +0.98 | +0.94 |  |  |  |  | +0.55 | +0.56 | +0.97 |  |  |  | unknown |

Gap classes: outweighed=20 · gated_out=25 · blind=11.

## 3. Expand — stay research

vectorbt / Zipline / OpenBB / MarketDataApp / qlib / FinRL / AlphaSift / Vibe-Trading do **not** get avoid or elevate columns. They stay expand-only until the same PIT / fee / audit bar as factor-mine.

## 4. Theme Radar miss baskets (09:30-knowable)

Graded high then lost. Features = prior Elite + prior AB + D join / morning weather / feature_asof when the file exists. Outcome = same-day `Change from Open`. Gold is the 8/12 *hit* contrast — fade veto should not have blocked it. Short side was weak early; fade vetoes are the first leak-free patch.

| basket | names | n | mean 1d | hit | high-FPE | d_RSI↑ | d_mcap↑ | CS | MF | join top-q | AB fail-any |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| optics | AAOI, COHR, LITE, GLW | 76 | -0.26 | 45% | 10/76 | 2/76 | 12/76 | 0/76 | 0/76 | 26/76 | 52/56 |
| ai_power | GEV, VRT, ETN, PWR, CAT | 95 | -0.62 | 39% | 23/95 | 3/95 | 3/95 | 0/95 | 0/95 | 34/95 | 65/70 |
| copper | FCX, SCCO, TECK, ERO, HBM | 95 | -0.51 | 41% | 0/95 | 9/95 | 15/95 | 11/95 | 0/95 | 48/95 | 65/70 |
| nuclear | CEG, VST, OKLO, SMR, CCJ | 95 | -0.25 | 48% | 19/95 | 6/95 | 11/95 | 0/95 | 0/95 | 26/95 | 65/70 |
| gold_hit | GDX, GLD, NEM, AEM | 76 | -0.15 | 47% | 0/76 | 7/76 | 7/76 | 1/76 | 0/76 | 29/76 | 26/56 |

### In-basket blotter (paper 1d/3d losers + book-gap worst buys)

| date | ticker | fwd | src |
|---|---|---:|---|
| 2026-08-14 | `CEG` | -2.00 | paper:1d_top |
| 2026-08-14 | `VST` | -1.75 | paper:1d_size |
| 2026-08-14 | `VST` | -5.59 | paper:1d_top |
| 2026-08-14 | `VST` | -4.21 | paper:3d_top |
| 2026-08-19 | `CEG` | -0.82 | paper:1d_top |
| 2026-08-27 | `ERO` | -11.30 | book_gaps |
| 2026-09-03 | `CEG` | -0.71 | paper:1d_top |
| 2026-09-03 | `CEG` | -0.47 | paper:1d_size |

### External mechanism map (no new scrape)

Join `total_score` is **not** an elevate. Nothing in this table clears both-tape as a long overlay. Cheap / Magic Formula is not a long.

| mechanism | goal | target_sleeve | hold | score_clock | exact Elite / AB / weather field | basket fire | veto, not fuel | elevate |
|---|---|---|---:|---|---|---|---|---|
| Theme Radar fade vetoes | **Avoid (1d only)** | theme_radar_1d | 1 | 1d open→close | `Forward P/E` ≥ 35; `d_RSI` = Δ `Relative Strength Index (14)` (prior − prior-prior); `d_Market Cap` = % Δ `Market Cap` (same vintage). Knobs in `finviz_style_flags`: HIGH_FPE=35, D_RSI_UP=5, D_MCAP_PCT=3. | high-FPE optics: 10/76; ai_power: 23/95; copper: 0/95; nuclear: 19/95; gold_hit: 0/76. d_RSI↑ optics: 2/76; ai_power: 3/95; copper: 9/95; nuclear: 6/95; gold_hit: 7/76. d_mcap↑ optics: 12/76; ai_power: 3/95; copper: 15/95; nuclear: 11/95; gold_hit: 7/76. Surviving both-tape avoid is high-FPE alone; d_RSI / d_mcap stay veto *candidates*, not rank fuel. Combined `radar_hot` failed both-tape — do not OR into the live veto. 1d clock only — not flatten_h5 / flatten_robust. | YES on 1d clock only. Does not auto-apply to flatten_h5 / flatten_robust. | NO — fades are not buy-rank fuel. |
| CANSLIM scanners | **Expand (not Elevate)** | theme_radar_1d | 1 | 1d open→close | C `EPS Growth Quarter Over Quarter` + `EPS Surprise`; A `EPS Growth This Year` / `EPS Growth Past 3 Years`; N `52-Week High` (% below high); S prior `Relative Volume` + `Average Volume`; L `Performance (Quarter)` + AB `P01_peer_lead_week`; I `Institutional Ownership` / `Institutional Transactions`; M weather `signals.general_direction` / `signals.risk`. | optics: 0/76; ai_power: 0/95; copper: 11/95; nuclear: 0/95; gold_hit: 1/76. Fired on copper (FCX/TECK/ERO) — those names then lost. Panel already failed both-tape as long (up xs −0.35). Do not bump. | Do not invert CANSLIM into a fade veto without a new bar. | NO — dies on 1d up tapes. Reject until matching hold. |
| Magic Formula | **Expand (not Elevate)** | theme_radar_1d | 1 | 1d open→close | `Income` / `Enterprise Value` (else `1/EV/EBITDA`, else `1/P/E`); ROC = `Return on Invested Capital`. Exclude Financial/Utilities; `Market Cap` < 100 dropped. | optics: 0/76; ai_power: 0/95; copper: 0/95; nuclear: 0/95; gold_hit: 0/76. Cheap/MF is the gold-contrast trap, not a rescue. Panel up xs −0.07 — failed both-tape as long. | Not a fade. Do not treat cheap as avoid either. | NO — do not promote cheap/MF as long. |
| Stock-Screener-System multi-factor fail-any | **Avoid (shaped)** | theme_radar_1d | 1 | 1d open→close | AB `status_*` any BAD / `n_bad` > 0 on prior `{D}_ab_checklist_enriched.csv`. Join `veto_when` already named: `earn:today`; `ext:extreme` AND weather `risk=off` (`00_grounding/join_rules.json`). | AB fail-any optics: 52/56; ai_power: 65/70; copper: 65/70; nuclear: 65/70; gold_hit: 26/56. ~93% of stock name-days have some `status_*=BAD` — too wide for a veto. Join `veto_when` optics: 0/76; ai_power: 0/95; copper: 0/95; nuclear: 0/95; gold_hit: 0/76 (0 on these baskets). Enriched AB starts 08-19 — earlier is thin-n. Do not invent a new screener. | Fail-any is a veto shape, not a rank add. | NO. |
| AlphaSuite / ATR risk caps | **Avoid / size (if relevant)** | all flatten_* (size floor) | n/a | prior Elite ATR/Price | Elite `Average True Range` / `Price` → `atr_pct`. Already the liquidity floor `MIN_ATR_PCT=2.5` in `ticker_lookback` / stock-book. Not a long rank. | ATR below floor optics: 0/76; ai_power: 0/95; copper: 0/95; nuclear: 0/95; gold_hit: 18/76. Only GLD (ETF) sits under 2.5% — a gold *hit*, so the floor would have wrongly gated a winner. Optics/AI/copper/nuclear: 0. Size cap only, not a Theme Radar substitute. | Size cap only. Do not score ATR% as buy-rank. | NO. |
| AlphaSift L1→L2 re-rank | **Expand only** | theme_radar_1d | 1 | 1d open→close | Existing layers only: `data/join/{D}_ranked.csv` `total_score` / `score_norm`; book `score_1d`; `data/feature_asof/{D}_feature_asof.csv` `join_rank` / `join` / `ab`. No new sift scrape. | join top-quintile optics: 26/76; ai_power: 34/95; copper: 48/95; nuclear: 26/95; gold_hit: 29/76. These miss names were already graded high on join/AB — L2 on the same layers would have kept them elevated, then they lost. | Do not feed fade columns into a buy re-rank. | NO — existing ranker, already high on miss names; do not bump. |
| vectorbt sweeps | **Expand only** | flatten_h1/h3/h5 | 1/3/5 | Nd open→exit + Futubull | Wrap `factor_mine_book` 09:30 `open` + Futubull fees. Panel: `data/prices/ohlc.parquet` `(date,ticker)`. No vectorbt default close-to-close fills. | Harness not written — 0 fires. Expand-only sidecar. | N/A until a recipe is scored on the both-tape bar. | NO — not a bump column. |
| Zipline cross-section | **Expand only** | flatten_h1/h3/h5 | 1/3/5 | Nd open→exit | Same `data/prices/ohlc.parquet` + PIT book as vectorbt. No Zipline pipeline in-repo. Do not pull Zipline data. | No in-repo pipeline — 0 fires. Thin-n / not wired. | N/A. | NO. |
| OpenBB SEC / surprise | **Expand (thin-gap only)** | sidecar | — | asof < D | Elite already has `EPS Surprise`, `Revenue Surprise`, `Earnings Date`. AB `val_B01_eps_surprise` / `status_B01_eps_surprise`, B02, B17, B18. Do not replace. | No new OpenBB pull. Surprise headers already on prior Elite for names Finviz covers. Gap-fill only when the Elite cell is blank — do not invent 8-K scrapes for OKLO/SMR/GDX. | Same-day surprise on D is a leak. Prior vintage only. | NO — do not bump on a beat. |
| qlib / FinRL sidecars | **Expand only** | sidecar → flatten_h* | matching hold | Nd open→exit (unscored) | `sidecars/qlib/`, `sidecars/finrl/`, `data/sidecars/{name}/{asof}/preds.parquet`. Join `ticker` + `asof_date` < D (or D iff morning-packet vintage). | No sidecar preds on disk — 0 fires. Bar not cleared. | N/A. | NO — offline until the same PIT / fee / audit bar. |

## 5. Sift autopsy fold (`fullscan_autopsy_avoid_elevate.md`)

Source: swarm `/workspace/fullscan_autopsy_avoid_elevate.md` (often not on disk) scored against this panel. Master table: [`EXTERNAL_ENRICH_MAP.md`](EXTERNAL_ENRICH_MAP.md).

| Sift finding | In-repo check | Keep? |
|---|---|---|
| FPE≥35 blocked ~2/15 worst buys (BTBT, INDI) | Yes. ACMR FPE 25, join/AB ~+1. ERO FPE 8.18, join +0.99. | **Keep.** Optional avoid; does not save join-hot cheap names. |
| ERO = bought copper spike, not a missed theme | Yes. Cheap FPE, CANSLIM printed, 1w −11%. 08-27 🔵/🚨/fade **blank** — do not invent a 🚨. | **Keep** cheap≠long. Honest thin on fade cameras. |
| Optics 0/4; AAOI −24% | Always-on high-FPE: AAOI 0/19, COHR 1/19, LITE 1/19, GLW 8/19. No 1d/3d paper lot for AAOI. 1d mean −0.05. | Partial. Do not invent −24%. GLW is the optics FPE fade. |
| AI power 0/5; GEV FPE≥35 on 14/14 | GEV **19/19** (med 39). VRT 1/19, PWR 3/19, ETN/CAT 0. | **GEV = natural Avoid.** Other four are not. |
| Copper graded high then pain | Join top-q 48/95. FCX/TECK/ERO CANSLIM then lost. | **Keep.** Do not elevate copper / CANSLIM. |
| Nuclear mixed; CEG worked | CEG 1d mean +0.41; paper lots 08-14 / 08-19 / 09-03 **lost**. VST paper losers. CCJ 19/19 high-FPE. | Mixed. Do not elevate CEG. CCJ = Avoid. |
| Gold mid-ranks ripped (NEM/AEM) | High-FPE 0/76. NEM join high 12/19, AEM 11/19 — already high. | Fade veto would not have blocked the 8/12 hit. Not an MF long. |
| Only `radar_high_fpe` both-tape YES; no `radar_hot` OR; `elevate_bump` failed; cheap≠long | Matches §1–2. | **Keep.** |
| Top Avoid extras: soft 🚨∧fade; join-hot∧AB-silent micros; optics/AI heat sit (n=1) | Early 🚨/fade empty. Micros = gated_out (expand). Heat sit n=1 = **thin-n**. | Log only. Do not promote. |
| Top Elevate: gold-miner; CANSLIM A∧S; log outweighed | Gold is a contrast hit. A∧S is an unscored subset of a rule that dies on up tapes. Outweighed=20. | **Do not bump.** Log only. |
| REAX +853% tax | Outweighed **and** FPE 65. | Keep FPE veto **optional**. |

## 6. Horizon re-mine checklist (fee-aware KEEP)

Patches must be strategy-native. 1d `Change from Open` is the Theme Radar clock. A `flatten_h5` patch must be scored on **5-session** 09:30-open → exit + Futubull fees (`factor_mine.hold_window`).

Fee-aware KEEP re-mine: [`OVERLAY_HORIZON_BT.md`](OVERLAY_HORIZON_BT.md) · `python3 -m src.overlay_horizon_bt --write`. Bar = sleeve clock + prior Elite + 09:30 open + Futubull + peer-excess both-tape + leftover/unit $ + top-2 day share + walk-forward. Universe-sum of skipped losers is rejected.

| sleeve | hold_sessions | score_clock | overlay status | fee-aware FPE avoid | 1d FPE auto-apply? |
|---|---:|---|---|---|---|
| `theme_radar_1d` | 1 | 1d open→close + fees | Percent IC both-tape YES (this autopsy). | **FAIL** — after Futubull, avoided $ xs vs peer **+0.09** (up-tape +0.35). Not an avoid. | n/a |
| `flatten_h1` | 1 | leftover min-hold 1 + fees | Wish-list ≠ live. | **THIN** — n=11, 0 SPY-down, leftover +$326. Do not promote. | **No.** |
| `flatten_h3` | 3 | leftover min-hold 3 + fees | Wish-list ≠ live. | **THIN** — n=9, leftover +$456. | **No.** |
| `flatten_h5` | 5 | leftover min-hold 5 + fees | 1d FPE does not apply. | **FAIL** — 5d FPE board up-tape flips: IC_up −0.033 Sign_up **40%** (2/5) n=5; IC_down −0.131 Sign_down 100% n=10. Do not wire. Leftover +$723 is not a rescue. | **No.** |
| `flatten_robust` | 3 (recycle) | live 3d size-book + flatten clock | **LIVE. Untouched.** | **FAIL** — gated 08-20/21 gold, veto never fired. | **No.** |
| `flatten_live_h1/h3/h5` | 1 / 3 / 5 | gated tickets | thin-n (2 entry days) | **FAIL** — veto never fired. | **No.** |

Keep did **not** clear a PASS. `flatten_h5` × FPE-avoid = **FAIL / do not wire** (5d board, up-tape flips). FPE stays on the 1d Theme Radar clock only. d_RSI / d_mcap 5d inconclusive. Elevate stays rejected. Do not write `LIVE_POLICY`.

## Optional columns (not live gates)

| column | meaning | promote? |
|---|---|---|
| `avoid_veto` | Theme Radar fade: prior `Forward P/E` ≥ 35 (surviving both-tape) | optional sticker only |
| `radar_rsi_up` / `radar_mcap_up` | d_RSI≥5 / d_mcap≥3% | veto *candidates*, **not** buy-rank fuel |
| `elevate_bump` | CANSLIM + clean radar + AB lead | **do not bump** — failed both-tape |
| `radar_high_fpe` | `Forward P/E` ≥ 35 (prior Elite) | fade sticker; cheap≠long |
| `radar_cheap_fpe` | 0 < FPE ≤ 15 | **not** an elevate |
| `d_rsi` / `d_mcap_pct` | prior vs prior-prior Elite | veto inputs, not rank fuel |
| `mf_flag` / `canslim_flag` | existing style flags | expand / combine, not auto-long |
| join `total_score` | `data/join/{D}_ranked.csv` | **do not bump** |

Join: `Ticker` + feature export date = `feature_export_date(D)`. Script: `python -m src.finviz_style_flags --csv data/exports/finviz_{prior}.csv --prior data/exports/finviz_{prior2}.csv --ab data/ab_checklist/{prior}_ab_checklist_enriched.csv`.

## Do not

- Promote on thin-n or one-tape only.
- Treat Magic Formula cheap as a long overlay.
- Use join `total_score` / CANSLIM / MF to bump a long.
- Feed high Forward P/E, d_RSI, or d_Market Cap into buy-rank fuel.
- Use same-day `Change` / `Gap` / RelVol as an avoid/elevate input.
- Invent scrapes (OpenBB SEC, Zipline data, qlib preds).
- Promote soft 🚨∧fade or optics/AI heat-sit on n=1.
- Elevate on CANSLIM A∧S or gold-miner style without a both-tape bar.
- Copy 1d Theme Radar FPE onto `flatten_h5` or `flatten_robust`. The 5d board **failed both-tape**. Do not wire.
- Elevate on a sleeve whose hold was never scored.
- Edit `LIVE_POLICY` or `flatten_robust`.

