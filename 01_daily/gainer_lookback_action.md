# Gainer lookback action

_Generated 2026-09-03T00:11:19.335627-04:00_

Universe: liquid Finviz top **25** gainers (Change% ≥ 2%, mcap ≥ $100M, adv ≥ 500k) from **2026-08-13** to **2026-09-02**. 350 gainer-days · 276 names · 15 sessions.

Each name is painted with ticker lookback (09:30 cameras + coaches + featured mine setups). The **Action** column is the authoritative BUY / SELL / NO BUY / HOLD. Same-day Change% only selects the universe — it does not enter the call. Catch = BUY and the forward move is up, or SELL and it is down. **pnl 1d** is signed (BUY keeps +1d, SELL flips it). Gainer-morning SELLs are a hard test: those names already ripped, so a fade is fighting the tape. **History** is the fairer read — every printed lookback day of those names.

Default preset **`featured`**. BUY on the gainer morning (recall): **16.9%** (59/350).

First read: **SELL / first-crack is the edge** (~65% 1d on history). Featured **BUY is still ~coin-flip** on +1d for these names. Most rippers print HOLD at 09:30 — we do not invent a long after the fact. Tweak `00_grounding/lookback_action_params.json` and re-run the Action.

## Preset sweep

Paint once, score each rule set. **Gainer-days** = the morning of the rip. **History** = every printed lookback day of those names (the full sheet).

| Preset | Slice | n | BUY | SELL | NO BUY | HOLD | catch 1d | BUY 1d | SELL 1d | catch 3d | catch 1w | 1d+3d | pnl 1d |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `featured` ← | gainer-days | 350 | 59 | 39 | 43 | 209 | 66.7% (54/81) | 57.8% | 77.8% | 51.6% | 59.6% | 43.1% | +2.90% |
| `featured` | history | 4140 | 655 | 446 | 540 | 2499 | 57.5% (462/804) | 50.2% | 64.9% | 54.4% | 55.8% | 37.0% | +2.31% |
| `strict` | gainer-days | 350 | 20 | 39 | 43 | 248 | 69.1% (38/55) | 52.6% | 77.8% | 58.3% | 60.6% | 48.6% | +3.85% |
| `strict` | history | 4140 | 196 | 446 | 540 | 2958 | 60.5% (328/542) | 48.6% | 64.9% | 60.4% | 55.6% | 41.7% | +2.94% |
| `setups` | gainer-days | 350 | 55 | 39 | 0 | 256 | 68.4% (54/79) | 60.5% | 77.8% | 51.6% | 59.6% | 43.1% | +3.16% |
| `setups` | history | 4140 | 625 | 446 | 0 | 3069 | 57.5% (450/782) | 50.0% | 64.9% | 54.4% | 55.8% | 37.0% | +2.33% |
| `lane` | gainer-days | 350 | 13 | 39 | 43 | 255 | 71.8% (28/39) | 0.0% | 77.8% | 70.0% | 70.6% | 55.0% | +4.40% |
| `lane` | history | 4140 | 67 | 446 | 540 | 3087 | 64.0% (277/433) | 54.1% | 64.9% | 63.2% | 59.9% | 42.5% | +1.32% |
| `loose` | gainer-days | 350 | 83 | 39 | 43 | 185 | 66.3% (67/101) | 60.0% | 77.8% | 51.2% | 56.5% | 44.4% | +2.73% |
| `loose` | history | 4140 | 943 | 446 | 540 | 2211 | 58.3% (568/974) | 53.8% | 64.9% | 55.8% | 54.3% | 39.7% | +2.16% |

## How the default call is made

1. A featured **fade** setup (`first crack`, `🚨+heat🔴`) → **SELL**.
2. Hall pass **blocked** → **NO BUY**.
3. Hall pass standard / group leader / catalyst / probable → **BUY**.
4. Else a featured **long** setup with 1d edge ≥ the preset cut (vol+AB, vol+gen🔴, vol+join🔴, 🔵+heat) → **BUY**.
5. Else **HOLD**. Pre-lattice days (before 2026-08-31) have no hall pass, so setups carry the call.

Judge-yellow and 🔵-stretch stay out of the default cut (edge too soft / too common). Flip to `loose` to include them.

## Gainer mornings (default preset)

| Date | # | Ticker | Δ that day | Action | Why | Lane | Setups | +1d | +3d | +1w | 1d | 3d | 1w |
|---|---:|---|---:|---|---|---|---|---:|---:|---:|---|---|---|
| 2026-08-13 | 1 | `ARX` | +43.44% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.36% | +0.26% | +0.31% | — | — | — |
| 2026-08-13 | 2 | `OMER` | +21.47% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.92% | -0.92% | +7.38% | — | — | — |
| 2026-08-13 | 3 | `AIRO` | +20.73% | **HOLD** | no featured long / fade / lane | ⬜ | — | -10.64% | -16.15% | -23.06% | — | — | — |
| 2026-08-13 | 4 | `NCMI` | +18.69% | **HOLD** | no featured long / fade / lane | ⬜ | — | +6.72% | -5.97% | -4.85% | — | — | — |
| 2026-08-13 | 5 | `MXCT` | +17.72% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.04% | -8.63% | -0.72% | — | — | — |
| 2026-08-13 | 6 | `QMLS` | +17.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.25% | -6.78% | -25.86% | — | — | — |
| 2026-08-13 | 7 | `AVAH` | +16.96% | **HOLD** | no featured long / fade / lane | ⬜ | — | +9.12% | +12.22% | +14.88% | — | — | — |
| 2026-08-13 | 8 | `ANGX` | +16.71% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.16% | +12.27% | +1.16% | — | — | — |
| 2026-08-13 | 9 | `CRMD` | +16.62% | **HOLD** | no featured long / fade / lane | ⬜ | — | -7.37% | +0.37% | +1.11% | — | — | — |
| 2026-08-13 | 10 | `TBBB` | +15.98% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.87% | -2.32% | +1.16% | — | — | — |
| 2026-08-13 | 11 | `LVWR` | +15.19% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.00% | -0.80% | -9.60% | — | — | — |
| 2026-08-13 | 12 | `AMPY` | +15.15% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.03% | -0.21% | +1.66% | — | — | — |
| 2026-08-13 | 13 | `SNDK` | +14.90% | **HOLD** | no featured long / fade / lane | ⬜ | — | +7.39% | +6.39% | +4.75% | — | — | — |
| 2026-08-13 | 14 | `BIRK` | +14.06% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.02% | -9.20% | -15.73% | — | — | — |
| 2026-08-13 | 15 | `MH` | +13.65% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.46% | -3.32% | -5.23% | — | — | — |
| 2026-08-13 | 16 | `BCAR` | +13.00% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.79% | -12.05% | -14.85% | — | — | — |
| 2026-08-13 | 17 | `WWW` | +11.30% | **HOLD** | no featured long / fade / lane | ⬜ | — | +5.79% | +0.55% | +2.87% | — | — | — |
| 2026-08-13 | 18 | `CRDL` | +10.86% | **HOLD** | no featured long / fade / lane | ⬜ | — | +5.26% | +10.82% | +10.23% | — | — | — |
| 2026-08-13 | 19 | `GANX` | +10.85% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.08% | -6.24% | -8.54% | — | — | — |
| 2026-08-13 | 20 | `ZENA` | +10.53% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.47% | -4.22% | -10.33% | — | — | — |
| 2026-08-13 | 21 | `HYLN` | +9.13% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.22% | -6.08% | -18.00% | — | — | — |
| 2026-08-13 | 22 | `HLIT` | +8.75% | **HOLD** | no featured long / fade / lane | ⬜ | — | +6.42% | -2.68% | -5.43% | — | — | — |
| 2026-08-13 | 23 | `SKHY` | +8.60% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.40% | -6.07% | -1.56% | — | — | — |
| 2026-08-13 | 24 | `QMCO` | +8.55% | **HOLD** | no featured long / fade / lane | ⬜ | — | +5.79% | -4.70% | -16.98% | — | — | — |
| 2026-08-13 | 25 | `ANRO` | +8.53% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.49% | +5.67% | +6.04% | — | — | — |
| 2026-08-14 | 1 | `CAPR` | +53.44% | **HOLD** | no featured long / fade / lane | ⬜ | — | +12.03% | +20.00% | -5.41% | — | — | — |
| 2026-08-14 | 2 | `HTFL` | +31.70% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.33% | +8.20% | +18.23% | — | — | — |
| 2026-08-14 | 3 | `UMAC` | +24.49% | **HOLD** | no featured long / fade / lane | ⬜ | — | -11.48% | -16.91% | -19.47% | — | — | — |
| 2026-08-14 | 4 | `NPWR` | +20.37% | **HOLD** | no featured long / fade / lane | ⬜ | — | -11.28% | -14.36% | -12.82% | — | — | — |
| 2026-08-14 | 5 | `LPTH` | +18.29% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.44% | -12.72% | -4.94% | — | — | — |
| 2026-08-14 | 6 | `NMAX` | +16.44% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.69% | +0.37% | -1.10% | — | — | — |
| 2026-08-14 | 7 | `ALOY` | +15.17% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.88% | -15.01% | -15.08% | — | — | — |
| 2026-08-14 | 8 | `INO` | +14.99% | **SELL** | fade: first crack | ⬜ | first crack — | +5.50% | +19.27% | +8.26% | ❌ | ❌ | ❌ |
| 2026-08-14 | 9 | `AAOI` | +14.84% | **HOLD** | no featured long / fade / lane | ⬜ | — | +3.07% | -18.69% | -16.94% | — | — | — |
| 2026-08-14 | 10 | `RDDT` | +13.62% | **HOLD** | no featured long / fade / lane | ⬜ | — | -7.63% | -14.81% | -13.93% | — | — | — |
| 2026-08-14 | 11 | `BYND` | +12.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | -11.98% | -1.38% | +6.97% | — | — | — |
| 2026-08-14 | 12 | `CDNL` | +11.90% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.48% | +9.92% | +10.35% | — | — | — |
| 2026-08-14 | 13 | `ABX` | +10.98% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.62% | -1.29% | +4.21% | — | — | — |
| 2026-08-14 | 14 | `STDN` | +10.50% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.00% | -10.07% | -5.03% | — | — | — |
| 2026-08-14 | 15 | `METC` | +10.49% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.16% | -1.55% | -2.86% | — | — | — |
| 2026-08-14 | 16 | `FCEL` | +10.47% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.04% | -9.25% | -12.65% | — | — | — |
| 2026-08-14 | 17 | `KOPN` | +10.46% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.27% | -10.36% | -8.36% | — | — | — |
| 2026-08-14 | 18 | `NU` | +10.37% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.22% | -4.07% | -4.27% | — | — | — |
| 2026-08-14 | 19 | `AEHR` | +10.31% | **HOLD** | no featured long / fade / lane | ⬜ | — | +8.62% | -19.47% | -24.12% | — | — | — |
| 2026-08-14 | 20 | `CLYM` | +10.11% | **HOLD** | no featured long / fade / lane | ⬜ | — | +6.80% | +6.18% | +5.82% | — | — | — |
| 2026-08-14 | 21 | `LUNR` | +10.02% | **HOLD** | no featured long / fade / lane | ⬜ | — | +7.21% | -2.58% | -3.63% | — | — | — |
| 2026-08-14 | 22 | `OUST` | +9.56% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.19% | -17.76% | -21.12% | — | — | — |
| 2026-08-14 | 23 | `BORR` | +9.53% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.58% | -0.68% | +1.58% | — | — | — |
| 2026-08-14 | 24 | `VERA` | +9.43% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.00% | +2.04% | +2.04% | — | — | — |
| 2026-08-14 | 25 | `CELC` | +9.37% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.48% | +1.79% | +1.66% | — | — | — |
| 2026-08-17 | 1 | `WFF` | +237.15% | **SELL** | fade: first crack | ⬜ | first crack — | +9.90% | +8.91% | -2.97% | ❌ | ❌ | ✅ |
| 2026-08-17 | 2 | `FDMT` | +20.43% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.20% | +10.69% | -0.65% | — | — | — |
| 2026-08-17 | 3 | `OABI` | +17.75% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.62% | +12.56% | +14.92% | — | — | — |
| 2026-08-17 | 4 | `JLHL` | +14.95% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.87% | -6.80% | -11.33% | — | — | — |
| 2026-08-17 | 5 | `CBRS` | +14.55% | **SELL** | fade: first crack | ⬜ | first crack — | -12.69% | -16.72% | -25.70% | ✅ | ✅ | ✅ |
| 2026-08-17 | 6 | `AXTI` | +14.00% | **SELL** | fade: first crack | ⬜ | first crack — | -14.23% | -23.81% | -33.23% | ✅ | ✅ | ✅ |
| 2026-08-17 | 7 | `HIVE` | +10.83% | **HOLD** | no featured long / fade / lane | ⬜ | — | -9.45% | +1.95% | -4.24% | — | — | — |
| 2026-08-17 | 8 | `ANGX` | +9.61% | **SELL** | fade: first crack | ⬜ | first crack — | +2.97% | -7.22% | -5.95% | ❌ | ✅ | ✅ |
| 2026-08-17 | 9 | `COHR` | +9.47% | **SELL** | fade: first crack | ⬜ | first crack — | -12.75% | -17.42% | -21.74% | ✅ | ✅ | ✅ |
| 2026-08-17 | 10 | `SNDK` | +9.47% | **SELL** | fade: first crack | ⬜ | first crack — | -9.01% | -10.42% | -16.96% | ✅ | ✅ | ✅ |
| 2026-08-17 | 11 | `SMTC` | +9.29% | **SELL** | fade: first crack | ⬜ | first crack — | -12.31% | -19.02% | -22.29% | ✅ | ✅ | ✅ |
| 2026-08-17 | 12 | `FIGR` | +9.24% | **SELL** | fade: first crack | ⬜ | first crack — | -0.66% | +0.85% | +8.46% | ✅ | ❌ | ❌ |
| 2026-08-17 | 13 | `TDTH` | +8.99% | **SELL** | fade: first crack | ⬜ | first crack — | +6.22% | -4.66% | -0.52% | ❌ | ✅ | ✅ |
| 2026-08-17 | 14 | `QTRX` | +8.88% | **HOLD** | no featured long / fade / lane | ⬜ | — | -8.22% | +6.85% | -5.48% | — | — | — |
| 2026-08-17 | 15 | `HTHT` | +8.70% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.48% | +6.29% | +3.95% | — | — | — |
| 2026-08-17 | 16 | `ALM` | +8.52% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.64% | +8.13% | +10.02% | — | — | — |
| 2026-08-17 | 17 | `PGEN` | +8.11% | **HOLD** | no featured long / fade / lane | ⬜ | — | +3.05% | +13.59% | +7.94% | — | — | — |
| 2026-08-17 | 18 | `CRDO` | +7.83% | **SELL** | fade: first crack | ⬜ | first crack — | -13.03% | -18.20% | -22.20% | ✅ | ✅ | ✅ |
| 2026-08-17 | 19 | `INDP` | +7.48% | **HOLD** | no featured long / fade / lane | ⬜ | — | -6.40% | +38.00% | +16.00% | — | — | — |
| 2026-08-17 | 20 | `MRVL` | +7.42% | **SELL** | fade: first crack | ⬜ | first crack — | -7.82% | +7.12% | -2.85% | ✅ | ❌ | ✅ |
| 2026-08-17 | 21 | `LITE` | +7.07% | **SELL** | fade: first crack | ⬜ | first crack — | -9.87% | -9.25% | -15.10% | ✅ | ✅ | ✅ |
| 2026-08-17 | 22 | `DSX` | +7.06% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.38% | -0.38% | +1.51% | — | — | — |
| 2026-08-17 | 23 | `NXE` | +6.98% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.56% | -4.93% | +0.65% | — | — | — |
| 2026-08-17 | 24 | `PURR` | +6.78% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.70% | +40.98% | +51.75% | — | — | — |
| 2026-08-17 | 25 | `WDC` | +6.59% | **SELL** | fade: first crack | ⬜ | first crack — | -7.43% | -12.49% | -19.09% | ✅ | ✅ | ✅ |
| 2026-08-18 | 1 | `AMLX` | +48.48% | **HOLD** | no featured long / fade / lane | ⬜ | — | +9.94% | +10.11% | +9.03% | — | — | — |
| 2026-08-18 | 2 | `WEAV` | +31.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.41% | +0.14% | +0.14% | — | — | — |
| 2026-08-18 | 3 | `WFF` | +25.87% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | -0.90% | -6.76% | -13.06% | ❌ | ❌ | ❌ |
| 2026-08-18 | 4 | `HAE` | +16.68% | **HOLD** | no featured long / fade / lane | ⬜ | — | +3.59% | +3.97% | +4.05% | — | — | — |
| 2026-08-18 | 5 | `DUOT` | +15.22% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | -7.25% | -10.96% | -10.01% | ❌ | ❌ | ❌ |
| 2026-08-18 | 6 | `IVVD` | +14.68% | **HOLD** | no featured long / fade / lane | ⬜ | — | +10.60% | +14.86% | +10.60% | — | — | — |
| 2026-08-18 | 7 | `CHRS` | +13.91% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.75% | +5.97% | +1.49% | — | — | — |
| 2026-08-18 | 8 | `UGI` | +12.35% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.42% | -2.08% | -0.52% | — | — | — |
| 2026-08-18 | 9 | `ALEC` | +10.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | +9.96% | +6.79% | +4.07% | — | — | — |
| 2026-08-18 | 10 | `GUTS` | +10.00% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.32% | +8.79% | +8.79% | — | — | — |
| 2026-08-18 | 11 | `KURA` | +9.84% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.62% | -3.57% | +10.14% | — | — | — |
| 2026-08-18 | 12 | `DVLT` | +9.18% | **HOLD** | no featured long / fade / lane | ⬜ | — | -23.08% | -17.95% | -20.51% | — | — | — |
| 2026-08-18 | 13 | `SENS` | +8.93% | **HOLD** | no featured long / fade / lane | ⬜ | — | +4.83% | +11.74% | +14.27% | — | — | — |
| 2026-08-18 | 14 | `EYPT` | +8.73% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +15.17% | -2.74% | -9.69% | ✅ | ❌ | ❌ |
| 2026-08-18 | 15 | `TDTH` | +8.55% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.39% | -8.78% | -4.88% | — | — | — |
| 2026-08-18 | 16 | `NMAX` | +8.25% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | -4.55% | -5.95% | -6.56% | ❌ | ❌ | ❌ |
| 2026-08-18 | 17 | `TRGP` | +7.54% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.23% | +0.45% | -1.78% | — | — | — |
| 2026-08-18 | 18 | `REAX` | +7.39% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +5.28% | -0.38% | +805.66% | ✅ | ❌ | ✅ |
| 2026-08-18 | 19 | `FOUR` | +7.29% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.92% | +2.63% | +5.14% | — | — | — |
| 2026-08-18 | 20 | `ANGX` | +7.29% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | -5.16% | -11.13% | -7.22% | ❌ | ❌ | ❌ |
| 2026-08-18 | 21 | `DUOL` | +7.16% | **HOLD** | no featured long / fade / lane | ⬜ | — | +4.58% | +4.65% | +5.24% | — | — | — |
| 2026-08-18 | 22 | `BBWI` | +6.78% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.85% | -1.95% | -2.85% | — | — | — |
| 2026-08-18 | 23 | `RANI` | +6.77% | **HOLD** | no featured long / fade / lane | ⬜ | — | +11.15% | +6.67% | +9.09% | — | — | — |
| 2026-08-18 | 24 | `SECZ` | +6.59% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +2.38% | +4.08% | +10.36% | ✅ | ✅ | ✅ |
| 2026-08-18 | 25 | `ULTA` | +6.24% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.15% | +0.91% | +4.42% | — | — | — |
| 2026-08-19 | 1 | `MRNA` | +123.42% | **HOLD** | no featured long / fade / lane | ⬜ | — | -23.55% | -20.13% | — | — | — | — |
| 2026-08-19 | 2 | `AZI` | +46.96% | **HOLD** | no featured long / fade / lane | ⬜ | — | -13.25% | -15.66% | — | — | — | — |
| 2026-08-19 | 3 | `MRVI` | +25.21% | **HOLD** | no featured long / fade / lane | ⬜ | — | +14.88% | +14.88% | — | — | — | — |
| 2026-08-19 | 4 | `CYPH` | +21.21% | **HOLD** | no featured long / fade / lane | ⬜ | — | +11.21% | +53.27% | — | — | — | — |
| 2026-08-19 | 5 | `TEM` | +20.83% | **HOLD** | no featured long / fade / lane | ⬜ | — | +8.82% | +9.55% | — | — | — | — |
| 2026-08-19 | 6 | `BNTX` | +19.20% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.97% | -1.57% | — | — | — | — |
| 2026-08-19 | 7 | `HUMA` | +19.08% | **HOLD** | no featured long / fade / lane | ⬜ | — | -7.35% | -8.84% | — | — | — | — |
| 2026-08-19 | 8 | `BTGO` | +18.74% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 —; 🔵 stretch — | +5.60% | +11.52% | — | ✅ | ✅ | — |
| 2026-08-19 | 9 | `TWST` | +18.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.26% | -1.10% | — | — | — | — |
| 2026-08-19 | 10 | `ABTC` | +18.22% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +7.21% | +6.96% | — | — | — | — |
| 2026-08-19 | 11 | `DNA` | +17.02% | **HOLD** | no featured long / fade / lane | ⬜ | — | -9.73% | -9.47% | — | — | — | — |
| 2026-08-19 | 12 | `EL` | +15.46% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.90% | +4.94% | — | — | — | — |
| 2026-08-19 | 13 | `CDE` | +15.05% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +0.86% | -2.10% | — | — | — | — |
| 2026-08-19 | 14 | `ASST` | +14.14% | **HOLD** | no featured long / fade / lane | ⬜ | — | +9.50% | +34.55% | — | — | — | — |
| 2026-08-19 | 15 | `HL` | +14.12% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +1.36% | -1.85% | — | — | — | — |
| 2026-08-19 | 16 | `SG` | +13.82% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.49% | +7.17% | — | — | — | — |
| 2026-08-19 | 17 | `MSTR` | +13.73% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +7.81% | +19.51% | — | — | — | — |
| 2026-08-19 | 18 | `SBET` | +13.59% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +7.20% | +18.08% | — | — | — | — |
| 2026-08-19 | 19 | `BMNR` | +13.46% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +6.57% | +20.95% | — | — | — | — |
| 2026-08-19 | 20 | `EXK` | +13.30% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.18% | -2.27% | — | — | — | — |
| 2026-08-19 | 21 | `PPC` | +13.20% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.13% | +3.00% | — | — | — | — |
| 2026-08-19 | 22 | `SCZM` | +12.95% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.31% | -0.10% | — | — | — | — |
| 2026-08-19 | 23 | `NG` | +12.75% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.36% | +9.22% | — | — | — | — |
| 2026-08-19 | 24 | `PACB` | +12.72% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.15% | +0.00% | — | — | — | — |
| 2026-08-19 | 25 | `BLSH` | +12.56% | **HOLD** | no featured long / fade / lane | ⬜ | — | +5.65% | +14.71% | — | — | — | — |
| 2026-08-20 | 1 | `INDP` | +32.98% | **HOLD** | no featured long / fade / lane | ⬜ | — | -6.52% | -9.42% | -17.39% | — | — | — |
| 2026-08-20 | 2 | `CAN` | +17.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | +27.24% | +29.03% | +43.37% | — | — | — |
| 2026-08-20 | 3 | `BTBT` | +15.71% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 — | -4.08% | -4.08% | -2.19% | ❌ | ❌ | ❌ |
| 2026-08-20 | 4 | `MRVI` | +14.40% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | +5.33% | +2.79% | +7.75% | ✅ | ✅ | ✅ |
| 2026-08-20 | 5 | `DFDV` | +13.89% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+join🔴 —; 🔵 stretch — | +4.51% | +10.35% | +19.36% | ✅ | ✅ | ✅ |
| 2026-08-20 | 6 | `TEM` | +11.43% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | +9.06% | +0.49% | +2.75% | ✅ | ✅ | ✅ |
| 2026-08-20 | 7 | `ENHA` | +9.98% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +1.18% | -2.35% | -5.29% | — | — | — |
| 2026-08-20 | 8 | `MARA` | +9.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.99% | +1.26% | +0.63% | — | — | — |
| 2026-08-20 | 9 | `DE` | +9.30% | **HOLD** | no featured long / fade / lane | ⬜ | — | +4.27% | +4.54% | +2.19% | — | — | — |
| 2026-08-20 | 10 | `QDEL` | +9.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.74% | -2.42% | +0.40% | — | — | — |
| 2026-08-20 | 11 | `XXI` | +9.26% | **HOLD** | no featured long / fade / lane | ⬜ | — | +4.85% | +3.72% | +1.78% | — | — | — |
| 2026-08-20 | 12 | `ORBS` | +9.20% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 — | +6.41% | +1.57% | -3.27% | ✅ | ✅ | ❌ |
| 2026-08-20 | 13 | `ARCT` | +9.13% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +22.38% | +29.30% | +44.04% | — | — | — |
| 2026-08-20 | 14 | `PRQR` | +8.78% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +3.08% | +9.69% | +3.52% | — | — | — |
| 2026-08-20 | 15 | `IOVA` | +8.70% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | -7.79% | -10.12% | -10.12% | ❌ | ❌ | ❌ |
| 2026-08-20 | 16 | `BTDR` | +8.62% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+join🔴 —; 🔵 stretch — | +9.01% | +8.15% | +2.30% | ✅ | ✅ | ✅ |
| 2026-08-20 | 17 | `GORO` | +8.48% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +6.33% | +18.67% | +18.67% | — | — | — |
| 2026-08-20 | 18 | `HIVE` | +8.43% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -3.19% | -7.67% | -8.31% | — | — | — |
| 2026-08-20 | 19 | `GRAL` | +8.41% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +2.01% | -1.78% | +2.08% | — | — | — |
| 2026-08-20 | 20 | `CYPH` | +8.40% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | +19.33% | +37.81% | +36.98% | ✅ | ✅ | ✅ |
| 2026-08-20 | 21 | `MSTR` | +7.73% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 —; 🔵 stretch — | +6.10% | +8.20% | +9.61% | ✅ | ✅ | ✅ |
| 2026-08-20 | 22 | `ABTC` | +7.72% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -6.38% | +2.71% | -3.42% | — | — | — |
| 2026-08-20 | 23 | `CAI` | +7.62% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | +6.88% | +9.03% | +9.11% | ✅ | ✅ | ✅ |
| 2026-08-20 | 24 | `QTRX` | +7.60% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.17% | -10.26% | -14.10% | — | — | — |
| 2026-08-20 | 25 | `TRON` | +7.33% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 — | +7.49% | +9.09% | +14.97% | ✅ | ✅ | ✅ |
| 2026-08-21 | 1 | `USDE` | +78.75% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; 🔵 stretch — | -8.64% | — | +8.91% | ❌ | — | ✅ |
| 2026-08-21 | 2 | `CAN` | +25.53% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 —; 🔵 stretch — | +4.22% | — | +23.94% | ✅ | — | ✅ |
| 2026-08-21 | 3 | `ARCT` | +20.61% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB — | +2.31% | — | +20.22% | ✅ | — | ✅ |
| 2026-08-21 | 4 | `CRML` | +17.67% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.08% | — | +13.08% | — | — | — |
| 2026-08-21 | 5 | `ASST` | +17.39% | **HOLD** | no featured long / fade / lane | ⬜ | — | +8.78% | — | +26.89% | — | — | — |
| 2026-08-21 | 6 | `BKKT` | +15.93% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+join🔴 — | -3.38% | — | -1.98% | ❌ | — | ❌ |
| 2026-08-21 | 7 | `TMC` | +15.68% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.17% | — | +6.26% | — | — | — |
| 2026-08-21 | 8 | `GUTS` | +15.34% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.85% | — | -5.13% | — | — | — |
| 2026-08-21 | 9 | `QSI` | +15.13% | **HOLD** | no featured long / fade / lane | ⬜ | — | -8.98% | — | -4.19% | — | — | — |
| 2026-08-21 | 10 | `ELMT` | +14.61% | **HOLD** | no featured long / fade / lane | ⬜ | — | -9.85% | — | -9.15% | — | — | — |
| 2026-08-21 | 11 | `SAFX` | +14.09% | **HOLD** | no featured long / fade / lane | ⬜ | — | +7.69% | — | +13.85% | — | — | — |
| 2026-08-21 | 12 | `DEFT` | +13.07% | **HOLD** | no featured long / fade / lane | ⬜ | — | +13.04% | — | +13.04% | — | — | — |
| 2026-08-21 | 13 | `HOOD` | +13.06% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.92% | — | +1.51% | — | — | — |
| 2026-08-21 | 14 | `LAR` | +12.46% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -1.61% | — | +1.17% | — | — | — |
| 2026-08-21 | 15 | `DNN` | +11.46% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | +1.14% | — | +4.86% | — | — | — |
| 2026-08-21 | 16 | `INFQ` | +11.12% | **HOLD** | no featured long / fade / lane | ⬜ | — | -7.72% | — | +1.42% | — | — | — |
| 2026-08-21 | 17 | `SLS` | +11.04% | **HOLD** | no featured long / fade / lane | ⬜ | — | -8.54% | — | -1.62% | — | — | — |
| 2026-08-21 | 18 | `USAR` | +10.99% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -5.30% | — | -0.05% | — | — | — |
| 2026-08-21 | 19 | `ALOY` | +10.93% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -8.48% | — | -8.64% | — | — | — |
| 2026-08-21 | 20 | `UEC` | +10.81% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -1.96% | — | +6.74% | — | — | — |
| 2026-08-21 | 21 | `OI` | +10.58% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.70% | — | +0.42% | — | — | — |
| 2026-08-21 | 22 | `EROC` | +10.11% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.53% | — | -6.37% | — | — | — |
| 2026-08-21 | 23 | `EU` | +10.05% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.67% | — | +15.83% | — | — | — |
| 2026-08-21 | 24 | `COIN` | +9.80% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | -1.28% | — | +2.27% | — | — | — |
| 2026-08-21 | 25 | `ABAT` | +9.66% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.06% | — | +3.89% | — | — | — |
| 2026-08-24 | 1 | `BMEA` | +22.62% | **SELL** | fade: first crack | ⬜ | first crack — | -4.17% | +1.79% | +1.79% | ✅ | ❌ | ❌ |
| 2026-08-24 | 2 | `NPWR` | +20.29% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.46% | -11.71% | -11.22% | — | — | — |
| 2026-08-24 | 3 | `PUSA` | +16.62% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.25% | -3.75% | -5.00% | — | — | — |
| 2026-08-24 | 4 | `ALVO` | +16.28% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+gen🔴 — | +1.94% | -4.66% | -3.69% | ✅ | ❌ | ❌ |
| 2026-08-24 | 5 | `SUJA` | +15.69% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.94% | +6.19% | +13.50% | — | — | — |
| 2026-08-24 | 6 | `CYPH` | +15.49% | **SELL** | fade: first crack | ⬜ | vol+AB —; vol+gen🔴 —; first crack — | +0.00% | -0.61% | +10.98% | ❌ | ✅ | ❌ |
| 2026-08-24 | 7 | `FWDI` | +13.29% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +0.69% | +1.89% | +2.40% | ✅ | ✅ | ✅ |
| 2026-08-24 | 8 | `DEFT` | +13.05% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+gen🔴 — | -4.62% | -9.23% | -4.62% | ❌ | ❌ | ❌ |
| 2026-08-24 | 9 | `CAPR` | +12.16% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +1.99% | +32.77% | +32.77% | ✅ | ✅ | ✅ |
| 2026-08-24 | 10 | `GORO` | +11.91% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; jdg🟡 — | -0.28% | -0.28% | +2.24% | ❌ | ❌ | ✅ |
| 2026-08-24 | 11 | `ASST` | +8.78% | **BUY** | setup vol+gen🔴 (1d edge +1.91) | ⬜ | vol+gen🔴 —; vol+join🔴 — | +1.92% | +8.48% | +12.26% | ✅ | ✅ | ✅ |
| 2026-08-24 | 12 | `ALIT` | +8.15% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.32% | -6.83% | -8.84% | — | — | — |
| 2026-08-24 | 13 | `ZURA` | +7.95% | **HOLD** | no featured long / fade / lane | ⬜ | — | +2.85% | -5.22% | -10.76% | — | — | — |
| 2026-08-24 | 14 | `SAFX` | +7.93% | **HOLD** | no featured long / fade / lane | ⬜ | — | +5.71% | +11.43% | +5.71% | — | — | — |
| 2026-08-24 | 15 | `VITL` | +7.57% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.94% | -3.78% | -4.22% | — | — | — |
| 2026-08-24 | 16 | `JANX` | +7.53% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.55% | +1.02% | -4.01% | — | — | — |
| 2026-08-24 | 17 | `BMNR` | +7.23% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+gen🔴 —; vol+join🔴 — | -1.10% | +1.76% | -2.17% | ❌ | ✅ | ❌ |
| 2026-08-24 | 18 | `RUM` | +7.18% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.61% | -3.30% | -8.66% | — | — | — |
| 2026-08-24 | 19 | `KURA` | +6.90% | **HOLD** | no featured long / fade / lane | ⬜ | — | +6.84% | +2.75% | -0.08% | — | — | — |
| 2026-08-24 | 20 | `CCOI` | +6.83% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.39% | +2.55% | -3.16% | — | — | — |
| 2026-08-24 | 21 | `EZPW` | +6.73% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.87% | -1.43% | -4.04% | — | — | — |
| 2026-08-24 | 22 | `LIFE` | +6.67% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.77% | +7.33% | +9.06% | — | — | — |
| 2026-08-24 | 23 | `ZIP` | +6.65% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+gen🔴 —; vol+join🔴 — | +1.11% | -4.01% | -1.56% | ✅ | ❌ | ❌ |
| 2026-08-24 | 24 | `ABTC` | +6.62% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+gen🔴 —; vol+join🔴 — | +2.96% | -3.19% | -7.57% | ✅ | ❌ | ❌ |
| 2026-08-24 | 25 | `DFDV` | +6.35% | **SELL** | fade: first crack | ⬜ | vol+AB —; vol+gen🔴 —; vol+join🔴 —; first crack — | -0.72% | +7.40% | +17.18% | ✅ | ❌ | ❌ |
| 2026-08-25 | 1 | `KURA` | +9.52% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB —; vol+join🔴 — | — | -2.95% | -7.00% | — | ❌ | ❌ |
| 2026-08-25 | 2 | `AVBP` | +9.34% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | +0.32% | -2.75% | — | — | — |
| 2026-08-25 | 3 | `FLNC` | +8.29% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -2.81% | -8.84% | — | — | — |
| 2026-08-25 | 4 | `INDP` | +7.97% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -7.20% | -8.80% | — | — | — |
| 2026-08-25 | 5 | `ABX` | +7.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -4.66% | -6.89% | — | — | — |
| 2026-08-25 | 6 | `AVEX` | +6.44% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | — | -2.09% | -8.82% | — | — | — |
| 2026-08-25 | 7 | `ITG` | +6.03% | **HOLD** | no featured long / fade / lane | ⬜ | 🔵 stretch — | — | +4.63% | -1.06% | — | — | — |
| 2026-08-25 | 8 | `SENS` | +5.81% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -5.34% | -6.45% | — | — | — |
| 2026-08-25 | 9 | `CAPR` | +5.77% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 —; 🔵 stretch — | — | +39.92% | +41.73% | — | ✅ | ✅ |
| 2026-08-25 | 10 | `BE` | +5.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | +0.97% | -6.30% | — | — | — |
| 2026-08-25 | 11 | `BZ` | +5.70% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | +10.29% | +5.21% | — | — | — |
| 2026-08-25 | 12 | `AXTI` | +5.66% | **HOLD** | no featured long / fade / lane | ⬜ | jdg🟡 — | — | -3.15% | -15.06% | — | — | — |
| 2026-08-25 | 13 | `RZLT` | +5.59% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -5.86% | -12.10% | — | — | — |
| 2026-08-25 | 14 | `ACRS` | +5.55% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -7.51% | -10.51% | — | — | — |
| 2026-08-25 | 15 | `NVTS` | +5.48% | **HOLD** | no featured long / fade / lane | ⬜ | jdg🟡 — | — | -3.02% | -11.94% | — | — | — |
| 2026-08-25 | 16 | `ASPN` | +5.39% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -0.95% | -5.49% | — | — | — |
| 2026-08-25 | 17 | `IRDM` | +5.35% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -4.39% | -5.74% | — | — | — |
| 2026-08-25 | 18 | `TMCI` | +5.26% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | +0.00% | +0.00% | — | — | — |
| 2026-08-25 | 19 | `CNTN` | +5.14% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 —; 🔵 stretch — | — | +4.44% | +1.33% | — | ✅ | ✅ |
| 2026-08-25 | 20 | `BKSY` | +5.00% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -7.85% | -13.58% | — | — | — |
| 2026-08-25 | 21 | `ALOY` | +4.87% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -1.81% | -11.87% | — | — | — |
| 2026-08-25 | 22 | `SVC` | +4.74% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -8.94% | -5.24% | — | — | — |
| 2026-08-25 | 23 | `MAIR` | +4.73% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | +3.90% | +0.96% | — | — | — |
| 2026-08-25 | 24 | `AAOI` | +4.70% | **BUY** | setup vol+join🔴 (1d edge +1.74) | ⬜ | vol+join🔴 —; jdg🟡 — | — | +0.52% | -7.16% | — | ✅ | ❌ |
| 2026-08-25 | 25 | `BRR` | +4.65% | **HOLD** | no featured long / fade / lane | ⬜ | — | — | -4.00% | -4.00% | — | — | — |
| 2026-08-27 | 1 | `ANF` | +35.67% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.35% | -3.21% | — | — | — | — |
| 2026-08-27 | 2 | `BHVN` | +17.87% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.90% | -8.85% | — | — | — | — |
| 2026-08-27 | 3 | `BZ` | +15.65% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.46% | -8.86% | — | — | — | — |
| 2026-08-27 | 4 | `CAPR` | +12.91% | **HOLD** | no featured long / fade / lane | ⬜ | — | +7.48% | +8.87% | — | — | — | — |
| 2026-08-27 | 5 | `LVWR` | +11.38% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.73% | -13.87% | — | — | — | — |
| 2026-08-27 | 6 | `SEDG` | +10.71% | **HOLD** | no featured long / fade / lane | ⬜ | jdg🟡 — | +1.30% | -3.87% | — | — | — | — |
| 2026-08-27 | 7 | `SMTC` | +10.41% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 —; 🔵 stretch — | +1.16% | -8.03% | — | ✅ | ❌ | — |
| 2026-08-27 | 8 | `GRRR` | +10.04% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 —; 🔵 stretch — | +1.36% | -4.21% | — | ✅ | ❌ | — |
| 2026-08-27 | 9 | `URBN` | +9.46% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.01% | -2.73% | — | — | — | — |
| 2026-08-27 | 10 | `VYX` | +9.23% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 —; 🔵 stretch — | +3.38% | -6.87% | — | ✅ | ❌ | — |
| 2026-08-27 | 11 | `PYXS` | +9.15% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.60% | -5.99% | — | — | — | — |
| 2026-08-27 | 12 | `SAFX` | +9.07% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 — | -5.13% | -5.13% | — | ❌ | ❌ | — |
| 2026-08-27 | 13 | `SIMO` | +8.80% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 — | -3.05% | -8.33% | — | ❌ | ❌ | — |
| 2026-08-27 | 14 | `OPTX` | +8.55% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 —; 🔵 stretch — | +2.58% | -3.76% | — | ✅ | ❌ | — |
| 2026-08-27 | 15 | `XPOF` | +8.40% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.11% | -4.22% | — | — | — | — |
| 2026-08-27 | 16 | `TTMI` | +8.37% | **BUY** | setup 🔵+heat🟢 (1d edge +1.76) | ⬜ | 🔵+heat🟢 —; jdg🟡 —; 🔵 stretch — | +2.49% | -3.91% | — | ✅ | ❌ | — |
| 2026-08-27 | 17 | `EQ` | +8.26% | **HOLD** | no featured long / fade / lane | ⬜ | — | +3.81% | -3.81% | — | — | — | — |
| 2026-08-27 | 18 | `APMD` | +8.07% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.91% | -12.10% | — | — | — | — |
| 2026-08-27 | 19 | `OPTU` | +8.00% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.56% | -10.19% | — | — | — | — |
| 2026-08-27 | 20 | `ERAS` | +7.83% | **HOLD** | no featured long / fade / lane | ⬜ | — | +1.77% | -7.57% | — | — | — | — |
| 2026-08-27 | 21 | `BBWI` | +7.51% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.32% | +1.06% | — | — | — | — |
| 2026-08-27 | 22 | `BTSG` | +7.20% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.73% | -4.81% | — | — | — | — |
| 2026-08-27 | 23 | `CRDL` | +7.00% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.74% | -7.48% | — | — | — | — |
| 2026-08-27 | 24 | `NVRI` | +6.75% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.64% | -3.47% | — | — | — | — |
| 2026-08-27 | 25 | `ZYME` | +6.68% | **HOLD** | no featured long / fade / lane | ⬜ | — | -1.02% | +0.07% | — | — | — | — |
| 2026-08-28 | 1 | `USDE` | +34.95% | **HOLD** | no featured long / fade / lane | ⬜ | — | -13.26% | +3.59% | — | — | — | — |
| 2026-08-28 | 2 | `OKTA` | +28.63% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB — | -3.89% | -3.75% | — | ❌ | ❌ | — |
| 2026-08-28 | 3 | `CRM` | +22.58% | **HOLD** | no featured long / fade / lane | ⬜ | — | +0.74% | +2.40% | — | — | — | — |
| 2026-08-28 | 4 | `CRWD` | +20.50% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.37% | -5.66% | — | — | — | — |
| 2026-08-28 | 5 | `DFDV` | +17.33% | **SELL** | fade: 🚨+heat🔴, first crack | ⬜ | 🚨+heat🔴 —; first crack — | -7.01% | -2.27% | — | ✅ | ✅ | — |
| 2026-08-28 | 6 | `CYPH` | +15.95% | **SELL** | fade: 🚨+heat🔴 | ⬜ | vol+AB —; vol+join🔴 —; 🚨+heat🔴 — | -3.70% | -9.52% | — | ✅ | ✅ | — |
| 2026-08-28 | 7 | `VEEV` | +15.20% | **SELL** | fade: 🚨+heat🔴 | ⬜ | 🚨+heat🔴 — | -2.75% | -1.03% | — | ✅ | ✅ | — |
| 2026-08-28 | 8 | `RPD` | +14.50% | **HOLD** | no featured long / fade / lane | ⬜ | — | -2.15% | -11.04% | — | — | — | — |
| 2026-08-28 | 9 | `ACDC` | +13.85% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.13% | -5.13% | — | — | — | — |
| 2026-08-28 | 10 | `SNPS` | +13.39% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.81% | -10.77% | — | — | — | — |
| 2026-08-28 | 11 | `BRUN` | +13.30% | **HOLD** | no featured long / fade / lane | ⬜ | — | -18.87% | -18.39% | — | — | — | — |
| 2026-08-28 | 12 | `FIG` | +13.28% | **HOLD** | no featured long / fade / lane | ⬜ | — | -6.30% | -11.17% | — | — | — | — |
| 2026-08-28 | 13 | `TH` | +12.99% | **BUY** | setup vol+AB (1d edge +2.76) | ⬜ | vol+AB — | -2.69% | -4.69% | — | ❌ | ❌ | — |
| 2026-08-28 | 14 | `PANW` | +12.83% | **HOLD** | no featured long / fade / lane | ⬜ | — | -3.38% | -5.42% | — | — | — | — |
| 2026-08-28 | 15 | `FWDI` | +12.31% | **HOLD** | no featured long / fade / lane | ⬜ | — | -10.51% | -6.76% | — | — | — | — |
| 2026-08-28 | 16 | `SAIL` | +12.24% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.04% | -5.97% | — | — | — | — |
| 2026-08-28 | 17 | `FROG` | +12.10% | **BUY** | setup 🔵+heat🔴 (1d edge +3.09) | ⬜ | 🔵+heat🔴 —; 🔵 stretch — | -4.36% | -11.78% | — | ❌ | ❌ | — |
| 2026-08-28 | 18 | `TENB` | +11.76% | **HOLD** | no featured long / fade / lane | ⬜ | — | -0.32% | -5.71% | — | — | — | — |
| 2026-08-28 | 19 | `WRAP` | +11.69% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.81% | -7.56% | — | — | — | — |
| 2026-08-28 | 20 | `MSTR` | +11.54% | **HOLD** | no featured long / fade / lane | ⬜ | — | -6.03% | -9.11% | — | — | — | — |
| 2026-08-28 | 21 | `RBRK` | +11.33% | **HOLD** | no featured long / fade / lane | ⬜ | — | -13.61% | -17.40% | — | — | — | — |
| 2026-08-28 | 22 | `CAN` | +11.25% | **HOLD** | no featured long / fade / lane | ⬜ | — | -13.64% | -27.27% | — | — | — | — |
| 2026-08-28 | 23 | `BB` | +11.04% | **HOLD** | no featured long / fade / lane | ⬜ | — | -5.90% | -9.48% | — | — | — | — |
| 2026-08-28 | 24 | `PURR` | +10.99% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.52% | -11.54% | — | — | — | — |
| 2026-08-28 | 25 | `SRPT` | +10.95% | **HOLD** | no featured long / fade / lane | ⬜ | — | -4.12% | -1.20% | — | — | — | — |
| 2026-08-31 | 1 | `TRGP` | +14.90% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; 🔵 stretch — | -11.07% | — | — | — | — | — |
| 2026-08-31 | 2 | `NEOV` | +13.29% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | +14.02% | — | — | ❌ | — | — |
| 2026-08-31 | 3 | `AME` | +11.44% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -10.68% | — | — | ✅ | — | — |
| 2026-08-31 | 4 | `NMRA` | +10.07% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -11.77% | — | — | ✅ | — | — |
| 2026-08-31 | 5 | `CTMX` | +8.72% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 — | -1.15% | — | — | — | — | — |
| 2026-08-31 | 6 | `ELMT` | +8.05% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -8.95% | — | — | ✅ | — | — |
| 2026-08-31 | 7 | `SUJA` | +7.68% | **NO BUY** | hall pass blocked | blocked | vol+gen🔴 —; 🔵+heat🟢 —; vol+join🔴 — | -7.73% | — | — | — | — | — |
| 2026-08-31 | 8 | `SLDB` | +7.47% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; 🔵 stretch — | -7.62% | — | — | — | — | — |
| 2026-08-31 | 9 | `NTNX` | +7.40% | **BUY** | lane=probable; vol+AB, vol+gen🔴 | probable | vol+AB —; vol+gen🔴 —; vol+join🔴 —; jdg🟡 — | -7.05% | — | — | ❌ | — | — |
| 2026-08-31 | 10 | `CYPH` | +6.43% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; vol+AB —; vol+gen🔴 — | -2.75% | — | — | — | — | — |
| 2026-08-31 | 11 | `NAGE` | +6.38% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; 🔵 stretch — | -7.14% | — | — | — | — | — |
| 2026-08-31 | 12 | `AREC` | +6.14% | **NO BUY** | hall pass blocked | blocked | — | -7.84% | — | — | — | — | — |
| 2026-08-31 | 13 | `OHI` | +5.93% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -5.83% | — | — | ✅ | — | — |
| 2026-08-31 | 14 | `BMRN` | +5.89% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; 🔵 stretch — | -4.35% | — | — | — | — | — |
| 2026-08-31 | 15 | `SNAP` | +5.88% | **NO BUY** | hall pass blocked | blocked | — | -3.30% | — | — | — | — | — |
| 2026-08-31 | 16 | `STT` | +5.86% | **NO BUY** | hall pass blocked | blocked | — | -6.49% | — | — | — | — | — |
| 2026-08-31 | 17 | `PURR` | +5.51% | **NO BUY** | hall pass blocked | blocked | vol+AB —; vol+gen🔴 — | -3.35% | — | — | — | — | — |
| 2026-08-31 | 18 | `KRMN` | +5.29% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -12.78% | — | — | ✅ | — | — |
| 2026-08-31 | 19 | `AIV` | +5.10% | **NO BUY** | hall pass blocked | blocked | — | -6.34% | — | — | — | — | — |
| 2026-08-31 | 20 | `TRLV` | +4.99% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -4.19% | — | — | ✅ | — | — |
| 2026-08-31 | 21 | `PTRN` | +4.87% | **NO BUY** | hall pass blocked | blocked | jdg🟡 — | -6.62% | — | — | — | — | — |
| 2026-08-31 | 22 | `HDSN` | +4.79% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | -5.45% | — | — | — | — | — |
| 2026-08-31 | 23 | `ALEC` | +4.70% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 —; 🔵 stretch — | -7.75% | — | — | — | — | — |
| 2026-08-31 | 24 | `RANI` | +4.69% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 — | -2.27% | — | — | — | — | — |
| 2026-08-31 | 25 | `DPRO` | +4.59% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 —; jdg🟡 — | +13.92% | — | — | ❌ | — | — |
| 2026-09-01 | 1 | `GPRO` | +95.13% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 —; jdg🟡 — | -28.07% | — | — | ✅ | — | — |
| 2026-09-01 | 2 | `PRQR` | +11.16% | **BUY** | lane=probable | probable | — | -10.55% | — | — | ❌ | — | — |
| 2026-09-01 | 3 | `FRVO` | +10.86% | **NO BUY** | hall pass blocked | blocked | — | +15.84% | — | — | — | — | — |
| 2026-09-01 | 4 | `FATE` | +9.21% | **NO BUY** | hall pass blocked | blocked | — | -6.90% | — | — | — | — | — |
| 2026-09-01 | 5 | `TII` | +8.62% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -10.79% | — | — | ✅ | — | — |
| 2026-09-01 | 6 | `AVXL` | +8.57% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | -5.59% | — | — | — | — | — |
| 2026-09-01 | 7 | `BMO` | +8.47% | **NO BUY** | hall pass blocked | blocked | — | -8.90% | — | — | — | — | — |
| 2026-09-01 | 8 | `KMX` | +7.14% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -7.17% | — | — | ✅ | — | — |
| 2026-09-01 | 9 | `ZNTL` | +6.12% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | -7.02% | — | — | — | — | — |
| 2026-09-01 | 10 | `IRD` | +5.86% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | +0.00% | — | — | — | — | — |
| 2026-09-01 | 11 | `FOX` | +5.66% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -7.23% | — | — | ✅ | — | — |
| 2026-09-01 | 12 | `BEP` | +5.50% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | -6.26% | — | — | — | — | — |
| 2026-09-01 | 13 | `TEL` | +5.48% | **BUY** | lane=probable | probable | jdg🟡 — | -3.92% | — | — | ❌ | — | — |
| 2026-09-01 | 14 | `STIM` | +5.28% | **NO BUY** | hall pass blocked | blocked | — | -5.02% | — | — | — | — | — |
| 2026-09-01 | 15 | `VLRS` | +5.05% | **NO BUY** | hall pass blocked | blocked | — | -6.99% | — | — | — | — | — |
| 2026-09-01 | 16 | `KBR` | +5.04% | **NO BUY** | hall pass blocked | blocked | — | -6.17% | — | — | — | — | — |
| 2026-09-01 | 17 | `DUOL` | +5.01% | **NO BUY** | hall pass blocked | blocked | jdg🟡 — | +1.91% | — | — | — | — | — |
| 2026-09-01 | 18 | `MRLN` | +4.95% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -11.63% | — | — | ✅ | — | — |
| 2026-09-01 | 19 | `RY` | +4.68% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -4.79% | — | — | ✅ | — | — |
| 2026-09-01 | 20 | `NVS` | +4.62% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | +1.36% | — | — | ❌ | — | — |
| 2026-09-01 | 21 | `SIBN` | +4.60% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | -4.88% | — | — | ✅ | — | — |
| 2026-09-01 | 22 | `ENOV` | +4.56% | **NO BUY** | hall pass blocked | blocked | — | -20.15% | — | — | — | — | — |
| 2026-09-01 | 23 | `HELP` | +4.42% | **NO BUY** | hall pass blocked | blocked | — | -5.54% | — | — | — | — | — |
| 2026-09-01 | 24 | `RLMD` | +4.36% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | -1.98% | — | — | — | — | — |
| 2026-09-01 | 25 | `TTI` | +4.35% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🔴 — | -9.31% | — | — | — | — | — |
| 2026-09-02 | 1 | `GPRO` | +40.38% | **NO BUY** | hall pass blocked | blocked | vol+AB —; vol+gen🔴 —; 🔵+heat🟢 —; vol+join🔴 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 2 | `FRVO` | +28.41% | **NO BUY** | hall pass blocked | blocked | vol+gen🔴 —; 🔵+heat🟢 —; vol+join🔴 — | — | — | — | — | — | — |
| 2026-09-02 | 3 | `CRK` | +11.02% | **SELL** | fade: first crack | probable | first crack — | — | — | — | — | — | — |
| 2026-09-02 | 4 | `MMED` | +10.66% | **NO BUY** | hall pass blocked | blocked | — | — | — | — | — | — | — |
| 2026-09-02 | 5 | `DEFT` | +10.05% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | — | — | — | — | — | — |
| 2026-09-02 | 6 | `ARCT` | +9.93% | **BUY** | lane=probable | probable | — | — | — | — | — | — | — |
| 2026-09-02 | 7 | `MRNA` | +9.93% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 8 | `CTMX` | +9.23% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 9 | `SLN` | +8.94% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 10 | `EIX` | +8.93% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 11 | `CRDL` | +8.86% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 12 | `SID` | +8.65% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 13 | `NVAX` | +8.00% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 14 | `CLYM` | +7.94% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 15 | `CNXC` | +7.24% | **NO BUY** | hall pass blocked | blocked | — | — | — | — | — | — | — |
| 2026-09-02 | 16 | `BMEA` | +7.14% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 17 | `SION` | +7.13% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 18 | `DUOL` | +7.02% | **SELL** | fade: 🚨+heat🔴 | blocked | 🚨+heat🔴 — | — | — | — | — | — | — |
| 2026-09-02 | 19 | `SAFX` | +6.89% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 20 | `SDGR` | +6.87% | **BUY** | lane=probable | probable | — | — | — | — | — | — | — |
| 2026-09-02 | 21 | `GMRS` | +6.75% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 22 | `KLRA` | +6.56% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |
| 2026-09-02 | 23 | `VIR` | +6.31% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 24 | `ALEC` | +6.19% | **BUY** | lane=probable; 🔵+heat🟢 | probable | 🔵+heat🟢 — | — | — | — | — | — | — |
| 2026-09-02 | 25 | `PYXS` | +6.17% | **NO BUY** | hall pass blocked | blocked | 🔵+heat🟢 —; 🔵 stretch — | — | — | — | — | — | — |

_Δ that day is the Finviz Change% that put the name on the gainer list (outcome, not an input). +1d / +3d / +1w are the next-session closes from the lookback price panel._

