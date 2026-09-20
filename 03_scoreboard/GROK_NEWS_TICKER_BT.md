# Grok news → ticker cash-book (research)

Window **2026-08-13 → 2026-09-18** · last closed session **2026-09-18**. Two-stage: harvest automations to frozen files, then replay. Not live. Does not touch factor-mine / flatten / Webull.

## Coverage

- Stage 1 automation days dumped: **0** (0 results).
- Sessions filled from repo files only: **26** of 26.
- `automation_get_results`: **False**. automation_get_results unavailable in this environment (cloud VM / GH Actions cannot see automation run logs)
- GH Actions must not harvest. Replay reads frozen dumps + dated repo news.

## Leak rules

- `known_at` = automation createTime, or Finviz News Time if the title is on that day’s export. Earlier stamp only if that stamp sits on a file dated ≤ the fill session.
- Fill = next official 09:30 **strictly after** known_at. 09:30:00 ET is too late for that open. RTH → next open. Missing official open → no fill.
- Mapper uses D-1 Company / Sector / Industry only. Never same-day Change%/Gap/RelVol. Never “Finviz attached this headline to ticker X”.
- No close digest, no post-close research_baseline, no OOS files while mapping an IS fill.
- Hormuz carry with no new shock is not an article.
- Long-only (no short locate) + hard-red S≤−3 sit. $10k leftover split, Futubull fees, whole shares.

## Headline books vs controls

| Window | n articles | n ticker-days | same-day name hit | `grok_n4_h1` book% | starts YES | $ days | `union_hot_n4_h1` | `flatten_h5` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | 816 | 2907 | 1185/2305 (51%) | -3.79 | 6/26 | 23% | +31.97 | +3.39 |
| 2026-08-13→2026-09-09 | 652 | 2205 | 1090/2152 (51%) | -3.68 | 6/19 | 32% | +14.95 | +10.54 |
| 2026-09-10→2026-09-18 | 164 | 702 | 95/153 (62%) | -0.11 | 0/7 | 0% | +14.80 | -6.47 |

### Variants (full window)

| Recipe | book% | starts YES | $ days | trades |
|---|---:|---:|---:|---:|
| `grok_n4_h1` | -3.79 | 6/26 | 23% | 86 |
| `grok_n4_h2` | +0.55 | — | 35% | 56 |
| `grok_n8_h1` | -4.30 | — | 19% | 174 |
| `grok_n8_h2` | -3.53 | — | 31% | 122 |

## 3 hits

- **SECZ** `bullish` fill 2026-09-03 same-day +8.73 / next-close +13.63 — Three Infrastructure Layers Are Converging Into a Single Investable Thesis for Tokenized Finance
- **HOOD** `bullish` fill 2026-08-19 same-day +3.09 / next-close +2.37 — Robinhood CEO Calls On U.S. To Approve Tokenized Stocks
- **COIN** `bullish` fill 2026-08-26 same-day +0.07 / next-close +4.99 — Coinbase Debuts Tokenized Stocks On Base Network

## 3 misses

- **AIXC** `bullish` fill 2026-08-20 same-day -19.52 / next-close -23.29 — SEC proposes Regulation Crypto Assets
- **ANTA** `bearish` fill 2026-09-01 same-day +14.71 / next-close +9.85 — George Santos Gets Kalshi's First-Ever Lifetime Ban
- **EMAT** `bearish` fill 2026-08-18 same-day +13.66 / next-close +3.83 — StreetWatch: As Washington Puts a 100 Percent Tariff on Foreign Drones, the Magnet Inside Them Comes Into Focus

## Method

1. Stage 1 harvest (Cursor / Grok Bot, not GH Action) dumps each automation result since 2026-08-13 into `data/grok_automations/{date}_{task}.json`.
2. Stage 2 keeps policy / regulator / exemption / ban / tariff / Fed / court / bill rows. Drops earnings PR, SEC filings, Yahoo/Cramer tabloid, single-name FDA approvals, carried Hormuz.
3. Tickers must overlap D-1 company text. Sector-wide only when the article is sector-wide (EPA GHG → coal/gas generators; random software no).
4. Cash book is the factor-mine family: $10k, leftover split, Futubull fees, whole shares, sell first, hard-red sit.
5. `union_hot_n4_h1` / `flatten_h5` full-window numbers are the published cash books. 08-13→09-09 and 09-10→end are running-book splits of that same path (not a live rewrite).

