# Jev book backtest

Every unique title, Jev hop-0/1/2 code path, Finviz-listed sides only. Clock is **published_at** (next RTH), not parse time. X / Y = n_bull / n_bear on one ticker in one entry session. Z = trading sessions after that entry open.

## Scoreboard

- unique titles: **40020** (raw=697983)
- hop-0 keep / drop: **34890** / **5130**
- booked (impulse + listed up/down): **582** votes=1871
- named tape 0-1d: 160/315 = 50.8%
- named tape 1-4w: 323/671 = 48.1%

## Per-vote hit (article × ticker, publication clock)

- named: 0-1d 734/1502 = 48.9%; 2d 653/1405 = 46.5%; 3d 648/1359 = 47.7%; 4d 594/1331 = 44.6%; 5d 593/1293 = 45.9%; 1-4w 335/699 = 47.9%
- substitute / peer: 0-1d 55/112 = 49.1%; 2d 52/99 = 52.5%; 3d 53/98 = 54.1%; 4d 46/98 = 46.9%; 5d 44/95 = 46.3%; 1-4w 22/68 = 32.4%
- all booked: 0-1d 789/1614 = 48.9%; 2d 705/1504 = 46.9%; 3d 701/1457 = 48.1%; 4d 640/1429 = 44.8%; 5d 637/1388 = 45.9%; 1-4w 357/767 = 46.5%

## Ledger X / Y (same ticker, same entry session)

- name groups: 1741  singleton=1639  converge=94  clash=8
- singleton: 0-1d 171/351 = 48.7%; 2d 166/325 = 51.1%; 3d 155/313 = 49.5%; 4d 144/312 = 46.2%; 5d 147/304 = 48.4%; 1-4w n/a
- converge (X≥2, Y=0 or Y≥2, X=0): 0-1d 21/40 = 52.5%; 2d 20/38 = 52.6%; 3d 21/38 = 55.3%; 4d 17/38 = 44.7%; 5d 18/38 = 47.4%; 1-4w n/a
- clash (X≥1 and Y≥1, net side): 0-1d n/a; 2d n/a; 3d n/a; 4d n/a; 5d n/a; 1-4w n/a

One entity per session. n_bull / n_bear count impulse up/down stories after title-normalize. Regime and weather do not vote. clash = both sides on that entity that session. converge = two or more stories, one side. Hit rates grade the entity once: converge uses that side, clash uses the net side (net 0 is ungraded). Long-horizon classes stay out of the 0-1d..5d denominator.

## Window

- earliest on disk: 2026-04-26. earliest parse: 2026-08-08. latest: 2026-09-21.

X = n_bull, Y = n_bear on one ticker in one entry session. Z is trading sessions after the next RTH open after published_at. No invented ticker. No Lane. keep.json unwired. Parquet tape only.

## Top converge (X vs Y)

| date | ticker | X up | Y down | net | 0-1d | 5d | titles |
|---|---|---:|---:|---:|---:|---:|---|
| 2026-08-11 | BLK | 6 | 0 | 6 | 1.61 | 2.1 | Kaplan Fox & Kilsheimer LLP Alerts Embecta Corp. (NASDAQ: EMBC) Investors to a Securities Class Action Deadline on Augus |
| 2026-08-11 | ITIC | 0 | 6 | -6 | -0.84 | 1.89 | Kaplan Fox & Kilsheimer LLP Alerts Embecta Corp. (NASDAQ: EMBC) Investors to a Securities Class Action Deadline on Augus |
| 2026-08-11 | GROW | 0 | 5 | -5 | 0.65 | 6.47 | Kaplan Fox & Kilsheimer LLP Alerts Embecta Corp. (NASDAQ: EMBC) Investors to a Securities Class Action Deadline on Augus |
| 2026-07-30 | FSS | 0 | 4 | -4 | 3.06 | 6.09 | US Federal Reserve holds rates steady as inflation hawks call for hike - CNA; Federal Reserve holds rates steady; three  |
| 2026-08-07 | Q | 4 | 0 | 4 | 1.13 | 4.28 | Ocugen Inc (OCGN) (Q2 2026) Earnings Call Highlights: FDA Clearance for OCU410 Phase 3 Trial ...; Relmada Therapeutics I |
| 2026-08-24 | CPHI | 0 | 3 | -3 | -5.0 | -11.0 | Tesla Recalls Nearly 3 Million Cars in China for Door Handles, Will Introduce Software Changes - Not a Tesla App; Tesla  |
| 2026-08-24 | TSLA | 0 | 3 | -3 | -3.45 | 1.81 | Tesla Recalls Nearly 3 Million Cars in China for Door Handles, Will Introduce Software Changes - Not a Tesla App; Tesla  |
| 2026-08-13 | SAP | 3 | 0 | 3 | 2.37 | 6.19 | Black Rock Coffee Bar, Inc. Sued for Securities Law Violations - Contact the DJS Law Group to Discuss Your Rights - BRCB |
| 2026-09-28 | MIRM | 3 | 0 | 3 | -2.27 | None | Mirum Pharmaceuticals and Incyte Announce U.S. FDA Approval of Atebrioz (zilurgisertib) for Adult and Pediatric Patients |
| 2026-07-30 | NWS | 0 | 3 | -3 | -1.33 | 0.68 | Fed Holds Rates Steady, but 3 Members Favored a Rate Hike / National News / U.S. News - U.S. News & World Report; Federa |
| 2026-08-24 | YUMC | 0 | 3 | -3 | 1.2 | -7.72 | Tesla Recalls Nearly 3 Million Cars in China for Door Handles, Will Introduce Software Changes - Not a Tesla App; Tesla  |
| 2026-08-26 | YUMC | 0 | 3 | -3 | -1.15 | -6.24 | China flexes its auto-safety chops with huge recall of Tesla, other cars - Reuters; Why Tesla has been caught up in a ma |
| 2026-09-28 | MSFT | 3 | 0 | 3 | 0.75 | None | Beta Bionics, Inc. Sued for Securities Law Violations; Investors Who Lost Money Should Contact Block & Leviton LLP; Rack |
| 2026-07-30 | NWSA | 0 | 3 | -3 | -0.46 | 5.14 | Fed Holds Rates Steady, but 3 Members Favored a Rate Hike / National News / U.S. News - U.S. News & World Report; Federa |
| 2026-08-11 | FNF | 3 | 0 | 3 | -0.42 | 1.47 | CAPR Investor Announcement: Capricor Therapeutics Sued on behalf of Investors to Recover Losses after Clinical Data Lead |
| 2026-08-26 | TSLA | 0 | 3 | -3 | 0.16 | 3.4 | China flexes its auto-safety chops with huge recall of Tesla, other cars - Reuters; Why Tesla has been caught up in a ma |
| 2026-09-24 | MLSS | 3 | 0 | 3 | None | None | Mineralys Therapeutics' Transform-HTN Open-Label Extension Trial Data Selected for Late-Breaking Presentation at America |
| 2026-08-10 | ADVB | 2 | 0 | 2 | 26.97 | 32.1 | 3D Systems Receives $9 Million Contract Award from U.S. Air Force for Advanced Large-Format Metal 3D Printing System; La |
| 2026-08-11 | PLUG | 2 | 0 | 2 | -10.48 | -12.9 | Earnings live updates: Plug Power stock jumps on improving margins, raised revenue guidance; Plug Power beats revenue es |
| 2026-08-26 | MLSS | 0 | 2 | -2 | 8.0 | -8.0 | Medical device maker Boston Scientific is being hit by a cyberattack. The shares are falling - CNBC; Boston Scientific h |

## Hop-0 reasons (top)

- code_leftover: 33049
- junk_shape: 2761
- punct: 2190
- code_deal: 1087
- code_policy: 544
- source: 98
- code_oil: 90
- reprint_weather: 81
- code_print: 66
- code_choke: 34
- code_fed: 12
- code_ai: 6
- code_home: 2

## Hop-1 classes on keeps (top)

- discard: 31131
- regime_state: 901
- print_vs_priced: 612
- corporate_action_mna: 605
- capital_return: 346
- trial_readout: 286
- dilution: 172
- listing_flow: 167
- guidance: 117
- market_structure: 86
- gate: 85
- blast_legal: 80
- access_control: 42
- product_harm: 41
- factor_impulse: 38
- integrity: 36
- labor_stop: 34
- activist_campaign: 27
- blast_cyber: 17
- peer_spill: 16

Vs the last Lane unique-title book (`NEWS_IMPACT_UNIQUE_TITLE.md`):
that book had unique=29446, signed listed=1495, converge 0-1d **35/50 = 70.0%**.
This Jev run has more unique titles (theme-radar on disk) but **no edge**:
named 0-1d 48.9%, converge 0-1d 21/40 = 52.5%. Top converge is still
polluted by law-firm alerts and Fed reprints attaching the wrong listed
names. Hop-2 sides only Finviz hits — it does not yet rank *relevance*.

No Lane. No invented ticker. `keep.json` unwired.

