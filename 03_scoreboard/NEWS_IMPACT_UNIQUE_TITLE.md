# News-impact unique-title book

Deterministic router on **every unique title** (all sources, all dates). Not the 09-17 slice. Not only the old 94 taped rows. No Lane. Research-only.

## Scoreboard

- **grok_automations**: files=157 items=157 status=used (was 0). Gmail `noreply@x.ai` dump 2026-08-13..2026-09-21; `body_complete=true` on 20/157 only.
- **unique n**: 29446
- **graded n** (has tape, 0-1d graded row): 1277
- **signed listed**: 1495  **no tape**: 218
- **converge** groups: 60  0-1d 35/50 = 70.0%
- **singleton** groups: 8497  0-1d 647/1225 = 52.8%

No fake five-digit graded n. Graded n is tape that exists.

## Window

- earliest on disk: 2026-04-26. earliest parse: 2026-08-08. latest: 2026-09-21.
- June 2026 parse present: False.

## Honest funnel (after grok_automations wire)

- raw headlines (before title dedupe): **333289**
- unique after title dedupe: **29446**
- non-reaction / non-weather: **8771**
- impulse + up/down + listed expression: **1538**
- has tape (graded 0-1d row): **1277**

has_tape_graded counts unique articles with at least one graded 0-1d tape row. If this is only hundreds, that is the truth — the five-digit target needs unused/empty sources (theme-radar headline export, RSS/Supabase dumps, June parses) to fill.

## Graded rates (hygiene + horizon + listed + tape)

- 0-1d: 122/237 = 51.5%
- 1-4w: 250/521 = 48.0%
- guidance slice 0-1d: n/a
- guidance slice 1-4w: 63/132 = 47.7%
- macro headline-level basket 0-1d (not legs): 6/8 = 75.0% (stories=40, reprints_collapsed=15)

Reaction kill, FOMC/macro collapse, and class-horizon skip are the #306 / #307 rules. Missing tape is **not** a miss.

## Sources

- **parsed_json** `01_daily/news/*_parsed.json` — files=31 2026-08-08..2026-09-21 status=used
- **finviz_export** `data/exports/finviz_YYYY-MM-DD.csv (News Title + Daily Digest)` — files=35 2026-04-26..2026-09-21 status=used
- **finviz_digest** `01_daily/news/*finviz*digest*.json` — files=62 2026-07-30..2026-09-21 status=used
- **events_json** `01_daily/events/*_events.json` — files=29 2026-08-10..2026-09-21 status=used
- **actions_keep** `01_daily/news/*_actions.json (KEEP / conditional evidence)` — files=33 2026-08-09..2026-09-21 status=used
- **grok_automations** `data/grok_automations/{date}_{slug}.json` — files=157 items=157 2026-08-13..2026-09-21 status=used
  - GH Actions tokens cannot call the Automations API. Ingest via bot/Cursor (Gmail noreply@x.ai or automation_get_results) then commit dumps. See docs/GROK_AUTOMATIONS_HARVEST.md.
- **rss_dumps** `collectors/rss_news.py (workflow exists; no dump dir on disk)` — files=0 .. status=empty
- **supabase_dumps** `src/db.py news pooler (no local dump on disk)` — files=0 .. status=empty
- **theme_radar_snapshots** `https://github.com/SRoyaltyy/theme-radar data/snapshots/*.csv` — files=30 2026-08-06..2026-09-18 status=unused_readonly
  - Theme Radar: please export a headline pack (date, ticker, News Title, News Time, Daily Digest, News URL) from data/snapshots/*.csv into a JSON/CSV we can ingest in fullscan. Do not merge the repos.

## Daily refresh (Grok Automations)

GH Actions tokens cannot call the Automations API. Ingest via bot/Cursor Gmail harvest (`noreply@x.ai`) then commit dumps. `automation_get_results` was unavailable; Gmail previews are truncated (`body_complete=true` on 20/157). Actions must not call live Automations. Stub: `python3 scripts/ingest_grok_automations.py`. See `docs/GROK_AUTOMATIONS_HARVEST.md` and `data/grok_automations/MANIFEST.md`.

## No tape (218 signed listed rows)

These would be gradeable (impulse + up/down + listed expression) but have no OHLC. Listed, not graded. Tape was not invented.

| # | Title | Tickers | Source | Note |
| ---: | --- | --- | --- | --- |
| 1 | Holtec Nuclear IPO (HNUC) pricing window | HNUC:up | `parsed` | no tape |
| 2 | Holtec Nuclear IPO (HNUC) pricing/listing | HNUC:up | `parsed` | no tape |
| 3 | Holtec Nuclear IPO (HNUC) | HNUC:up | `parsed` | no tape |
| 4 | Aptiv PLC stock outperforms competitors on strong trading day | PLC:down | `finviz_export` | no tape |
| 5 | James Hardie Industries PLC (JHIUF) (Q1 2027) Earnings Call Highlights: Record Growth and ... | JHIUF:up | `finviz_export` | no tape |
| 6 | Honda deepens relationship with quantum computing firm | ADR:up | `finviz_export` | no tape |
| 7 | $1.1B Loan on HPP, Blackstone LA Studios Hits Special Servicing | FFO:up | `finviz_export` | no tape |
| 8 | Are MKTX, SMTI, FBRX, LXFR Obtaining Fair Deals for their Shareholders? | PLC:up | `finviz_export` | no tape |
| 9 | Pangaea Logistics Solutions, Ltd. Q2 2026 Earnings Call Summary | TCE:up | `finviz_export` | no tape |
| 10 | Pearson Interim Results for the six months to 30th June 2026 (Unaudited) | ADR:up | `finviz_export` | no tape |
| 11 | Republic Bancorp: Q2 Earnings Snapshot | KY:up | `finviz_export` | no tape |
| 12 | Rentokil Initial PLC (RKLIF) (H1 2026) Earnings Call Highlights: Revenue Growth and Margin ... | RKLIF:down | `finviz_export` | no tape |
| 13 | Runway Growth Finance Corp. Q2 2026 Earnings Call Summary | NII:up | `finviz_export` | no tape |
| 14 | SI-BONE, Inc. Q2 2026 Earnings Call Summary | BONE:up | `finviz_export` | no tape |
| 15 | Smurfit Westrock PLC stock outperforms competitors on strong trading day | PLC:down | `finviz_export` | no tape |
| 16 | Mobilicom Ltd (MOBBW) (Q2 2026) Earnings Call Highlights: Record Defense Revenue and Strategic ... | MOBBW:up | `finviz_export` | no tape |
| 17 | Japan's economy manages 1.1% growth rate despite headwinds | ADR:up | `finviz_export` | no tape |
| 18 | Equatorial SA (EQUEY) (Q2 2026) Earnings Call Highlights: Strategic Expansion and Resilient ... | EQUEY:down | `finviz_export` | no tape |
| 19 | What did c-store CEOs make in 2025? Heres what the filings show. | FEMSA:down | `finviz_export` | no tape |
| 20 | Stantec announces amendment to Normal Course Issuer Bid | NCIB:up | `finviz_export` | no tape |
| 21 | Full Truck Alliance Tops Forecasts as Transaction Revenue Accelerates | ADR:up | `finviz_export` | no tape |
| 22 | Cytokinetics Announces Upcoming Presentations at the European Society of Cardiology (ESC) Congress 2026 | ESC:up | `finviz_export` | no tape |
| 23 | Woodside Energy Group Ltd (WOPEF) (H1 2026) Earnings Call Highlights: Record Free Cash Flow ... | WOPEF:down | `finviz_export` | no tape |
| 24 | Mexico's Primero raises $12 million seed to bring AI to Latin American blue-chips | FEMSA:down | `finviz_export` | no tape |
| 25 | ISG Announces 2026 ISG Paragon Awards Asia Winners | ISG:up | `finviz_export` | no tape |
| 26 | AI Advances Analytics Software from Reporting to Action, ISG says | ISG:up | `finviz_export` | no tape |
| 27 | Russian strike kills 27 near Kyiv as Moscow militarizes schools | DARC:down | `finviz_export` | no tape |
| 28 | SI-BONE To Present at Morgan Stanley 24th Global Healthcare Conference on September 14, 2026 | BONE:up | `finviz_export` | no tape |
| 29 | AI Helps Integrated Tools Take on Security Threats, ISG Says | ISG:up | `finviz_export` | no tape |
| 30 | Cboe Global Markets Reports Trading Volume for August 2026 | XSP:up | `finviz_export` | no tape |
| 31 | ISG Announces Finalists for 2026 ISG Women in Digital Awards | ISG:up | `finviz_export` | no tape |
| 32 | Learning Platforms Gain Flexibility to Meet Changing Skill Requirements, Employee Expectations, ISG says | ISG:up | `finviz_export` | no tape |
| 33 | Alcoa Corporation Announces Pricing of Debt Offering to Finance Cash Consideration for Acquisition of South32s Bauxite, Alumina and Aluminum Assets | AA:down | `finviz_export` | no tape |
| 34 | Avino Earns Second Consecutive TSX30(TM) Recognition | TM:down | `finviz_export` | no tape |
| 35 | Caleres Inc (CAL) (Q2 2026) Earnings Call Highlights: Strategic Growth and Margin Expansion ... | CAL:up | `finviz_export` | no tape |
| 36 | Casey's General Stores Inc (CASY) (Q1 2027) Earnings Call Highlights: EPS Soars 28% to $7. ... | CASY:up | `finviz_export` | no tape |
| 37 | CBRE IM Buys Net Lease REIT For $1.6B, Plans More Investment | CBRE:up | `finviz_export` | no tape |
| 38 | Core & Main Inc (CNM) (Q2 2026) Earnings Call Highlights: Record Buybacks and Strategic ... | CNM:up | `finviz_export` | no tape |
| 39 | EVI Industries Inc (EVI) (Q4 2026) Earnings Call Highlights: Record Revenue and Strategic ... | EVI:up | `finviz_export` | no tape |
| 40 | Shareholders who lost money in shares of Flotek Industries, Inc. (NYSE: FTK) should contact Wolf Haldenstein Immediately | ICE:down | `finviz_export` | no tape |
| 41 | NYSE Texas Office Opening: Globe Life Co-CEOs Cover Lone Star State Importance | ICE:up | `finviz_export` | no tape |
| 42 | Moonwalk Biosciences Announces $70 Million Series B: NYSE Content Update | ICE:up | `finviz_export` | no tape |
| 43 | Trust Stamp offers free onboarding for financial institutions amid IDScan data breach | IDAI:down | `finviz_export` | no tape |
| 44 | Ingram Micro Holding Corporation Announces Pricing of Secondary Offering of Common Stock by its Principal Stockholder and a Concurrent Stock Repurchase | INGM:down | `finviz_export` | no tape |
| 45 | Korn Ferry (KFY) (Q1 2027) Earnings Call Highlights: Record Growth Fueled by AMS Acquisition ... | KFY:up | `finviz_export` | no tape |
| 46 | Manchester United plc Announces Fourth Quarter Fiscal 2026 Earnings Report Date | MANU:down | `finviz_export` | no tape |
| 47 | Kaplan Fox Urges Medline Inc. (MDLN) Investors to Contact the Firm About Possible Securities Law Violations | MDLN:down | `finviz_export` | no tape |
| 48 | MIND Technology Inc (MIND) (Q2 2027) Earnings Call Highlights: Navigating Geopolitical ... | MIND:down | `finviz_export` | no tape |
| 49 | Amazon Is Selling Its First Sterling Bonds in Four-Part Deal | AMZN:up | `finviz_export` | no tape |
| 50 | Perma-Pipe International Holdings Inc (PPIH) (Q2 2026) Earnings Call Highlights: Record Sales ... | PPIH:up | `finviz_export` | no tape |
| 51 | Pearson and Illinois Tech to Explore Employability Pathways and Workforce-Focused Learning Credentials | ADR:up | `finviz_export` | no tape |
| 52 | Research Solutions Inc (RSSS) (Q4 2026) Earnings Call Highlights: Record Gross Margins and ... | RSSS:up | `finviz_export` | no tape |
| 53 | Schrodinger Announces Licensing and Collaboration Agreement with Tectora Therapeutics, a New Biotechnology Company It Co-founded to Advance Immunology and Inflammation Programs | WBD:up | `finviz_export` | no tape |
| 54 | SFL Corp Ltd's Dividend Analysis | CAR:up | `finviz_export` | no tape |
| 55 | SS Innovations International(SSII) Turning India's Surgical Robotics Opportunity Into a Global Growth Story | SSII:up | `finviz_export` | no tape |
| 56 | Smurfit Westrock PLC stock underperforms Wednesday when compared to competitors | PLC:down | `finviz_export` | no tape |
| 57 | UDR to Participate in Upcoming Real Estate Conferences | UDR:up | `finviz_export` | no tape |
| 58 | United Rentals to Present at the Morgan Stanley 14th Annual Laguna Conference | URI:up | `finviz_export` | no tape |
| 59 | Americas Gold and Silver Recognized as a Top Performer in the 2026 TSX30(TM) | TM:down | `finviz_export` | no tape |
| 60 | Waterdrop Inc (WDH) (Q2 2026) Earnings Call Highlights: Revenue Jumps 72. ... | WDH:down | `finviz_export` | no tape |
| 61 | Robbins LLP is Investigating Allegations that XTI Aerospace, Inc. Misled Investors Regarding the Effectiveness of its Disclosure Controls and Procedures | XTIA:down | `finviz_export` | no tape |
| 62 | Cutting Canada tariffs alone won't flatten US aluminum premium, Alcoa says | AA:down | `finviz_export` | no tape |
| 63 | Is Microsoft Stock Overvalued At 28x Earnings? | MSFT:down | `finviz_export` | no tape |
| 64 | Cboe Global Markets to Present at the Barclays Global Financial Services Conference on September 16 | XSP:up | `finviz_export` | no tape |
| 65 | Office Market Splits as Prime Space Pulls Further Ahead | CBRE:up | `finviz_export` | no tape |
| 66 | Frequency Electronics Inc (FEIM) (Q1 2027) Earnings Call Highlights: Record Revenue, Surging ... | FEIM:up | `finviz_export` | no tape |
| 67 | Oxxo USA taps Gaskins to lead marketing and category management | FEMSA:down | `finviz_export` | no tape |
| 68 | Earnings To Watch: Hooker Furnishings Corp (HOFT) Q2 2027 -- GF Value Sees 7% Upside | HOFT:up | `finviz_export` | no tape |
| 69 | Miss these deadlines and IRS penalties start stacking up | HRB:up | `finviz_export` | no tape |
| 70 | Intercontinental Exchange Sept. Mortgage Monitor- Property Insurance Costs Rise 8.7% Annually | ICE:up | `finviz_export` | no tape |
| 71 | AI Expands Strategic Role of Contact Centers, ISG Says | ISG:up | `finviz_export` | no tape |
| 72 | Imperial Petroleum Inc (IMPP) (Q2 2026) Earnings Call Highlights: Record Revenue, Debt-Free ... | IMPP:up | `finviz_export` | no tape |
| 73 | IRSA Inversiones y Representaciones SA (IRS) (Q4 2026) Earnings Call Highlights: Record Rental ... | IRS:up | `finviz_export` | no tape |
| 74 | ITT Announces Participation at D.A. Davidsons 25th Annual Diversified Industrials & Services Conference on Sept. 24 | ITT:up | `finviz_export` | no tape |
| 75 | Lakeland Industries Inc (LAKE) Q2 2027 Earnings Call Highlights: Fire Segment Momentum and ... | LAKE:down | `finviz_export` | no tape |
| 76 | The Lovesac Co (LOVE) (Q2 2027) Earnings Call Highlights: Record Non-Q4 Sales and Tariff ... | LOVE:up | `finviz_export` | no tape |
| 77 | Lesaka Technologies Inc (LSAK) (Q4 2026) Earnings Call Highlights: First GAAP Profit Since 2022 ... | LSAK:up | `finviz_export` | no tape |
| 78 | Tsakos Energy Navigation Ltd (TEN) (Q2 2026) Earnings Call Highlights: Record Net Income Surges ... | TEN:up | `finviz_export` | no tape |
| 79 | Bear of the Day: Tyson Foods (TSN) | TSN:down | `finviz_export` | no tape |
| 80 | Vince Holding Corp (VNCE) Q2 2026 Earnings Call Highlights: Sales Surge 11. ... | VNCE:up | `finviz_export` | no tape |
| 81 | 34 Miles Out to Sea, Nowhere to Hide from the Sun: Tint World and XPEL Team Up to Protect Historic Frying Pan Shoals Tower | XPEL:up | `finviz_export` | no tape |
| 82 | Zumiez Inc (ZUMZ) (Q2 2026) Earnings Call Highlights: US Footwear Weakness Drives Guidance Cut ... | ZUMZ:down | `finviz_export` | no tape |
| 83 | AudioEye (AEYE) Crossed Above the 200-Day Moving Average: What That Means for Investors | AEYE:up | `finviz_export` | no tape |
| 84 | Goldman, BofA vie to manage Anthropic employee wealth post-IPO - report | BAC:up | `finviz_export` | no tape |
| 85 | Is Concrete Pumping Holdings (BBCP) Stock Undervalued Right Now? | BBCP:up | `finviz_export` | no tape |
| 86 | Why Rising Bond Yields Arent Throttling The Economy, For Now | BCS:up | `finviz_export` | no tape |
| 87 | Kaplan Fox Encourages Capricor Therapeutics, Inc. (CAPR) Investors Who Suffered Losses to Seek a Leadership Role Before September 28, 2026 | CAPR:down, BLA:down | `finviz_export` | no tape |
| 88 | CBRE Sees AI Office Demand Strengthening Prime Space | CBRE:up | `finviz_export` | no tape |
| 89 | Is ChargePoint (CHPT) Outperforming Other Auto-Tires-Trucks Stocks This Year? | CHPT:up | `finviz_export` | no tape |
| 90 | Should Value Investors Buy CNO Financial Group (CNO) Stock? | CNO:up | `finviz_export` | no tape |
| 91 | Mecka AI nears $500M valuation in Sequoia-led deal amid rush for robot training data | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 92 | Is Salesforce Stock Cheap Because Software Is Dying? | CRM:up | `finviz_export` | no tape |
| 93 | IBEX Ltd (IBEX) (Q4 2026) Earnings Call Highlights: Record Revenue and AI-Driven Growth Momentum | IBEX:up | `finviz_export` | no tape |
| 94 | NYSE Honors Victims of 9/11: NYSE Content Update | ICE:up | `finviz_export` | no tape |
| 95 | ISG Announces 2026 ISG Women in Digital Awards Winners for the Americas | ISG:up | `finviz_export` | no tape |
| 96 | Jabil Inc. stock underperforms Friday when compared to competitors despite daily gains | APH:up | `finviz_export` | no tape |
| 97 | 3 Reasons Growth Investors Will Love Jones Lang LaSalle (JLL) | JLL:up | `finviz_export` | no tape |
| 98 | Wall Street Analysts Think M/I Homes (MHO) Is a Good Investment: Is It? | MHO:up | `finviz_export` | no tape |
| 99 | Micron Takes Unexpected Step as Tensions Rise | MU:down | `finviz_export` | no tape |
| 100 | Kaplan Fox Encourages Microvast Holdings, Inc. (MVST) Investors Who Suffered Losses to Seek a Leadership Role Before September 21, 2026 | MVST:up | `finviz_export` | no tape |
| 101 | RKLB Stock: What's Behind The 39% Drop? | DARC:down | `finviz_export` | no tape |
| 102 | Kaplan Fox Urges Primoris Services Corporation (PRIM) Investors Seeking Recovery to Contact the Firm Before September 21, 2026 | PRIM:down | `finviz_export` | no tape |
| 103 | Rent the Runway Inc (RENT) (Q2 2026) Earnings Call Highlights: Record Revenue and New CEO Amid ... | RENT:up | `finviz_export` | no tape |
| 104 | Research Solutions Inc (RSSS) (Q3 2026) Earnings Call Highlights: Margin Expansion and AI ... | RSSS:up | `finviz_export` | no tape |
| 105 | Can Snowflake Stock Keep Climbing On The Work Its AI Tools Bring In? | SNOW:up | `finviz_export` | no tape |
| 106 | Kaplan Fox Encourages Unicycive Therapeutics, Inc. (UNCY) Investors with Significant Losses to Contact the Firm Before November 2, 2026 | UNCY:up | `finviz_export` | no tape |
| 107 | Greystone Logistics Inc (GLGI) (Q4 2026) Earnings Call Highlights: Navigating the Loss of a ... | GLGI:down | `finviz_export` | no tape |
| 108 | Burnham faces UK economic test as budget day approaches | BCS:up | `finviz_export` | no tape |
| 109 | Premium Offices Pull Ahead as Owners Rethink Workplaces | CBRE:up | `finviz_export` | no tape |
| 110 | MasterCraft Boat Holdings Inc (MCFT) Q4 2026 Earnings Call Highlights: Transformational Marine ... | MCFT:down | `finviz_export` | no tape |
| 111 | Going Beyond Apple's Presentation: Interesting Tidbits and Observations From Apple's 'Surprise and Shine' Event (Part 1) | GOOGL:down, META:up | `finviz_export` | no tape |
| 112 | Review & Preview: AI Is At It Again | ANF:up | `finviz_export` | no tape |
| 113 | HealthStream Defines a New Enterprise Category Alongside the EHR and ERP, the Enterprise Clinical Workforce Platform (ECWP) | ECWP:down | `finviz_export` | no tape |
| 114 | AI Slowdown Debate Splits Tech CEOs: Nvidia, Broadcom Dismiss Any Threat While Elon Musk Calls For Peer Review System | NVDA:down | `finviz_export` | no tape |
| 115 | Broadridge Expands Next-gen Digital Assets Capabilities to U.S. Wealth Management Firms | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 116 | Kaplan Fox Urges Avis Budget Group, Inc. (CAR) Investors Seeking Recovery to Contact the Firm Before September 29, 2026 | CAR:down | `finviz_export` | no tape |
| 117 | NYLIM CBRE Global Infrastructure Megatrends Term Fund (NYSE: MEGI) Declares Monthly Distributions for September, October, and November 2026 and Availability of 19(a) Notice | ICE:up, CBRE:up | `finviz_export` | no tape |
| 118 | Coda Octopus Group Inc (CODA) (Q3 2026) Earnings Call Highlights: Defense Surge Offsets Marine ... | CODA:up | `finviz_export` | no tape |
| 119 | BNY Mellon Strategic Municipal Bond Fund, Inc.'s Dividend Analysis | DSM:up | `finviz_export` | no tape |
| 120 | Kaplan Fox Urges Datavault AI Inc. (DVLT) Investors Seeking Recovery to Contact the Firm Before October 5, 2026 | DVLT:down | `finviz_export` | no tape |
| 121 | BNY Mellon Strategic Municipals, Inc.'s Dividend Analysis | LEO:up | `finviz_export` | no tape |
| 122 | D-Wave Quantum (QBTS) Finalizes Definitive Agreement With U.S. Department of Commerce for Up to $100M to Accelerate U.S. Leadership in Quantum Computing | QBTS:down | `finviz_export` | no tape |
| 123 | RF Industries Ltd (RFIL) (Q3 2026) Earnings Call Highlights: Record Revenue and Expanding ... | RFIL:up | `finviz_export` | no tape |
| 124 | Radiant Logistics Inc (RLGT) (Q4 2026) Earnings Call Highlights: Record Quarterly Revenue and ... | RLGT:up | `finviz_export` | no tape |
| 125 | RLJ Lodging Trust Announces Third Quarter 2026 Earnings Release and Conference Call Dates | RLJ:up | `finviz_export` | no tape |
| 126 | Did You Pay For Palantir's Guidance Or For Something Else? | SNOW:up | `finviz_export` | no tape |
| 127 | Oprah Winfrey to Host 'Immersive' Live Show at Las Vegas Sphere | SPHR:up | `finviz_export` | no tape |
| 128 | Solidion Technology Announces Stock Buyback Program | STI:up | `finviz_export` | no tape |
| 129 | Kaplan Fox Encourages UWM Holdings Corporation (UWMC) Investors Who Suffered Losses to Seek a Leadership Role Before October 13, 2026 | UWMC:up | `finviz_export` | no tape |
| 130 | Apple reportedly accepts Samsung's 1Q27 memory hike, raising the 2027 smartphone cost floor | GOOGL:down, META:up | `finviz_export` | no tape |
| 131 | 3 Defensive-Styled Stocks to Help Weather Volatility | ADM:up | `finviz_export` | no tape |
| 132 | Robinhood To Offer Share Redemptions And Voting Rights On Tokenized Stocks | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 133 | AMN Healthcare Survey Reveals Critical Divide Between Healthcare Leaders and Frontline Workers | AMN:up | `finviz_export` | no tape |
| 134 | DIAMONDROCK HOSPITALITY ANNOUNCES THIRD QUARTER 2026 EARNINGS RELEASE AND CONFERENCE CALL | DRH:down | `finviz_export` | no tape |
| 135 | This new tax law could save your business money | HRB:up | `finviz_export` | no tape |
| 136 | MindWalk Holdings Corp (HYFT) (Q1 2027) Earnings Call Highlights: Revenue Climbs 21% as ReefIQ ... | HYFT:down | `finviz_export` | no tape |
| 137 | Installed Building Products Inc's Dividend Analysis | IBP:down | `finviz_export` | no tape |
| 138 | Perk Brings Its Spend Platform to U.S.: NYSE Content Update | ICE:up | `finviz_export` | no tape |
| 139 | AI is Delivering Measurable Value Across Healthcare Revenue Cycle, ISG Expert Says | ISG:up | `finviz_export` | no tape |
| 140 | Kestra Medical Technologies Ltd (KMTS) (Q1 2027) Earnings Call Highlights: Revenue Jumps 60% as ... | KMTS:up | `finviz_export` | no tape |
| 141 | Paramount Skydance Corp's Dividend Analysis | WBD:down | `finviz_export` | no tape |
| 142 | Palantir wins higher UBS target as customer demand outpaces capacity | SNOW:up | `finviz_export` | no tape |
| 143 | WRD 3.0 Powers the AION i60 with Championship-Winning Technology Available from Delivery | MU:down | `finviz_export` | no tape |
| 144 | American Airlines CEO says shift to premium continues | AAL:down, DAL:down, UAL:down, LUV:down, ALK:down, XLE:up | `finviz_export` | no tape |
| 145 | iPhone 18 Pro preorders rise up to 20% in Taiwan as foldable Duo debuts | GOOGL:down, META:up | `finviz_export` | no tape |
| 146 | American Savings Bank Raises $129 Million in IPO: NYSE Content Update | ICE:up | `finviz_export` | no tape |
| 147 | Nestlé, PepsiCo join forces on paper packaging push | DVN:down | `finviz_export` | no tape |
| 148 | XOM Keeps Climbing. Should You Climb On? | DVN:down | `finviz_export` | no tape |
| 149 | High Tide Inc (HITI) (Q3 2026) Earnings Call Highlights: Record $198. ... | HITI:up | `finviz_export` | no tape |
| 150 | Kaplan Fox Urges Hyliion Holdings Corp. (HYLN) Investors Seeking Recovery to Contact the Firm Before October 27, 2026 | HYLN:up | `finviz_export` | no tape |
| 151 | Data Management Builds Framework for AI to Safely Scale, ISG says | ISG:up | `finviz_export` | no tape |
| 152 | IonQ, ORNL, NVIDIA, and the University of Tennessee, Knoxville Show AI Method Reduces Quantum Optimization Trade Off | NVDA:up | `finviz_export` | no tape |
| 153 | Live Nation Makes Show Day Easier for Fans With Salesforce's Agentforce | CRM:down | `finviz_export` | no tape |
| 154 | Kaplan Fox Reminds PROCEPT BioRobotics Corporation (PRCT) Investors That They Have Until September 22, 2026 to Move for Lead Plaintiff | PRCT:down | `finviz_export` | no tape |
| 155 | Rent the Runway, Inc. Q2 2026 Earnings Call Summary | RENT:down | `finviz_export` | no tape |
| 156 | Why Is Palantir Stock Priced For A Swing This Wide After A Flat Year? | SNOW:up | `finviz_export` | no tape |
| 157 | Kaplan Fox Urges ARS Pharmaceuticals Inc. (SPRY) Investors Seeking Recovery to Contact the Firm Before October 5, 2026 | SPRY:down | `finviz_export` | no tape |
| 158 | Trip.com Group Ltd (TCOM) (Q2 2026) Earnings Call Highlights: International OTA Revenue Jumps ... | TCOM:up | `finviz_export` | no tape |
| 159 | TELUS Digital Named as a Leader in Everest Group Customer Experience Management (CXM) Services PEAK Matrix Assessment - Americas for the Eighth Consecutive Year | CXM:down | `finviz_export` | no tape |
| 160 | Airlines and Cruise Stocks Face New Oil Shock | AAL:down, DAL:down, UAL:down, LUV:down, ALK:down, XLE:up | `finviz_export` | no tape |
| 161 | Robinhood Rises as the SEC Clears Tokenized Stocks | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 162 | American Savings Bank Jumps More Than 6% in Trading Debut: NYSE Content Update | ICE:up | `finviz_export` | no tape |
| 163 | Baidu, Inc. Sued for Securities Law Violations - Contact the DJS Law Group to Discuss Your Rights BIDU | BIDU:down | `finviz_export` | no tape |
| 164 | Securitize Surges After SEC Clears Path for Tokenized Stock Trading | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 165 | Accelerate Completes Second Infrastructure Asset-Backed Securitization Amid Strong Institutional Demand | CBRE:up | `finviz_export` | no tape |
| 166 | 51 Talk Online Education Group (COE) (Q2 2026) Earnings Call Highlights: Gross Billings Surge ... | COE:up | `finviz_export` | no tape |
| 167 | Polymarket Hires Coinbase's Failed Social-Coin Architect | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 168 | Charles River Associates (CRA) to Present at Upcoming Investor Conferences | CRA:up | `finviz_export` | no tape |
| 169 | Evolution Petroleum Corp (EPM) (Q4 2026) Earnings Call Highlights: Revenue Jumps 20% as Permian ... | EPM:up | `finviz_export` | no tape |
| 170 | Kaplan Fox Encourages FuelCell Energy, Inc. (FCEL) Investors with Significant Losses to Contact the Firm Before November 10, 2026 | FCEL:down | `finviz_export` | no tape |
| 171 | SEC Sends Strong Signal to Robinhood, Coinbase Investors | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 172 | SEC Allows Tokenized Stocks After Clarity Act Falters | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 173 | U.S. Firms Expand ESM for Consistent, AI-Enabled Operations | ISG:up | `finviz_export` | no tape |
| 174 | Ispire Technology Inc (ISPR) (Q4 2026) Earnings Call Highlights: Malaysia Licenses Set Stage ... | ISPR:down | `finviz_export` | no tape |
| 175 | Lennar Corp (LEN) (Q3 2026) Earnings Call Highlights: Record Cycle Times and Margin Gains Amid ... | LEN:up | `finviz_export` | no tape |
| 176 | Ethos Technologies Highlights Why Key Sell Rules Also Apply To New IPOs | LIFE:up | `finviz_export` | no tape |
| 177 | Major airlines cut flights as higher jet fuel prices hit carriers | AAL:down, DAL:down, UAL:down, LUV:down, ALK:down, XLE:up | `finviz_export` | no tape |
| 178 | Kaplan Fox Encourages PROCEPT BioRobotics Corporation (PRCT) Investors With Substantial Losses to Contact the Firm Before September 22, 2026 | PRCT:down | `finviz_export` | no tape |
| 179 | IBM Matches a $1 Billion CHIPS Award For Quantum Foundry | IBM:down | `finviz_export` | no tape |
| 180 | Shell Warns 36 Million Lost LNG Tons Are Exhausting Market Buffers | DVN:down | `finviz_export` | no tape |
| 181 | AAL, UAL, DAL Get Fresh Rothschild Targets  Firm Sees Domestic Revenue Strength Extending Into 2027 | AAL:down, DAL:down, UAL:down, LUV:down, ALK:down, XLE:up | `finviz_export` | no tape |
| 182 | 3 Strong Buy Undervalued Stocks Right Now | ADM:up | `finviz_export` | no tape |
| 183 | SEC Greenlights Tokenized Stocks After Clarity Act Fails in Senate | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 184 | Google Delays Thompson Center Opening to 2028 in Chicago | CBRE:up | `finviz_export` | no tape |
| 185 | Eni divests 10% stake in Côte dIvoires Baleine oilfield to SOCAR | E:up | `finviz_export` | no tape |
| 186 | FuelCell (FCEL) Scrutinized Over Fit Energy Disclosures Driving Stock Down 15% -- HBSS | FCEL:down | `finviz_export` | no tape |
| 187 | Strategy, Circle, and Coinbase Lead Crypto Stock Rally on Tokenized Stock Approval | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 188 | Investing.coms stocks of the week | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 189 | The SEC Greenlights Tokenized Stocks... With Caveats | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 190 | U.K. Public Sector Adopts Platform-Based Delivery Approach | ISG:up | `finviz_export` | no tape |
| 191 | Using A Limit Order To Buy Occidental Petroleum At A Discount | DVN:down | `finviz_export` | no tape |
| 192 | Republic Bancorp Inc's Dividend Analysis | KY:up | `finviz_export` | no tape |
| 193 | RH UNVEILS RH ESTATES, THE GALLERY ON GREENWICH AVENUE - THE FIRST FREESTANDING RH ESTATES GALLERY IN THE WORLD | RH:up | `finviz_export` | no tape |
| 194 | Is Palantir's Lead Over Its Peers Already Priced In? | SNOW:up | `finviz_export` | no tape |
| 195 | Like a rabbi buying a church: Delta once bought an entire oil refinery because it got tired of guessing at fuel prices | DVN:down | `finviz_export` | no tape |
| 196 | A sudden end to the Iran war would strike a blow against oil prices and energy stocks. Yet company insiders are buying. | KMI:down | `finviz_export` | no tape |
| 197 | Robinhood CEO Says Civil Liability May Not Be Enough To Contain Biggest AI Risks | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 198 | China'sÂ CXMT says new memory-chip platform enters mass production | MU:down | `finviz_export` | no tape |
| 199 | T. Rowe Price's Head Of Digital Assets Says Tokenized Stocks Must Be 'Instantly Fungible,' Agency Rules Can Move Without CLARITY Act | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 200 | Visa Moves to Close Meme Coin Credit Card Rewards Loophole | COIN:down | `finviz_export` | no tape |
| 201 | Could IBRX Be A Tokenized Stock? Founders Tease Sends ImmunityBio Traders Into Speculation Mode | COIN:up, CRCL:up, SCHW:down, SECZ:up | `finviz_export` | no tape |
| 202 | LG Display launches 24.5-inch 720Hz Gaming OLED Panel | MU:down | `finviz_export` | no tape |
| 203 | Paramount, Warner Bros jump on report of settlement progress in merger fight | WBD:down | `finviz_export` | no tape |
| 204 | Modi pitches India as global chipmaking hub | ASML:up | `finviz_digest` | no tape |
| 205 | Utility bills are rising faster than inflation, BofA says. Expect higher prices long-term. | BAC:down | `finviz_digest` | no tape |
| 206 | IBMs Anderon finalises $1bn agreement for quantum foundry R&D | IBM:up | `finviz_digest` | no tape |
| 207 | SpaceX (SPCX) staggered lockup tranches | SPCX:up | `events` | no tape |
| 208 | Japan Q2 GDP grows 1.1% annualized, missing estimates | GDP:down | `events` | no tape |
| 209 | SpaceX (SPCX) day-90 lockup tranche | SPCX:up | `events` | no tape |
| 210 | Micron Taiwan union strike preparations | MU:down | `events` | no tape |
| 211 | Holtec Nuclear IPO (HNUC) — ~$825M nuclear listing | HNUC:up | `events` | no tape |
| 212 | Electra Therapeutics IPO (ETRA) | ETRA:up | `events` | no tape |
| 213 | Micron FY Q4 earnings (HBM/AI memory) | ASML:up | `events` | no tape |
| 214 | Electra Therapeutics (ETRA) IPO begins trading | ETRA:up | `events` | no tape |
| 215 | SEC 5-year tokenized-securities 'innovation exemption' | COIN:up, CRCL:up, SCHW:down, SECZ:up | `events` | no tape |
| 216 | SEC 5-year 'innovation exemption' for tokenized NMS stocks | COIN:up, CRCL:up, SCHW:down, SECZ:up | `events` | no tape |
| 217 | Electra Therapeutics (ETRA) IPO listed | ETRA:up | `events` | no tape |
| 218 | Fed hike, oil dip, tariff delay | COIN:up, CRCL:up, SCHW:down, SECZ:up | `grok_automations` | no tape |

## Theme Radar ask (do not edit that repo)

Theme Radar: please export a headline pack (date, ticker, News Title, News Time, Daily Digest, News URL) from data/snapshots/*.csv into a JSON/CSV we can ingest in fullscan. Do not merge the repos.

