# Lane one-shot 100

Research board. Each kept row was classified and analysed by Lane.
Gate 0 only rejected junk. Tickers come from the Finviz hit list.

- n_drawn: 400
- n_rejected: 353
- n_kept: 47
- invented_tickers: 0
- finviz_file: data/exports/finviz_2026-09-22.csv
- status: SHORTFALL

## Hop histogram

- lane::mistral::ministral-8b-2512: 140
- lane::openrouter::inclusionai/ling-3.0-flash-fin:free: 1

## Env (redacted)

- present ZHIPU_API_KEY len=49
- present GLM_API_KEY len=49
- present SILICONFLOW_API_KEY len=51
- present OPENROUTER_API_KEY len=73
- present DASHSCOPE_API_KEY len=115
- present DASHSCOPE_BASE_URL len=75
- present QWEN_API_KEY len=115
- present TOKENHUB_API_KEY len=51
- present TENCENT_API_KEY len=51
- present HUNYUAN_API_KEY len=51
- present TOKENHUB_BASE_URL len=35
- present TENCENT_BASE_URL len=35
- present GEMINI_API_KEY len=39
- present GOOGLE_AI_STUDIO_API_KEY len=53
- missing MOONSHOT_API_KEY
- missing MODELSCOPE_API_KEY
- missing MODELSCOPE_API_TOKEN
- missing MODELSCOPE_SDK_TOKEN
- present MISTRAL_API_KEY len=32
- present NVIDIA_NIM_API_KEY len=70
- present NVIDIA_API_KEY len=70
- present POLLINATIONS_API_KEY len=35
- missing GROQ_API_KEY
- missing HF_TOKEN
- missing GITHUB_MODELS_TOKEN
- missing SAMBANOVA_API_KEY
- missing CLOUDFLARE_API_TOKEN
- missing CLOUDFLARE_ACCOUNT_ID
- missing OLLAMA_URL
- present DEEPSEEK_API_KEY len=35
- missing LANE_ALLOW_PAID_DEEPSEEK
- missing LANE_URL

## Gold fixtures

- tsa: FAIL
- buist: FAIL
- tsv: FAIL
- amrx: FAIL
- naion: PASS
- hormuz: REJECTED
- outperforms: REJECTED

## Reject reasons

- lane_linker_missing: 234
- lane_bad_enum: 73
- no_signed_instrument: 19
- reaction_title: 3
- guidance_reaffirm_up:AVGO: 2
- dividend_only: 2
- regime_0_1d:None: 2
- guidance_reaffirm_up:None: 2
- hormuz_weather: 1
- outperforms_competitors: 1
- second_order_without_cite:Neos Investments:substitute: 1
- guidance_reaffirm_up:YQ: 1
- guidance_reaffirm_up:DIBS: 1
- gold_on_fed: 1
- guidance_reaffirm_up:MASS: 1
- guidance_reaffirm_up:SKHY: 1
- guidance_reaffirm_up:IMAX: 1
- q5_regime: 1
- guidance_reaffirm_up:COE: 1
- regime_0_1d:NP: 1
- guidance_reaffirm_up:KRKR: 1
- regime_0_1d:ACIO: 1
- guidance_reaffirm_up:XXII: 1
- regime_0_1d:EUDA: 1

## Rows

### 1. Four paying subscribers filed a class-action antitrust lawsuit, Buist et al. v. Anthropic PBC et al., accusing Anthropic, OpenAI, xAI (SpaceXAI), and Google of an illegal agreement to slow AI
- date: 2026-09-17T09:00:00-04:00
- harvest_source: gold_fixture
- q5: regime_break · event_class: regime_break · sign: None
- questions:
  - Did the physical or legal constraint change, or is this a reprint? — answered (Legal constraint on competitive behavior (antitrust) changed; this is a regime break.)
  - If it is a regime break, which prior book flips sign? — answered (The prior book flips sign for the harm set (Anthropic, OpenAI, xAI, Google) under A_AT_01 and A_AT_02.)
  - If it is weather, why is a 0-1d entity illegal? — blocked (Not applicable; this is a legal constraint, not a physical constraint like weather.)
- finviz hits:
  - GOOG — Alphabet Inc — Internet Content & Information
  - GOOGL — Alphabet Inc — Internet Content & Information
- winners: Meta
- losers: Anthropic, OpenAI, xAI (SpaceXAI), GOOGL
- ACTION: SELL GOOGL, 0-1d, because legal constraint on competitive behavior (antitrust); clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 2. Amneal Announces FDA Approval and Launch of Lanreotide Injection
- date: 2026-09-18T16:01:00-04:00
- harvest_source: gold_fixture
- q5: regime_break · event_class: regime_break · sign: None
- questions:
  - Did the physical or legal constraint change, or is this a reprint? — answered (The FDA approval for generic lanreotide is a **regime break** (new legal constraint: generic drug launch). This is not a reprint or weather event, as it introduces a new competitive dynamic in the market.)
  - If it is a regime break, which prior book flips sign? — answered (The prior book flips sign for **generic drug manufacturers** (e.g., Amneal) in the **specialty/generic drug sector**, as the FDA approval creates a new competitive supply constraint, benefiting buyers (patients) and harming incumbent branded drug sellers.)
  - If it is weather, why is a 0-1d entity illegal? — blocked (N/A (not a weather event).)
  - The lanreotide approval hit at 16:01. Which session can actually trade it, and why is that not the same-day 0-1d tape? — answered ({'session': 'next trading day (Tuesday open)', 'reason': 'The approval timestamp (16:01, after cash close) means the news is **not** reflected in the same-day 0-1d tape. The **clock=monday_open** rule applies, as the event is a **regime break** with a horizon of 1-6m, not a same-day print. Trading i)
- finviz hits:
  - AMRX — Amneal Pharmaceuticals Inc — Drug Manufacturers - Specialty & Generic
- winners: AMRX
- losers: time
- ACTION: BUY AMRX, 0-1d, because physical/legal constraint (FDA approval for generic drug launch); clock=monday_open
- watermark: lane::mistral::ministral-8b-2512

### 3. Class wrap links GLP-1 drugs semaglutide from Novo Nordisk and tirzepatide from Eli Lilly to NAION optic-nerve risk
- date: 2026-09-22T18:00:00-04:00
- harvest_source: gold_fixture
- q5: impulse · event_class: product_harm · sign: None
- questions:
  - Who is in the harm set (cannot operate, sued, or breached)? — answered (Both Novo Nordisk (NVO) and Eli Lilly (LLY) are in the harm set due to lawsuits and FDA warnings related to GLP-1 drugs and NAION risk.)
  - If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list? — blocked (No clear substitute for GLP-1 drugs is listed on the Finviz hit list for this class wrap.)
  - Which listed rival competes and is outside the harm set (unscathed_rival)? — blocked (No unscathed rival competing outside the harm set is identified in the provided data.)
  - Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined. — blocked (No arms dealer selling GLP-1 drugs to both sides (manufacturers and buyers) is identified.)
  - NAION/GLP-1 is a class wrap: which listed sponsors are on the Finviz hit list, and why is the clock not 0-1d? — answered ({'listed_sponsors': [], 'reason_clock_not_0_1d': 'The event is a review article with a medium-term horizon (NAION/GLP-1 class wrap), not a same-day print or immediate regulatory action. The clock is not 0-1d due to the nature of the review and ongoing legal/regulatory developments.'})
- finviz hits:
  - NVO — Novo Nordisk ADR — Drug Manufacturers - General
  - LLY — Lilly(Eli) & Co — Drug Manufacturers - General
- winners: —
- losers: NVO, LLY
- ACTION: AVOID_ADD NVO/LLY, medium, because potential adverse health effects (NAION) linked to GLP-1 drugs; clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 4. $72B Streaming Deal: ETFs to Gain From Netflix's Warner Bros. Takeover
- date: 2025-12-08 08:22:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is a corporate action (MNA) involving control of Warner Bros. Discovery by Netflix, not a sector-specific cash or paper event.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend or sector trade mentioned in the article.)
  - Who is the named issuer on the Finviz hit list? — blocked (No explicit Finviz hit list or named issuer mentioned beyond Warner Bros. Discovery.)
- finviz hits:
  - WBD — Warner Bros. Discovery Inc — Entertainment
  - WMG — Warner Music Group Corp — Entertainment
- winners: —
- losers: WBD
- ACTION: SELL WBD, 0-1d, because merger agreement (Warner Bros. Discovery acquisition by Netflix); clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 5. 'Big Short' Michael Burry Tunes Out AI Woo-Hoos  Finds Opportunity In Copper, QXO And 3 More Stocks
- date: 2026-09-21 22:40:40
- harvest_source: finviz_export
- q5: regime_break · event_class: regime_break · sign: None
- questions:
  - Did the physical or legal constraint change, or is this a reprint? — answered (The legal constraint (potential U.S. tariffs on refined copper) is a new risk shock, not a reprint. The physical constraint (copper supply/demand) remains unchanged.)
  - If it is a regime break, which prior book flips sign? — blocked (No prior book exists for this specific tariff risk in the supplied context.)
  - If it is weather, why is a 0-1d entity illegal? — blocked (Not applicable; this is not a weather-related event.)
- finviz hits:
  - ERO — Ero Copper Corp — Copper
- winners: —
- losers: ERO
- ACTION: SELL ERO, 0-1d, because potential U.S. tariffs on refined copper (legal/physical constraint); clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 6. 1 Number That Makes Nike's Turnaround Worth Watching This Month
- date: 2026-08-18T14:20:00Z
- harvest_source: parsed
- q5: impulse · event_class: guidance · sign: None
- questions:
  - What number changed versus what was already priced? — answered (The article references Nike's stock performance (down >30% year-to-date) and its turnaround strategy ('Win Now'), but no explicit financial metric (e.g., revenue, margins, unit sales) is changed or updated. The focus is on narrative guidance (turnaround progress) rather than a quantifiable print. Th)
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered (This is a factor (turnaround progress) that should not be pinned to a ticker, as it lacks a specific operational or financial print. The article is a narrative-driven analysis, not a data-driven update.)
  - If guidance was only reaffirmed, why is an up call illegal? — answered (Guidance was not reaffirmed; instead, the article discusses a *thematic* turnaround narrative ('Win Now') without a concrete update to prior guidance. The 'up call' is illegal because the article lacks a directional print (e.g., revenue beat/miss) to justify an upward revision.)
- finviz hits:
  - NKE — Nike Inc — Footwear & Accessories
- winners: —
- losers: NKE
- ACTION: AVOID_ADD NKE, 1-4w, because Nike's financial performance metrics or operational turnaround indicators (e.g., revenue, margins, or unit sales); clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 7. 1-800-Flowers.com Inc (FLWS) (Q4 2026) Earnings Call Highlights: Revenue Slump and Cost Cuts as ...
- date: 2026-09-10 21:00:31
- harvest_source: finviz_export
- q5: impulse · event_class: guidance · sign: None
- questions:
  - What number changed versus what was already priced? — answered ({'number_changed': '$10–15M', 'previously_priced': '$27.7M (consensus)'})
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered ({'issuer': True, 'ticker': 'FLWS'})
  - If guidance was only reaffirmed, why is an up call illegal? — answered ({'reason': 'Guidance was not reaffirmed; it was downgraded from $27.7M consensus to $10–15M, which is a material negative revision.'})
- finviz hits:
  - FLWS — 1-800 Flowers.com Inc — Specialty Retail
- winners: —
- losers: FLWS
- ACTION: AVOID_ADD FLWS, 1-4w, because FY27 adjusted EBITDA guidance ($10–15M vs. $27.7M consensus); clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 8. 10 DAYS LEFT: Rackspace Technology (RXT) Investors Should Contact Block & Leviton LLP Before September 28
- date: 2026-09-18 14:57:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer's own cash, paper, or control—it is a **sector story** tied to NVIDIA's AI infrastructure partnership, which competes with RXT's AI infrastructure ambitions. The event is a **corporate action** (MNA) that directly impacts RXT's market position in AI infrastructure, making it a)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - RXT — Rackspace Technology Inc — Software - Infrastructure
- winners: —
- losers: RXT
- ACTION: SELL RXT, 0-1d, because NVIDIA Cloud Partner Program partnership and Blackwell-powered AI infrastructure launch for sovereign enterprises; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 9. 190,044 Shares in CVS Health Corporation $CVS Bought by Wealthfront Advisers LLC
- date: 2026-08-19T13:15:59Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer's own cash, paper, or control—it is an institutional accumulation (Wealthfront Advisers LLC) in a Healthcare firm, not a sector story.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend context provided; irrelevant to insider flow.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is **CVS Health Corporation** (CVS).)
- finviz hits:
  - CVS — CVS Health Corp — Healthcare Plans
  - WLTH — Wealthfront Corp — Software - Application
- winners: —
- losers: CVS
- ACTION: SELL CVS, 0-1d, because institutional accumulation; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 10. 1st Source: Q2 Earnings Snapshot
- date: 2026-07-23 16:37:06
- harvest_source: finviz_export
- q5: impulse · event_class: price_cap · sign: None
- questions:
  - Who collects the old venue rent (exchange or broker)? — blocked (No explicit mention of old venue rent (exchange/broker) collection in the provided context; event_class=price_cap does not apply to exchanges/brokers.)
  - Which new venue just received permission, and is that name on the Finviz hit list? — blocked (No new venue permission or Finviz hit list reference in the title or body.)
  - Is the incumbent already building the same rail (then mixed, not a clean down)? — blocked (No evidence of incumbent building the same rail; context lacks rail-specific details.)
  - Does any hit sit in Energy even though the title never names that firm? Drop it. — blocked (No mention of Energy sector or firms in the title or body.)
- finviz hits:
  - SRCE — 1st Source Corp — Banks - Regional
- winners: SRCE
- losers: —
- ACTION: BUY SRCE, 0-1d, because price target adjustment by Piper Sandler; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 11. 2 Top Cybersecurity Stocks to Buy Amid AI Safety Fears: FTNT, QLYS
- date: 2026-09-14 17:50:00
- harvest_source: finviz_export
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (Institutional investor (CalSTRS) stake increase in QLYS is not a sector story; it reflects confidence in the firm’s cybersecurity capabilities amid AI safety concerns, positioning QLYS as a substitute for FTNT in the cybersecurity space.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - QLYS — Qualys Inc — Software - Infrastructure
- winners: QLYS
- losers: —
- ACTION: BUY QLYS, 0-1d, because institutional investor stake increase; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 12. 20/20 BioLabs Reschedules Investor Webinar to 2:00 p.m. Eastern Time on September 2, 2026 to Discuss OneTest Revenue and Expanding Commercial Opportunity
- date: 2026-08-28 18:26:00
- harvest_source: finviz_export
- q5: impulse · event_class: preannounce · sign: None
- questions:
  - What number changed versus what was already priced? — answered (Q2 2026 revenue increased by ~37% to ~$0.7M, eliminating convertible note debt, which expands commercial opportunity and signals positive momentum for the named issuer (AIDX). This is a named issuer print with a clear directional number change.)
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered (This is a named issuer (20/20 BioLabs/AIDX), not a factor. The revenue print is pinned to the company’s ticker.)
  - If guidance was only reaffirmed, why is an up call illegal? — blocked (Guidance was not reaffirmed; the event is a revenue print with a clear directional change (up). No reaffirmation to invalidate.)
- finviz hits:
  - AIDX — 20/20 Biolabs Inc — Medical Devices
- winners: AIDX
- losers: —
- ACTION: BUY AIDX, 0-1d, because commercial opportunity expansion discussion; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 13. 21shares Announces 1-for-10 Reverse Share Split for 21Shares 2x Long Sui ETF (TXXS)
- date: 2026-07-06 15:54:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (Reverse split reduces outstanding shares by ~90%, increasing per-share NAV and price but diluting liquidity and increasing volatility risk for leveraged crypto ETFs, acting as a scarce input constraint for retail investors.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Not applicable; this is not a routine dividend.)
  - Who is the named issuer on the Finviz hit list? — blocked (No sector trade or named issuer on Finviz hit list provided.)
- finviz hits:
  - TXXS — 21Shares 2x Long Sui ETF — Exchange Traded Fund
- winners: —
- losers: TXXS
- ACTION: SELL TXXS, 0-1d, because corporate action (M&A equivalent: reverse split); clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 14. 23,000 Jobs LOST: Why Wall Street Actually Sees It as GOOD News
- date: 2026-08-10T14:30:00Z
- harvest_source: parsed
- q5: impulse · event_class: labor_stop · sign: None
- questions:
  - Who is in the harm set (cannot operate, sued, or breached)? — answered (The financial services sector (broadly) is in the harm set due to job losses, as employment levels are a constraint. The article implies this is a negative development for the sector's operational capacity.)
  - If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list? — blocked (No substitute applicable; the article does not describe a blocked primary channel or a shift to an alternative end-use.)
  - Which listed rival competes and is outside the harm set (unscathed_rival)? — answered (Clear Street Group Inc (CLRS) and State Street Corp (STT) are unscathed rivals, as they are not explicitly named as losing jobs or being unable to operate.)
  - Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined. — blocked (No arms dealer identified; the article does not mention any entity selling the input (jobs) to both sides of the market.)
- finviz hits:
  - CLRS — Clear Street Group Inc — Capital Markets
  - STT — State Street Corp — Asset Management
- winners: CLRS, STT
- losers: Wall Street financial services sector (broadly, including capital markets and asset management)
- ACTION: BUY CLRS/STT, 0-1d, because employment levels in financial services sector; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 15. 3 Bank Stocks With High Dividends and Earnings Growth
- date: 2026-07-10 08:56:00
- harvest_source: finviz_export
- q5: impulse · event_class: capital_return · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The article references KeyCorp (MTB) as a bank stock with a high dividend yield (3.6%) and earnings growth potential, indicating it is the named issuer's own cash (dividend) and paper (earnings growth) story, not a sector trade.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No sector trade is mentioned in the article; it focuses on individual bank stocks.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is KeyCorp (MTB).)
- finviz hits:
  - MTB — M & T Bank Corp — Banks - Regional
- winners: MTB
- losers: —
- ACTION: BUY MTB, 0-1d, because dividend yield and earnings growth potential; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 16. 3 E Network Announces Closing of $1.0 Million Private Placement with CEO-Affiliated Entity
- date: 2026-09-21 08:30:00
- harvest_source: finviz_export
- q5: impulse · event_class: capital_return · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The $1.0M private placement is not the issuer’s own cash (it is external capital from a CEO-affiliated entity), nor is it a routine dividend or sector trade. The funds are earmarked for AI-focused development, working capital, and hiring, which are strategic investments rather than operational cash )
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Not applicable; this is not a routine dividend.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is **3 E Network Technology Group Ltd (MASK)**.)
- finviz hits:
  - MASK — 3 E Network Technology Group Ltd — Software - Application
- winners: —
- losers: MASK
- ACTION: SELL MASK, 0-1d, because capital_return: 3 E Network Announces Closing of $1.0 Million Private Placement with CEO-Affiliated Entity; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 17. 30% Free Cash Flow Yield Positions Charter Communications (CHTR) for De-leveraging and Shareholder Return
- date: 2026-08-20T12:50:15Z
- harvest_source: parsed
- q5: impulse · event_class: capital_return · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The article references free cash flow yield as a metric for shareholder return, implying a capital return event tied to the issuer's own cash (free cash flow). This is not a sector story, as it focuses on the firm's financial position and de-leveraging strategy.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend is mentioned; the article focuses on free cash flow yield and shareholder returns, not a dividend payout.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is Charter Communications (CHTR).)
- finviz hits:
  - CHTR — Charter Communications Inc — Telecom Services
- winners: —
- losers: CHTR
- ACTION: SELL CHTR, 0-1d, because free cash flow yield; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 18. 34 Miles Out to Sea, Nowhere to Hide from the Sun: Tint World and XPEL Team Up to Protect Historic Frying Pan Shoals Tower
- date: 2026-09-10 08:30:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (New manufacturing ramp costs (constraint) are a direct input cost for XPEL, acting as a headwind to earnings. This is a firm-level constraint, not a sector story.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No dividend or capital return event mentioned.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer is **XPEL Inc** (ticker: XPEL).)
- finviz hits:
  - XPEL — XPEL Inc — Auto Parts
- winners: —
- losers: XPEL
- ACTION: SELL XPEL, 0-1d, because new manufacturing ramp costs; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 19. 374Water wraps up mobile AirSCWO campaign for Minnesota PFAS project
- date: 2026-09-15 10:08:00
- harvest_source: finviz_export
- q5: impulse · event_class: blast_ops · sign: None
- questions:
  - Who is in the harm set (cannot operate, sued, or breached)? — answered (No explicit harm set identified in the article; 374Water completed the contract without operational issues, implying no legal or operational breaches.)
  - If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list? — blocked (No indication of a blocked primary channel or substitute listed in the Finviz hit list.)
  - Which listed rival competes and is outside the harm set (unscathed_rival)? — blocked (No competing unscathed rival mentioned in the article.)
  - Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined. — blocked (No arms dealer or seller of the input mentioned.)
- finviz hits:
  - SCWO — 374Water Inc — Pollution & Treatment Controls
- winners: SCWO
- losers: —
- ACTION: BUY SCWO, 0-1d, because physical/legal constraint on waste destruction operations; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 20. 4 Diversified Chemical Stocks to Gain Amid Stable End-market Demand - Zacks Investment Research
- date: Tue, 18 Aug 2026 12:28:35 GMT
- harvest_source: parsed
- q5: impulse · event_class: demand · sign: None
- questions:
  - Who pays the input, and who sells it? — answered (End-market demand stability implies buyers (e.g., automotive/construction sectors) pay for input chemicals, while sellers (e.g., EMN) supply them.)
  - Is this capacity added or capacity destroyed? — answered (Capacity is neither added nor destroyed; demand rebound and destocking end stabilize supply-demand balance (no net change).)
  - Which listed substitute wins if the primary supply is missing? — blocked (No clear substitute cited in the article for primary chemical supply.)
- finviz hits:
  - EMN — Eastman Chemical Co — Specialty Chemicals
- winners: EMN
- losers: —
- ACTION: BUY EMN, 0-1d, because end-market demand stability; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 21. 4th Circuit dismisses some charges against Wells Fargo after jury’s $22.1M fee
- date: 2026-08-25T15:28:00Z
- harvest_source: parsed
- q5: impulse · event_class: blast_legal · sign: None
- questions:
  - Who is in the harm set (cannot operate, sued, or breached)? — answered (Wells Fargo is in the harm set due to the legal judgment (4th Circuit dismissal of charges) reducing the $22.1M award but still affirming retaliation claim, indicating ongoing legal overhang and operational constraints.)
  - If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list? — blocked (No relevant substitute applies to this legal judgment.)
  - Which listed rival competes and is outside the harm set (unscathed_rival)? — blocked (No unscathed rival identified in this context.)
  - Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined. — blocked (No arms dealer identified in this context.)
- finviz hits:
  - WFC — Wells Fargo & Co — Banks - Diversified
- winners: —
- losers: WFC
- ACTION: SELL WFC, 0-1d, because legal judgment (4th Circuit dismissal of charges); clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 22. 5 Billionaire Money Managers Dumped Palantir in the Second Quarter. This Is the AI Applications Stock They're Buying Hand Over Fist Instead.
- date: 2026-08-25T09:26:00Z
- harvest_source: parsed
- q5: impulse · event_class: flow_mechanical · sign: None
- questions:
  - Who is forced to buy or sell the named line, and on which session? — answered (Billionaire money managers are forced to sell PLTR due to institutional asset allocation shift (q5=impulse), likely driven by relative underperformance or strategic reallocation toward competitors like GOOGL (Alphabet). The sell-off occurs on the session following the 13F filing release (August 14, )
  - Is the flow the issuer itself or an index vehicle? — blocked (The flow is not clearly tied to the issuer (PLTR) as an index vehicle, but rather reflects a direct institutional sell-off of the named line (PLTR) as a standalone asset.)
- finviz hits:
  - PLTR — Palantir Technologies Inc — Software - Infrastructure
- winners: —
- losers: PLTR
- ACTION: SELL PLTR, 0-1d, because institutional asset allocation shift; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 23. 5 Revealing Analyst Questions From BrightSpring Health Services’s Q2 Earnings Call
- date: 2026-08-07T09:23:16Z
- harvest_source: parsed
- q5: impulse · event_class: preannounce · sign: None
- questions:
  - What number changed versus what was already priced? — answered (Revenue ($3.15B vs. $2.99B estimate) and adjusted EPS ($0.22 vs. $0.19 estimate) beat expectations, but the market reacted negatively due to concerns about underlying headwinds (e.g., increased corporate expenses, generic drug impact like Revlimid). The named issuer’s print (revenue/EPS beat) was al)
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered (This is a **factor** (generic drug pressure, corporate expenses) that should not be pinned to a single ticker. The named issuer (BTSG) is the vehicle, but the **direction** is driven by macro headwinds (e.g., Revlimid’s contribution, cost inflation), not just the company’s print.)
  - If guidance was only reaffirmed, why is an up call illegal? — answered (Guidance was not reaffirmed—it was **implicitly challenged** by management’s comments on generic expectations (no changes) and CFO’s attribution of higher corporate expenses. The market’s down call is legal because the **factor_impulse** (costs/headwinds) was not priced in, despite the print beat.)
- finviz hits:
  - BTSG — BrightSpring Health Services Inc — Health Information Services
- winners: —
- losers: BTSG
- ACTION: SELL BTSG, 0-1d, because preannounce: 5 Revealing Analyst Questions From BrightSpring Health Services’s Q2 Earnings Call; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 24. 5 big analyst AI moves: Downgrades for SAP and Intuit; AMD lifted to Strong Buy
- date: 
- harvest_source: finviz_digest
- q5: impulse · event_class: guidance · sign: None
- questions:
  - What number changed versus what was already priced? — answered (Price target slashed from $605 to $331, reflecting downgrade to Neutral and weaker FY27 outlook due to AI-driven competitive pressures (TurboTax/QuickBooks).)
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered (This is a named issuer (Intuit), not a factor.)
  - If guidance was only reaffirmed, why is an up call illegal? — answered (Guidance was not reaffirmed; it was explicitly weakened (FY27 outlook downgraded) with a price target cut, making an up call illegal.)
- finviz hits:
  - INTU — Intuit Inc — Software - Application
- winners: —
- losers: INTU
- ACTION: AVOID_ADD INTU, 1-4w, because guidance: 5 big analyst AI moves: Downgrades for SAP and Intuit; AMD lifted to Strong Buy; clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 25. 7RCC Spot Bitcoin and Carbon Credit Futures ETF (NYSE Arca: BTCK) Begins Trading
- date: 2026-06-04 09:30:00
- harvest_source: finviz_export
- q5: impulse · event_class: listing_flow · sign: None
- questions:
  - Who is forced to buy or sell the named line, and on which session? — answered (Forced buyers are investors seeking exposure to BTCK on day 0, as the ETF listing creates a forced buying flow to acquire the newly listed shares.)
  - Is the flow the issuer itself or an index vehicle? — answered (The flow is an index vehicle (ETF) issued by 7RCC Global, not the issuer itself.)
- finviz hits:
  - BTCK — 7RCC Spot Bitcoin and Carbon Credit Futures ETF — Exchange Traded Fund
- winners: BTCK
- losers: —
- ACTION: BUY BTCK, 0-1d, because listing_flow: 7RCC Spot Bitcoin and Carbon Credit Futures ETF (NYSE Arca: BTCK) Begins Trading; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 26. A Corsair Gaming Director Dumps Nearly 9,000 Shares Representing a Substantial 28% of Their Direct Stake. Here's a Closer Look at the Transaction.
- date: 2026-08-17T12:25:01Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is the issuer's own paper (insider sale of 28% stake by a director), not a sector story.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked
  - Who is the named issuer on the Finviz hit list? — answered (Corsair Gaming Inc (CRSR) is the named issuer.)
- finviz hits:
  - CRSR — Corsair Gaming Inc — Computer Hardware
- winners: —
- losers: CRSR
- ACTION: SELL CRSR, 0-1d, because corporate_insider_sale; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 27. A Griffon Insider Sold Into an Earnings Jump but Kept $80 Million in Stock. Here's What to Know
- date: 2026-08-08T22:59:32Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The insider flow (Robert F. Mehmel, President & COO) sold shares into an earnings-driven price jump, retaining a significant stake (~$80M). This suggests the insider perceived the earnings as a one-time event rather than a sustained catalyst, implying a potential downside reversal in the firm's fund)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No sector trade data provided in the article or pack facts.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is **Griffon Corp (GFF)**.)
- finviz hits:
  - GFF — Griffon Corp — Building Products & Equipment
- winners: —
- losers: GFF
- ACTION: SELL GFF, 0-1d, because legal; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 28. A Griffon Insider Sold Into an Earnings Jump but Kept $80 Million in Stock. Here's What to Know - The Motley Fool
- date: Sat, 08 Aug 2026 22:42:14 GMT
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The insider flow is a sale of shares by a named executive (COO) during an earnings jump, indicating a belief that the stock is overvalued or that the earnings growth is not sustainable. This is not a sector story but a firm-specific event tied to insider sentiment.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend context is provided in the article or pack facts.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is Griffon Corp (GFF).)
- finviz hits:
  - GFF — Griffon Corp — Building Products & Equipment
- winners: —
- losers: GFF
- ACTION: SELL GFF, 0-1d, because stock_holding_disclosure; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 29. A Leading Hedge Fund Just Placed Massive Bets on Broadcom and Intel Stocks. Should You Follow Suit?
- date: 2026-08-26T15:40:00Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (Hedge fund positioning is a sector trade, not issuer-specific cash/paper/control. Both AVGO and INTC are in the same Semiconductors sector, and the hedge fund's bets reflect a directional view on the sector, not individual firm fundamentals.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend or sector trade mentioned in the article.)
  - Who is the named issuer on the Finviz hit list? — blocked (No named issuer explicitly listed in the Finviz context; the article focuses on hedge fund positioning.)
- finviz hits:
  - AVGO — Broadcom Inc — Semiconductors
  - INTC — Intel Corp — Semiconductors
- winners: INTC
- losers: AVGO
- ACTION: BUY INTC, 0-1d, because hedge fund positioning in Broadcom and Intel; clock=0-1d; SELL AVGO, 0-1d, because hedge fund positioning in Broadcom and Intel; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 30. A Live Nation Insider's Latest Transaction With Shares Up 22%: Here's What to Know
- date: 2026-08-08T16:16:19Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer's own cash, paper, or control—it is a non-discretionary sale of shares by an insider (Hopmans) to cover tax withholding from restricted stock vesting. The transaction is a routine insider tax liability, not a sector story or strategic move.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - LYV — Live Nation Entertainment Inc — Entertainment
- winners: —
- losers: LYV
- ACTION: SELL LYV, 0-1d, because insider transaction volume and timing; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 31. A Nearly $40 Million Reason to Buy Little-Known SuperX AI Stock
- date: 2026-08-26T20:18:38Z
- harvest_source: parsed
- q5: impulse · event_class: rumor · sign: None
- questions:
  - Did the physical or legal constraint change, or is this a reprint? — answered (The physical constraint changed with the $40M order for Nvidia B300 server clusters, creating new supply at an oligopoly node (AI server infrastructure) that benefits SUPX as a seller of AI hardware solutions.)
  - If it is a regime break, which prior book flips sign? — blocked (No prior book exists for this regime.)
  - If it is weather, why is a 0-1d entity illegal? — blocked (Not applicable; this is a regime change, not weather.)
- finviz hits:
  - SUPX — SuperX AI Technology Ltd — Software - Infrastructure
- winners: SUPX
- losers: —
- ACTION: BUY SUPX, 0-1d, because rumor: A Nearly $40 Million Reason to Buy Little-Known SuperX AI Stock; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 32. A New Buffett Takes On the Task of Preserving Berkshire Hathaway's Culture
- date: 2026-09-18 20:00:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer’s own cash, paper, or control—it is a governance transition (board composition change) with cultural preservation as the focus, not a sector story.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend or sector trade mentioned.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer is Berkshire Hathaway (BRK-A/BRK-B).)
- finviz hits:
  - BRK-A — Berkshire Hathaway Inc — Insurance - Diversified
  - BRK-B — Berkshire Hathaway Inc — Insurance - Diversified
- winners: —
- losers: BRK-A
- ACTION: SELL BRK-A, 0-1d, because board composition; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 33. A New Defense Stock Is Born: Lyntris IPO Set For Wednesday
- date: 2026-08-19T12:30:37Z
- harvest_source: parsed
- q5: impulse · event_class: listing_flow · sign: None
- questions:
  - Who is forced to buy or sell the named line, and on which session? — answered (Forced buyers (investors) are compelled to buy LYNX shares on the listing session (Wednesday), as evidenced by the IPO pricing and subsequent 14% drop from the list price, indicating a forced entry at a higher price than the market valued it.)
  - Is the flow the issuer itself or an index vehicle? — answered (The flow is the issuer itself (Lyntris Inc.), not an index vehicle.)
- finviz hits:
  - LYNX — Lyntris Inc — Aerospace & Defense
- winners: —
- losers: LYNX
- ACTION: SELL LYNX, 0-1d, because new defense stock IPO; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 34. A New Defense Stock Is Born: Lyntris IPO Set For Wednesday - Investor's Business Daily
- date: Wed, 19 Aug 2026 12:30:00 GMT
- harvest_source: parsed
- q5: impulse · event_class: listing_flow · sign: None
- questions:
  - Who is forced to buy or sell the named line, and on which session? — answered (Forced buyers are the investors participating in the IPO on the listing session (Wednesday). The flow is the issuer itself, Lyntris Inc., as it is an IPO.)
  - Is the flow the issuer itself or an index vehicle? — answered (The flow is the issuer itself, Lyntris Inc., not an index vehicle.)
- finviz hits:
  - LYNX — Lyntris Inc — Aerospace & Defense
- winners: LYNX
- losers: —
- ACTION: BUY LYNX, 0-1d, because listing_flow: A New Defense Stock Is Born: Lyntris IPO Set For Wednesday - Investor's Business Daily; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 35. A Qorvo Executive's Shares Vested Amid the Skyworks Deal. Here's What to Know
- date: 2026-08-08T21:24:10Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The insider flow is unrelated to the firm's own cash, paper, or control—it is a routine tax-driven vesting sale, not a sector story or M&A driver. The event is a substitute for the primary deal-related narrative.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Not applicable; this is not a routine dividend.)
  - Who is the named issuer on the Finviz hit list? — blocked (No explicit 'Finviz hit list' context provided.)
- finviz hits:
  - QRVO — Qorvo Inc — Semiconductors
  - SWKS — Skyworks Solutions Inc — Semiconductors
- winners: —
- losers: QRVO
- ACTION: SELL QRVO, 0-1d, because insider_flow: A Qorvo Executive's Shares Vested Amid the Skyworks Deal. Here's What to Know; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 36. A Qorvo Insider Disposed of Shares as a Merger Looms. Here's What Long-Term Investors Should Know
- date: 2026-08-08T21:18:18Z
- harvest_source: parsed
- q5: impulse · event_class: insider_flow · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The insider disposition is tied to a **merger looming**, indicating the firm's cash/paper/control is at risk of transfer (not a sector story). The insider's sale (4% of holdings) suggests a **downward signal** for the firm's valuation or control.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (No routine dividend mentioned; context is insider flow tied to M&A, not a sector trade.)
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer is **Qorvo Inc (QRVO)**.)
- finviz hits:
  - QRVO — Qorvo Inc — Semiconductors
- winners: —
- losers: QRVO
- ACTION: SELL QRVO, 0-1d, because insider share disposition; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 37. A Sinda drill hole returns 4,137 g/t silver-equivalent at Don Diego - Stock Titan
- date: Fri, 14 Aug 2026 13:18:00 GMT
- harvest_source: parsed
- q5: impulse · event_class: trial_readout · sign: None
- questions:
  - Who just became legal to sell or launch the product? — answered (No new entity became legally authorized to sell or launch the product; the article confirms existing exploration results at Don Diego mine, which are already part of Sinda's permitted operations post-IPO. The constraint (mineral resource grade) is not changing—only verified new data is news (A_Q5_01)
  - Is this a gate (approval) or only a trial readout (not approval)? — answered (This is a trial readout (event_class=trial_readout), not a gate (approval). The WINNER/LOSER TEST gate is not open here.)
  - What clock does the print land on: same session, next open, or Monday? — answered (The print lands on the same session (no delay to next open or Monday).)
- finviz hits:
  - SIND — Sinda Ltd — Other Precious Metals & Mining
- winners: —
- losers: SIND
- ACTION: AVOID_ADD SIND, 1-6m, because mineral resource grade at Don Diego mine; clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 38. A decade of profitability provides foundation for CLINUVEL's U.S. expansion
- date: 2026-08-27 01:42:00
- harvest_source: finviz_export
- q5: impulse · event_class: capital_return · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (The dividend declaration is the issuer’s own cash (constraint: dividend_declaration). The dividend is not a sector trade, as it is a routine capital return (firm family) and not tied to a broader market or industry movement. The cash reserves ($252.1M) and decade-long profitability signal financial )
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - CUVL — Clinuvel Pharmaceuticals Ltd ADR — Biotechnology
- winners: CUVL
- losers: —
- ACTION: BUY CUVL, 0-1d, because dividend_declaration; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 39. ABVC BioPharma, Inc. Announces Planned Spin-Off of BioKey (Cayman), Inc. Through Dividend Distribution of BioKey Shares to ABVC Shareholders
- date: 2026-06-23 08:30:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_spinoff · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer's own cash, paper, or control—it is a **corporate restructuring** (spin-off) and a **capital return** via dividend distribution of BioKey shares, not a routine dividend. The event involves a **sector-specific strategic realignment** (unlocking value of subsidiaries and technol)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - ABVC — ABVC BioPharma Inc — Biotechnology
- winners: —
- losers: ABVC
- ACTION: SELL ABVC, 0-1d, because corporate_action_spinoff: ABVC BioPharma, Inc. Announces Planned Spin-Off of BioKey (Cayman), Inc. Through Dividend Distribution of BioKey Shares to ABVC Shareholders; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 40. ACHR Stock Alert: What to Know as Archer Aviation Makes L.A. Plans Ahead of 2028 Olympics
- date: 2026-08-24T18:52:30Z
- harvest_source: parsed
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (ACHR’s operational commitment to the L.A. Olympics 2028 vertiport partnership (AEG) is a **substitute** for traditional ground transport (e.g., rental cars) under A_AIR_02, as it directly addresses a constraint (mobility) for event attendees. The deal is not a sector story (Aerospace & Defense) but )
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - ACHR — Archer Aviation Inc — Aerospace & Defense
- winners: ACHR
- losers: —
- ACTION: BUY ACHR, 0-1d, because L.A. Olympics 2028 operational commitment; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 41. ADC Therapeutics (ADCT) Navigates FDA Setback While Cutting Its Losses
- date: 2026-08-27T20:42:02Z
- harvest_source: parsed
- q5: regime_break · event_class: regime_break · sign: None
- questions:
  - Did the physical or legal constraint change, or is this a reprint? — answered (The FDA regulatory approval process for ADC Therapeutics' drug candidate (LOTUS-5 trial) represents a change in the physical/legal constraint due to FDA safety concerns, not a reprint.)
  - If it is a regime break, which prior book flips sign? — answered (The prior book flips sign for ADC Therapeutics' approval timeline and safety profile, directly impacting its drug candidate's regulatory path.)
  - If it is weather, why is a 0-1d entity illegal? — blocked (N/A (not applicable to this regime break))
- finviz hits:
  - ADCT — Adc Therapeutics SA — Drug Manufacturers - Specialty & Generic
- winners: —
- losers: ADCT
- ACTION: SELL ADCT, 0-1d, because FDA regulatory approval process for ADC Therapeutics' drug candidate; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 42. ADT Brings ADT Blu to Walmart.com, Expanding Availability Through One of the Nation's Largest Online Retailers
- date: 2026-09-14 09:00:00
- harvest_source: finviz_export
- q5: impulse · event_class: blast_ops · sign: None
- questions:
  - Who is in the harm set (cannot operate, sued, or breached)? — answered (ADT is in the harm set due to the service outage and selloff (4.2% during Monday’s session), indicating operational disruption and financial impact.)
  - If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list? — blocked (No clear indication of a blocked primary channel or buyer substitution in the provided context.)
  - Which listed rival competes and is outside the harm set (unscathed_rival)? — blocked (No clear unscathed rival competing in the DIY security market is identified in the provided context.)
  - Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined. — blocked (No evidence of an arms dealer selling ADT Blu to both sides of the market.)
- finviz hits:
  - ADT — ADT Inc — Security & Protection Services
  - WMT — Walmart Inc — Discount Stores
- winners: —
- losers: ADT
- ACTION: SELL ADT, 0-1d, because blast_ops: ADT Brings ADT Blu to Walmart.com, Expanding Availability Through One of the Nation's Largest Online Retailers; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 43. ADVASA Provides Clarification to Shareholders Regarding Its Direct Listing, Registered Resale Shares, Largest Shareholder Ownership and Management Transition
- date: 2026-09-09 10:38:00
- harvest_source: finviz_export
- q5: impulse · event_class: listing_flow · sign: None
- questions:
  - Who is forced to buy or sell the named line, and on which session? — answered (The direct listing of ADBT (Advasa Holdings Inc) acts as a substitute for incumbent exchanges/brokers (e.g., Nasdaq/NYSE/DTCC) by introducing tokenization rails, which may disrupt their traditional rent collection model (A_TSV_06). The 39% premarket bounce suggests forced buyers (investors) are reac)
  - Is the flow the issuer itself or an index vehicle? — answered (The flow is the issuer itself (Advasa Holdings Inc), not an index vehicle, as the article explicitly references the company's direct listing and shareholder clarifications.)
- finviz hits:
  - ADBT — Advasa Holdings Inc — Software - Application
- winners: ADBT
- losers: —
- ACTION: BUY ADBT, 0-1d, because direct listing; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 44. AECOM’s (ACM) Record Backlog Collides With A Costly Charge
- date: 2026-08-20T12:48:35Z
- harvest_source: parsed
- q5: impulse · event_class: input_cost · sign: None
- questions:
  - Who pays the input, and who sells it? — answered (AECOM (ACM) pays the input cost (construction project charge), while AECOM sells the engineering/construction services to its clients (buyers).)
  - Is this capacity added or capacity destroyed? — answered (This is **capacity destroyed** due to the costly charge (delayed project completion) reducing AECOM’s operational capacity.)
  - Which listed substitute wins if the primary supply is missing? — answered (Substitutes (e.g., alternative construction firms like Bechtel or Fluor) win if AECOM’s supply is missing, per **A_AIR_02** (competitors gain market share).)
- finviz hits:
  - ACM — AECOM — Engineering & Construction
- winners: AECOM's buyers (clients)
- losers: ACM
- ACTION: SELL ACM, 0-1d, because costly charge; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 45. AGNT: Q2 Earnings Snapshot
- date: 2026-08-04 17:01:27
- harvest_source: finviz_export
- q5: impulse · event_class: guidance · sign: None
- questions:
  - What number changed versus what was already priced? — answered (Q2 revenue printed at **$1.4B** (up 11% YoY), beating guidance of **$1.35B–$1.45B**, but **adjusted EBITDA guidance was tightened to $17–$22M** (vs. implied $30.7M in Q2 actual). The **factor impulse** (revenue beat) was offset by **guidance downgrade on EBITDA**, signaling macro caution (e.g., lega)
  - Is this one named issuer, or a factor that should not be pinned to a camera ticker? — answered (This is a **named issuer (AGNT)**—not a factor—since the print tied **specific guidance (EBITDA) to a ticker (AGNT) in the lede**. The **$1.4B revenue** is a book print, while the **EBITDA guidance cut** is a factor impulse (macro-driven, not inventory).)
  - If guidance was only reaffirmed, why is an up call illegal? — answered (An **up call is illegal** because the **EBITDA guidance downgrade** (a **downward factor impulse**) contradicts the **revenue beat** (upward book print). The **mixed direction** (revenue up, EBITDA guidance down) violates the **print_vs_priced** rule for this family.)
- finviz hits:
  - AGNT — AGNT Inc — Real Estate Services
- winners: —
- losers: AGNT
- ACTION: AVOID_ADD AGNT, 1-4w, because guidance: AGNT: Q2 Earnings Snapshot; clock=not_0_1d
- watermark: lane::mistral::ministral-8b-2512

### 46. AI Infrastructure Stocks: What CoreWeave's Debt Reveals - MarketWise
- date: Thu, 27 Aug 2026 14:04:30 GMT
- harvest_source: parsed
- q5: impulse · event_class: credit_funding · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is not the issuer's own cash or control—it is a **credit/funding risk signal** (GPU-backed debt) tied to AI infrastructure, not a sector story. The $2.6B loan reveals lender skepticism about AI buildout viability, signaling **liquidity constraints** rather than operational cash flow.)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked (Lane returned no answer for this question)
  - Who is the named issuer on the Finviz hit list? — blocked (Lane returned no answer for this question)
- finviz hits:
  - CRWV — CoreWeave Inc — Software - Infrastructure
  - MKTW — Marketwise Inc — Financial Data & Stock Exchanges
- winners: —
- losers: CRWV
- ACTION: SELL CRWV, 0-1d, because CoreWeave's debt disclosure introduces a credit/funding risk signal; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512

### 47. AIOS Tech Inc. Announces 20-for-1 Share Consolidation and Increase of Authorized Share Capital
- date: 2026-04-21 17:00:00
- harvest_source: finviz_export
- q5: impulse · event_class: corporate_action_mna · sign: None
- questions:
  - Is this the issuer's own cash, paper, or control — not a sector story? — answered (This is a routine corporate action (share consolidation) that reduces the share count and increases authorized capital, which can be seen as a strategic move to simplify share ownership and potentially improve liquidity. The surge in intraday price suggests market anticipation of improved shareholde)
  - If it is a routine dividend, why is there no 0-1d sector trade? — blocked
  - Who is the named issuer on the Finviz hit list? — answered (The named issuer on the Finviz hit list is AIOS Tech Inc.)
- finviz hits:
  - AIOS — AIOS Tech Inc — Information Technology Services
- winners: AIOS
- losers: —
- ACTION: BUY AIOS, 0-1d, because share consolidation and increase of authorized share capital; clock=0-1d
- watermark: lane::mistral::ministral-8b-2512
