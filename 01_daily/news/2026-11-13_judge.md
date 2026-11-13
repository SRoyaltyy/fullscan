# News Judge — 2026-11-13

### IMPORTANT NEWS (my ranking)
1. **Nvidia CEO Jensen Huang exits/ousted; shares -10%; interim CEO named; $3B China AI projects imperiled** — largest AI/SPX leadership shock in the set.
2. **Hot inflation strengthens Fed hike bets; Goolsbee calls report “borderline disturbing”; Goldman adds hike; Treasury yields rise** — one hawkish rates/risk cluster.
3. **CoreWeave beat and backlog revive AI-infrastructure demand; semis and Bloom Energy lift; Cisco results boost AI sentiment** — offsets Nvidia governance shock at sector level.
4. **Applied Materials record FQ4 beat and above-consensus Q1 outlook on AI chip demand** — confirms semis-equipment AI demand.
5. **Cisco FQ1 beat and FY27 guidance raise** — AI networking/enterprise demand signal.
6. **US existing home sales slump to more than 16-year low** — noise-sample rescue; rate drag on housing/consumer.
7. **JPMorgan cautious on AI-linked industrials; Eaton Neutral, GE Vernova PT cut** — caps AI-industrial sentiment.
8. **Hancock Whitney strikes ~$3.2B deal for Trustmark** — regional-bank consolidation context.

### STEP 1 — FRAMEWORK SCORE
- **Nvidia CEO exit / -10% / China AI projects** — keep; us_relevance high; channel risk; geography global_priced; severity regime; horizon 1d-1w; object spx:SPX beta + semis basket; polarity mixed; why: shares down but retail buy and peer AI bids offset; conf 0.90.
- **Hot inflation / hike bets / yields up / Goolsbee / Goldman** — keep; high; rates; us_domestic; regime; 1d-1w; object spx:SPX beta/rate-sensitive sectors; hawkish; why: raises discount-rate and Fed-path risk; conf 0.90.
- **CoreWeave beat/backlog + semis/Bloom + Cisco AI sentiment** — keep; high; sector_fundamental; us_supply_chain; session; 1d-1w; object sector_etf:SMH/AI-infra basket; bullish; why: revives AI capex demand trade; conf 0.86.
- **Applied Materials record FQ4 beat / Q1 outlook** — keep; high; sector_fundamental; us_supply_chain; session; 1d-1w; object sector_etf:SMH; bullish; why: semis equipment confirmation; conf 0.80.
- **Cisco FQ1 beat / FY27 raise** — keep; medium-high; sector_fundamental; us_domestic; session; 1d-1w; object sector_etf:IGV/XLK networking; bullish; why: AI networking demand read-through; conf 0.76.
- **Existing home sales 16-year low** — conditional; medium; rates; us_domestic; session; 1d-1w; object spx:rate-sensitive housing/consumer; bearish; why: confirms rate drag, second-tier macro; conf 0.65.
- **JPMorgan cautious on AI-linked industrials** — conditional; medium; sentiment; us_domestic; session; 1w; object sector_etf:XLI/AI industrials basket; bearish; why: analyst caution caps AI-industrial multiple; conf 0.62.
- **Hancock Whitney/Trustmark deal** — conditional; medium; sector_policy; us_domestic; noise; 1w; object basket:regional banks/KRE; context; why: consolidation signal, not broad risk driver; conf 0.55.

### STEP 2 — INTERACTIONS
- **Hawkish rates cluster + AI-infra bullish:** do not buy software/high-multiple AI on AI-demand hope alone; rates cap multiples.
- **Nvidia governance shock + CoreWeave/AMAT/Cisco AI beats:** semis/AI infra mixed; do not double-count AI demand against Nvidia CEO/China risk.
- **Hot inflation/yields up + housing slump:** treat as one rates cluster; housing weakness is confirmation, not a separate dovish signal.
- **JPMorgan AI-industrial caution + AI power demand:** caps XLI/AI-industrial basket even with AI-infra bid.

### STEP 3 — RECLASSIFY AUDIT
- **Rescued from single_name:** Nvidia CEO exit/interim CEO/$3B China AI projects — sector/macro force, not pure single-name color.
- **Rescued from noise:** Existing home sales 16-year low; Cerebras AI demand forecast raise; Nebius capacity-delay cut.
- **Dropped from usable:** Powell topic_fed false-positive cluster (Lake Powell, Powell Electronics, Powell’s Picks/Steamer, Norman/Dwight Powell, Jensen Huang/Kimberly Powell affair, Powell Industries); UK GDP/Sterling; BHP iron-ore overhaul; Fed enforcement action; stable jobless claims as low incremental signal.

### STEP 4 — B1 / SECTOR INJECT
NEWS_JUDGE: n=8 rescued=4
MACRO rates: [hawkish] Hot inflation + Goolsbee/Goldman hike odds lift yields (regime/1d-1w)
MACRO risk: [bearish] Nvidia CEO exit and -10% drop pressure AI leadership/SPX beta (session/1d-1w)
SECTOR semis/AI-infra: [bullish] CoreWeave backlog + Cisco/AMAT beats revive AI demand (SMH/IGV)
SECTOR industrials: [bearish] JPMorgan cautious on AI-linked industrials caps ETN/GEV (XLI)
SECTOR banks: [context] Hancock Whitney/Trustmark deal signals regional-bank consolidation (KRE)
MACRO housing: [bearish] Existing home sales 16-year low confirms rate drag (session)
SECTOR AI-infra: [mixed] Cerebras raise vs Nebius capacity cut (AI basket)
INTERACTION: Hawkish rates caps software multiples; AI-infra bullish vs Nvidia governance bearish = semis mixed; housing weakness is rates confirmation.
WATCH: Nvidia CEO-exit headline risk can dominate sector fundamentals intraday.

RULES_APPLIED: none — no pending scheduled high-impact macro binary, no fresh kinetic oil shock, no same-day Fed appearance at/before open, no missing-predict artifact trigger.

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Nvidia CEO Jensen Huang exit and -10% share drop, interim CEO, $3B China AI projects imperiled | keep=keep | channel=risk | severity=regime | horizon=1d-1w | object=spx:SPX beta and semis basket | pol=mixed | conf=0.90
- Hot inflation strengthens Fed hike bets; Goolsbee borderline disturbing; Goldman adds hike; yields rise | keep=keep | channel=rates | severity=regime | horizon=1d-1w | object=spx:SPX beta/rate-sensitive sectors | pol=hawkish | conf=0.90
- CoreWeave beat and backlog revive AI-infrastructure demand; semis/Bloom lift; Cisco boosts AI sentiment | keep=keep | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:SMH/AI-infra basket | pol=bullish | conf=0.86
- Applied Materials record FQ4 beat and above Q1 outlook on AI chip demand | keep=keep | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:SMH | pol=bullish | conf=0.80
- Cisco FQ1 EPS/revenue beat and FY27 guidance raise | keep=keep | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:IGV/XLK networking | pol=bullish | conf=0.76
- Existing home sales slump to more than 16-year low | keep=conditional | channel=rates | severity=session | horizon=1d-1w | object=spx:rate-sensitive housing/consumer | pol=bearish | conf=0.65
- JPMorgan cautious on AI-linked industrials; Eaton Neutral, GE Vernova PT cut | keep=conditional | channel=sentiment | severity=session | horizon=1w | object=sector_etf:XLI/AI industrials basket | pol=bearish | conf=0.62
- Hancock Whitney strikes ~$3.2B deal for Trustmark | keep=conditional | channel=sector_policy | severity=noise | horizon=1w | object=basket:regional banks/KRE | pol=context | conf=0.55
INTERACTIONS: hawkish rates + AI-infra bullish caps software multiples; Nvidia governance shock + CoreWeave/AMAT/Cisco beats = semis/AI mixed, do not double-count AI demand; hot inflation/yields + housing slump = one rates cluster; JPMorgan AI-industrial caution caps XLI despite AI power demand
RESCUED_FROM_NOISE: Nvidia CEO exit/interim CEO/$3B China AI projects (single_name); Existing home sales 16-year low (noise); Cerebras AI demand forecast raise (noise); Nebius capacity-delay cut (noise)
DROPPED_FROM_USABLE: Powell topic_fed false-positive cluster; UK GDP/Sterling; BHP iron-ore overhaul; Fed enforcement action; stable jobless claims
B1_INJECT:
NEWS_JUDGE: n=8 rescued=4
MACRO rates: [hawkish] Hot inflation + Goolsbee/Goldman hike odds lift yields (regime/1d-1w)
MACRO risk: [bearish] Nvidia CEO exit and -10% drop pressure AI leadership/SPX beta (session/1d-1w)
SECTOR semis/AI-infra: [bullish] CoreWeave backlog + Cisco/AMAT beats revive AI demand (SMH/IGV)
SECTOR industrials: [bearish] JPMorgan cautious on AI-linked industrials caps ETN/GEV (XLI)
SECTOR banks: [context] Hancock Whitney/Trustmark deal signals regional-bank consolidation (KRE)
MACRO housing: [bearish] Existing home sales 16-year low confirms rate drag (session)
SECTOR AI-infra: [mixed] Cerebras raise vs Nebius capacity cut (AI basket)
INTERACTION: Hawkish rates caps software multiples; AI-infra bullish vs Nvidia governance bearish = semis mixed; housing weakness is rates confirmation.
WATCH: Nvidia CEO-exit headline risk can dominate sector fundamentals intraday.
NEWS_PARSE_END

