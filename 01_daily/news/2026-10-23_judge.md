# News Judge — 2026-10-23

### IMPORTANT NEWS (my ranking)
1. Nasdaq 100 worst day in four months on capex/debt fatigue; META/SPCX/BE/MRNA focus — broad Nasdaq/SPX risk-appetite hit outranks single-name color — channel: risk.
2. Hawkish Fed comments lift October hike expectations; gold drops >$100 — rates/hike-odds shock hits SPX beta and gold/miners — channel: rates.
3. China restricts AI/chip-design exports; Taiwan tightens AI chip/server controls — direct semis/AI supply-chain policy shock — channel: sector_policy.
4. Brent crude above $111 on Strait of Hormuz attacks — energy/inflation/geopolitical risk force — channel: rates.
5. US September CPI +0.6% m/m, 3.7% y/y — sticky inflation keeps rates/FOMC risk live — channel: rates.
6. Mega-cap Q3 earnings: Meta; Microsoft/Alphabet capex read-through — AI capex scrutiny and Nasdaq beta — channel: risk.
7. Boeing 737 MAX 10 certification halted by FAA over software glitch; 31% of undelivered order book at risk — aerospace/industrials basket force — channel: sector_fundamental.
8. Supreme Court IEEPA tariff refunds — CIT certifies importer class — trade/legal overhang for tariff-exposed importers — channel: sector_policy.

### STEP 1 — FRAMEWORK SCORE
- Nasdaq 100 worst day/capex fatigue: keep | us_relevance high — direct SPX/Nasdaq beta | channel risk | geography us_domestic | severity session | horizon 1d-1w | object spx:Nasdaq-100/SPX beta | pol bearish | why capex/debt weariness hits mega-cap multiples | conf 0.82
- Hawkish Fed/Oct hike odds + gold drop: keep | us_relevance high — rates/front-end repricing | channel rates | geography us_domestic | severity regime | horizon 1w | object spx:SPX beta/rates | pol hawkish | why higher hike odds pressure duration and risk appetite | conf 0.78
- China/Taiwan AI-chip export controls: keep | us_relevance high — semis/AI supply chain | channel sector_policy | geography us_supply_chain | severity session | horizon 1w-1m | object sector_etf:SOXX/SMH/AI supply chain | pol bearish | why tighter export controls raise AI/semi revenue-risk | conf 0.80
- Brent >$111 on Hormuz attacks: keep | us_relevance high — inflation/energy transmission | channel rates | geography global_priced | severity regime | horizon 1d-1w | object sector_etf:XLE/energy; SPX inflation risk | pol mixed | why energy bullish, broad risk/inflation bearish | conf 0.77
- US September CPI +0.6% m/m, 3.7% y/y: keep | us_relevance high — sticky inflation constrains Fed | channel rates | geography us_domestic | severity session | horizon 1d-1w | object spx:SPX beta/rates | pol hawkish | why hot CPI keeps hike-risk alive | conf 0.76
- Mega-cap Q3 earnings/capex read-through: conditional | us_relevance high — Nasdaq concentration | channel risk | geography us_domestic | severity session | horizon 1d-1w | object spx:MSFT/GOOGL/META Nasdaq beta | pol mixed | why capex scrutiny cuts both AI demand and multiples | conf 0.68
- Boeing 737 MAX 10 FAA cert halt: conditional | us_relevance medium — BA plus aerospace suppliers | channel sector_fundamental | geography us_domestic | severity session | horizon 1w | object basket:BA/aerospace suppliers | pol bearish | why certification delay risks order-book timing | conf 0.70
- IEEPA tariff refunds class certified: conditional | us_relevance medium — importer/tariff-exposed baskets | channel sector_policy | geography us_domestic | severity session | horizon 1w-1m | object basket:importers/tariff-exposed | pol mixed | why legal refund path adds policy uncertainty | conf 0.62

### STEP 2 — INTERACTIONS
Fired: hawkish Fed/CPI + capex/debt fatigue → do not buy long-duration software/semis on dovish-rates hope; China/Taiwan AI export controls + capex fatigue → semis/AI mixed, do not double-count as one clean bearish signal; Brent/Hormuz + CPI/hike odds → treat as one rates/energy inflation cluster; Boeing FAA halt + industrials earnings → aerospace basket caution; AMGN positive Phase 3 dazodalibep data + healthcare sympathy → healthcare basket, not only AMGN; IEEPA tariff class cert + importers → sector_policy overhang. Not fired: weak labor absent; offshore-wind cancel absent; yields-down cyclicals absent.

### STEP 3 — RECLASSIFY AUDIT
Dropped from usable: France riots/political crisis — foreign_weak_link, low direct US beta; Brazil runoff — foreign political, low US risk transmission; UK Autumn Budget — foreign fiscal/bank tax not US driver; ECB rate decision — second-order to FOMC/CPI; China October LPR held — no incremental surprise; China IP/retail sales — mixed China data already covered by GDP beat; Japan CPI — foreign inflation, weak US link; Eurozone PMI miss — foreign growth, weak US link. Rescued from noise: none — noise sample empty. Finviz ticker digests used only as elevated sector context, not mechanical-noise rescue.

RULES_APPLIED: a-fresh-overnight-kinetic-oil-supply-escalation-iran-hormuz; a-scheduled-high-impact-macro-release-cpi-nfp-fomc-is-the-do.

### STEP 4 — B1 / SECTOR INJECT
NEWS_JUDGE: n=8 rescued=0
MACRO rates: [pol] hawkish Fed/Oct hike odds + CPI 3.7% keep front-end risk live (regime/1w)
MACRO risk: [pol] Nasdaq-100 worst day on capex/debt fatigue; mega-cap AI capex scrutiny (session/1d-1w)
SECTOR semis/AI: [pol] China AI/chip-design export curbs + Taiwan server/chip controls tighten supply chain (SOXX/SMH)
MACRO energy: [pol] Brent >$111 Hormuz attacks; oil/inflation/energy force (regime/session)
SECTOR industrials: [pol] Boeing 737 MAX 10 FAA cert halt risks 31% undelivered order book (BA/aerospace)
MACRO policy: [pol] IEEPA tariff refunds class certified; importer/tariff overhang (sector_policy)
SECTOR mega-cap: [pol] Meta/MSFT/GOOG Q3 capex/AI spend read-through for Nasdaq beta (session)
INTERACTION: hawkish Fed/CPI + capex fatigue → do not buy duration on dovish hope; oil + hawkish = rates/energy cluster
WATCH: noise sample empty; foreign political items low US beta

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Nasdaq 100 worst day in four months on capex/debt fatigue; META/SPCX/BE/MRNA focus | keep=keep | channel=risk | severity=session | horizon=1d-1w | object=spx:Nasdaq-100/SPX beta | pol=bearish | conf=0.82
- Hawkish Fed comments lift October hike expectations; gold drops >$100 | keep=keep | channel=rates | severity=regime | horizon=1w | object=spx:SPX beta/rates | pol=hawkish | conf=0.78
- China restricts AI/chip-design exports; Taiwan tightens AI chip/server controls | keep=keep | channel=sector_policy | severity=session | horizon=1w-1m | object=sector_etf:SOXX/SMH/AI supply chain | pol=bearish | conf=0.80
- Brent crude above $111 on Strait of Hormuz attacks | keep=keep | channel=rates | severity=regime | horizon=1d-1w | object=sector_etf:XLE/energy; SPX inflation risk | pol=mixed | conf=0.77
- US September CPI +0.6% m/m, 3.7% y/y | keep=keep | channel=rates | severity=session | horizon=1d-1w | object=spx:SPX beta/rates | pol=hawkish | conf=0.76
- Mega-cap Q3 earnings: Meta; Microsoft and Alphabet capex read-through | keep=conditional | channel=risk | severity=session | horizon=1d-1w | object=spx:MSFT/GOOGL/META Nasdaq beta | pol=mixed | conf=0.68
- Boeing 737 MAX 10 certification halted by FAA over software glitch; 31% undelivered order book at risk | keep=conditional | channel=sector_fundamental | severity=session | horizon=1w | object=basket:BA/aerospace suppliers | pol=bearish | conf=0.70
- Supreme Court IEEPA tariff refunds — CIT certifies importer class | keep=conditional | channel=sector_policy | severity=session | horizon=1w-1m | object=basket:importers/tariff-exposed | pol=mixed | conf=0.62
INTERACTIONS: hawkish Fed/CPI + capex fatigue → do not buy duration on dovish hope; China/Taiwan AI controls + capex fatigue → semis/AI mixed; Brent/Hormuz + CPI/hike odds → rates/energy cluster; Boeing FAA halt + industrials earnings → aerospace caution; AMGN Phase 3 positive + healthcare sympathy → healthcare basket; IEEPA tariff class cert + importers → sector_policy overhang
RESCUED_FROM_NOISE: none
DROPPED_FROM_USABLE: France riots and political crisis after cost-of-living protests; Brazil presidential runoff; UK Autumn Budget; ECB rate decision; China October LPR held; China September industrial production/retail sales; Japan September core CPI; Eurozone October flash PMI miss
B1_INJECT:
NEWS_JUDGE: n=8 rescued=0
MACRO rates: [pol] hawkish Fed/Oct hike odds + CPI 3.7% keep front-end risk live (regime/1w)
MACRO risk: [pol] Nasdaq-100 worst day on capex/debt fatigue; mega-cap AI capex scrutiny (session/1d-1w)
SECTOR semis/AI: [pol] China AI/chip-design export curbs + Taiwan server/chip controls tighten supply chain (SOXX/SMH)
MACRO energy: [pol] Brent >$111 Hormuz attacks; oil/inflation/energy force (regime/session)
SECTOR industrials: [pol] Boeing 737 MAX 10 FAA cert halt risks 31% undelivered order book (BA/aerospace)
MACRO policy: [pol] IEEPA tariff refunds class certified; importer/tariff overhang (sector_policy)
SECTOR mega-cap: [pol] Meta/MSFT/GOOG Q3 capex/AI spend read-through for Nasdaq beta (session)
INTERACTION: hawkish Fed/CPI + capex fatigue → do not buy duration on dovish hope; oil + hawkish = rates/energy cluster
WATCH: noise sample empty; foreign political items low US beta
NEWS_PARSE_END
