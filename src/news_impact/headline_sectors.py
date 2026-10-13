"""Evidence-bearing sector relevance, independent of the trading gate.

Catalog classifications describe a named company's business; title activity
matches describe the product/industry mentioned. Neither proves stock exposure.
"""
import re
from pathlib import Path
from .finviz_linker import get_index, _clean_company
from .headline_events import headline_text

VERSION = 'headline-sectors-v1'
# Canonical sector, activity, bounded noun phrase. These are economic activities,
# not issuer mappings or a list of news headlines.
ACTIVITIES = [
 ('Energy','Oil and gas pipelines',r'\bpipelines?\b'),
 ('Industrials','Project imports',r'\bproject import\b'),
 ('Financial','Catastrophe-risk financing',r'\b(?:cat|catastrophe) bond\b'),
 ('Utilities','Electric utilities',r'\bpower regulator|electricity regulator|discoms\b'),
 ('Energy','Diesel distribution and refining',r'\bdiesel\b'),
 ('Energy','Oil and gas',r'\b(?:crude oil|oil prices?|oil supply|Lukoil|gas price|gas trade|gas payday|deepwater gas)\b'),
 ('Consumer Defensive','Palm oil trade',r'\bpalm oil\b'),
 ('Technology','Semiconductors',r'\b(?:semiconductors?|chipmaker|chips? smuggling)\b'),
 ('Technology','Artificial intelligence services',r'\b(?:AI (?:services|cloning|adoption)|xAI)\b'),
 ('Technology','Vehicle noise-control technology',r'\bvehicle noise reduction\b'),
 ('Communication Services','Social media',r'\b(?:social media|Instagram)\b'),
 ('Communication Services','Film and television',r'\b(?:film|television|Prime Video|ABC premiere|spinoff.{0,20}(?:premiere|minutes viewed)|Hollywood)\b'),
 ('Financial','Lending and refinancing',r'\b(?:lending|refinancing|lender|borrowers?|LPR|bank lending)\b'),
 ('Financial','Capital markets',r'\b(?:public offers|IPO boom)\b'),
 ('Financial','Tokenized securities',r'\btokenized securities\b'),
 ('Real Estate','REITs',r'\bREITs?\b'),
 ('Financial','Pension services',r'\bpension funds?\b'),
 ('Financial','SME financing',r'\b(?:MSMEs|SME).{0,30}(?:interest aid|financ\w*)\b'),
 ('Industrials','Defence equipment',r'\b(?:defence|defense) (?:equipment|production)\b'),
 ('Industrials','Industrial transmission equipment',r'\btransmissions\b'),
 ('Industrials','Manufacturing',r'\bmanufacturing\b'),
 ('Industrials','Marine transport',r'\b(?:Hormuz|container shipping|marine transport)\b'),
 ('Industrials','Passenger transport',r'\b(?:per-trip transport|passenger transport)\b'),
 ('Industrials','Airlines',r'\b(?:international flights|air travel|airlines?)\b'),
 ('Industrials','Trucking',r'\btruckers?|trucking\b'),
 ('Utilities','Water utilities',r'\bwater rate\b'),
 ('Utilities','Electric utilities',r'\belectric (?:utilities|raises? guidance)\b'),
 ('Industrials','Industrial electricity use',r'\b(?:MSMEs|SMEs)\b.{0,45}\bpower subsidies\b'),
 ('Healthcare','Medical devices',r'\b(?:catheter|surgical.{0,30}valve|AUTUS|medical devices?)\b'),
 ('Healthcare','Food-safety and diagnostic testing',r'\bfood-safety|diagnostic testing\b'),
 ('Healthcare','Biotechnology',r'\b(?:biotech|pharma|clinical trial)\b'),
 ('Healthcare','Antiviral medicines',r'\b(?:HIV treatment|HSV lesions|antiviral)\b'),
 ('Consumer Cyclical','Apparel retail',r'\b(?:apparel retail|Primark)\b'),
 ('Consumer Defensive','Food businesses',r'\b(?:food businesses|food-safety|vegetable product|food\?)\b'),
 ('Consumer Cyclical','Consumer retail and services',r'\bholiday spending\b'),
 ('Consumer Cyclical','Garment manufacturing',r'\bgarment factory\b'),
 ('Consumer Cyclical','Auto manufacturing',r'\b(?:auto manufacturing|GM dealer|Kia|ICE and EV models)\b'),
 ('Consumer Cyclical','Auto retail and repair',r'\b(?:dealer inventory|AutoCanada|collision network|auto retail|buyback scheme.{0,30}ICE)\b'),
 ('Consumer Cyclical','Restaurants',r'\brestaurants?\b'),
 ('Consumer Cyclical','Furniture manufacturing',r'\bfurniture\b'),
 ('Consumer Defensive','Agriculture',r'\bfarmers?|harvest season|agriculture\b'),
 ('Consumer Defensive','Fishing',r'\bfisherfolk|fishing\b'),
 ('Basic Materials','Mining',r'\b(?:mining|mineral resource|zinc|copper and nickel)\b'),
 ('Basic Materials','Uranium mining',r'\buranium production\b'),
 ('Basic Materials','Copper and nickel mining',r'\bCu\b.{0,25}\bNi\b'),
 ('Basic Materials','Coatings manufacturing',r'\bcoatings\b'),
]
# Supplemental company facts for firms not covered by the US-listed catalog.
# Sources record business identity, never event labels or affected-stock claims.
SUPPLEMENTAL = [
 (r'\bConvatec\b','Healthcare','Medical devices','https://www.convatec.com/about-us/'),
 (r'\bAssociated British Foods\b','Consumer Defensive','Food businesses','https://www.abf.co.uk/our-businesses'),
 (r'\bHapag-Lloyd\b','Industrials','Container shipping','https://www.hapag-lloyd.com/en/company/about-us.html'),
 (r'\bSony\b','Communication Services','Entertainment','https://www.sony.com/en_us/SCA/who-we-are/overview.html'),
 (r'\bSony\b','Technology','Consumer Electronics','https://www.sony.com/en_us/SCA/who-we-are/overview.html'),
 (r'\bNeogen\b','Healthcare','Food-safety and diagnostic testing','https://www.neogen.com/ko/about/our-mission/'),
 (r'\bWestern Union\b','Financial','Money transfer','https://www.westernunion.com/corporate/about-us.html'),
 (r'\bHo Bee Land\b','Real Estate','Property development','https://www.hobee.com/about/corporate-profile'),
 (r'\bMultotec\b','Industrials','Mining equipment and services','https://www.multotec.com/en/mineral-processing-equipment'),
 (r'\bAggreko\b','Industrials','Power-generation equipment','https://www.aggreko.com/'),
 (r'\bAxalta\b','Basic Materials','Coatings manufacturing','https://www.axalta.com/careers_global/en_US/about-axalta/what-we-do.html'),
 (r'^Stake\b','Consumer Cyclical','Online gambling','https://stake.com/'),
 (r'\bBoots\b','Consumer Defensive','Pharmacy retail','https://www.boots.com/health-pharmacy/'),
 (r'\bPowersoft\b','Technology','Professional audio equipment','https://www.powersoft.com/en/about/who-we-are'),
]


def identify_headline_sectors(title, *, root=None):
    text = headline_text(title)
    matches = []
    def add(sector, activity, match, basis, **extra):
        matches.append({'sector': sector, 'activity': activity, 'basis': basis,
            'evidence': {'quote': match[0], 'start': match.start(), 'end': match.end()},
            'exposure_verified': False, **extra})
    for sector, activity, pattern in ACTIVITIES:
        m = re.search(pattern, text, re.I)
        if m:
            add(sector, activity, m, 'title_activity')
    for pattern, sector, activity, source in SUPPLEMENTAL:
        m = re.search(pattern, text, re.I)
        if m and m[0][0].isupper():
            add(sector, activity, m, 'company_business', source=source)
    # Repository-relative root works even when intake is invoked from elsewhere.
    index = get_index(Path(root) if root else Path(__file__).resolve().parents[2])
    low = text.lower()
    for row in index.rows:
        if row.get('etf'):
            continue
        # Title_names is deliberately loose for candidate discovery. Sector
        # attribution requires an actual bounded alias/brand/ticker span.
        # Full cleaned company names only. Partial alias matching pulls ordinary
        # words ("dollar", "now", "service") and unrelated people into the result.
        aliases = [_clean_company(row['company'])]
        aliases += [token for token, hits in index.brands.items() if row in hits]
        aliases += [r'\$'+re.escape(row['ticker']), r'\('+re.escape(row['ticker'])+r'\)', r'(?:NYSE|NASDAQ):\s*'+re.escape(row['ticker'])]
        for alias in aliases:
            if alias.startswith((r'\$', r'\(', '(?:NYSE')):
                if row['ticker'].lower() not in low:
                    continue
            elif len(alias)<4 or alias.lower() not in low:
                continue
            if ' ' not in alias and not alias.startswith((r'\$', r'\(', '(?:NYSE')) and alias not in index.brands:
                if alias in {'southern', 'northern', 'western', 'eastern', 'news', 'people', 'today', 'service', 'bill', 'limited'}:
                    continue
                # An issuer's proper name is capitalized; a lower-case noun or
                # verb identical to a company name is insufficient evidence.
                if not any(m[0][0].isupper() for m in re.finditer(r'\b'+re.escape(alias)+r'\b', text, re.I)):
                    continue
            pat = alias if alias.startswith((r'\$', r'\(', '(?:NYSE')) else re.escape(alias)
            m = re.search(r'(?<!\w)'+pat+r'(?!\w)', text, re.I)
            if m:
                if ' ' not in alias and not alias.startswith((r'\$', r'\(', '(?:NYSE')) and alias not in index.brands:
                    prefix=text[:m.start()]
                    if prefix.strip() and not re.search(r'\b(?:from|for|at|on|of)\s*$',prefix,re.I):
                        continue
                add(row['sector'], row['industry'], m, 'company_catalog', ticker=row['ticker'], company=row['company'])
                break
    matches = [m for m in matches if m['sector'] and m['activity']]
    return {'version': VERSION, 'input_scope': 'title_only',
        'status': 'identified' if matches else 'unknown', 'matches': matches,
        'catalog_source': index.source, 'catalog_available': bool(index.rows),
        'market_sectors': sorted({m['sector'] for m in matches}),
        'market_activities': sorted({m['activity'] for m in matches}),
        'unknowns': ['Sector relevance does not establish material exposure, winners, losers or price direction']}
