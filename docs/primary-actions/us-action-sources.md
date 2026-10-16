# US concrete actions and their official origins

Checked 15 Oct 2026 from the collector's public client. A headline in the news directory is not itself the action. The tables below say which official system publishes the action the headline is about, and whether that system answered.

The news directory at `dashboard/news-dir` held 12,031 parsed headlines (8 Aug 2026 through 1 Oct 2026), almost all from market aggregators (`rss_google_markets`, Yahoo Finance, Finviz). A strict pass, requiring an operative government act rather than the words "bill", "Fed", or "ruling" alone, found 155 unique titles. Commentary such as "stocks that could rally if the Fed raises rates" was excluded.

| Action class in the news | Unique titles | Official origin | What the scanner does |
|---|---:|---|---|
| FOMC rate decision | 65 | Federal Reserve monetary-policy press | `federalreserve.gov/feeds/press_monetary.xml` returned RSS. Enabled. |
| Tariff or trade restriction | 22 | USTR press, and the Federal Register when the measure is published | USTR `ustr.gov/rss.xml` returned RSS and is now a seed. Federal Register API is already collected. |
| Sanctions or export control | 22 | OFAC recent actions; BIS rules via the Federal Register | OFAC HTML index is collected. OFAC RSS `recent-actions/rss.xml` is 404 (retired). BIS has no working public RSS from here. |
| Fed personnel | 17 | White House presidential actions, then the Senate if a nomination is submitted | White House `presidential-actions/feed/` returned RSS. There is no public nominations API without a Congress.gov key. |
| Legislation moving | 9 | Congress.gov bill status; GovInfo for bill text, enrolled bills, and public laws | GovInfo `bills.xml`, `bills-enr.xml`, and `plaw.xml` are collected. Congress.gov floor feeds answered and were empty on this date. Bills presented to the president returned three undated items; the act name is now the headline. |
| Court ruling | 7 | Supreme Court slip opinions and orders; GovInfo participating-court RSS | Both Supreme Court HTML indexes and 165 GovInfo court feeds are collected. PACER is not fetched. |
| Agency enforcement | 6 | SEC press; FTC press; DOJ press | SEC press RSS returned 200. FTC RSS returned 403. The old DOJ URL was 404; `justice.gov/news/rss` returned 25 releases. |
| Executive order | 5 | Federal Register presidential documents, and the White House actions feed | Both answered. Federal Register `PRESDOCU` is inside the documents API already collected. |
| Treasury auction | 1 | TreasuryDirect announced-securities API | No feed was configured. The public JSON API returned 250 securities and the matching announcement PDF returned 200. A collector adapter now reads that API. |
| FDA approval | 1 | FDA press, and the Federal Register when the action is a rule or notice | Several FDA topic RSS feeds are enabled. The old press-announcements RSS path is 404. |

Most of the other 12,000 headlines are earnings, analyst notes, and market wraps. Those do not have a government origin.

## Systems that actually publish the action

These are the primary US systems. Directory membership is not a feed. An endpoint is "answered" only if this client received a parseable public document on 15 Oct 2026.

### Legislation

Congress.gov is the status system: bill text, actions, and latest action. The collector calls `https://api.congress.gov/v3/bill` when `DATA_GOV_API_KEY` is set, and keeps the public RSS files that do not require a key:

| Feed | Live result |
|---|---|
| `https://www.congress.gov/rss/presented-to-president.xml` | 200, three bills, no per-item dates. Act name is in the description. |
| `https://www.congress.gov/rss/house-floor-today.xml` | 200, valid empty channel. |
| `https://www.congress.gov/rss/senate-floor-today.xml` | 200, valid empty channel. |
| `https://www.congress.gov/rss/` HTML index | 403 from this client. Individual XML files still answered. |

GovInfo publishes the documents, not the live whip count:

| Feed | What it is |
|---|---|
| `https://www.govinfo.gov/rss/bills.xml` | Congressional bill versions |
| `https://www.govinfo.gov/rss/bills-enr.xml` | Enrolled bills, not yet law |
| `https://www.govinfo.gov/rss/plaw.xml` | Public and private laws |
| `https://www.govinfo.gov/rss/crpt.xml` | Committee reports |
| `https://www.govinfo.gov/rss/chrg.xml` | Hearings |
| `https://www.govinfo.gov/rss/crec.xml` | Congressional Record |

### Executive instruments and rules

One Federal Register API covers rules, proposed rules, notices, and presidential documents for every issuing agency, without a key:

`https://www.federalregister.gov/api/v1/documents.json`

Filter with `conditions[type][]=RULE|PRORULE|NOTICE|PRESDOCU` and `conditions[publication_date][gte]`. A presidential-document probe returned `count: 8594`. The White House feed `https://www.whitehouse.gov/presidential-actions/feed/` is the signing-day channel; the Federal Register copy is the citeable text. Regulations.gov documents are collected from `https://api.regulations.gov/v4/documents` with the same api.data.gov key: rules, proposed rules, and notices only. Supporting files and the comment-posting API are not called. SAM.gov contract opportunities still need a separate key and are not called.

### Monetary policy

`https://www.federalreserve.gov/feeds/press_monetary.xml` and `press_all.xml` returned RSS. Some sibling Fed feeds (enforcement, commercial paper, Z.1, SLOOS) returned 406 on the last scheduled run. Those are reference data or blocked conditional requests, not the FOMC decision feed.

### Sanctions, export controls, tariffs

| System | Endpoint | Live result |
|---|---|---|
| OFAC recent actions | `https://ofac.treasury.gov/recent-actions` | HTML 200. Dated links are already parsed. |
| OFAC RSS | `https://ofac.treasury.gov/recent-actions/rss.xml` | 404. Retired. GovDelivery is off the official-host list and is not requested. |
| USTR | `https://www.ustr.gov/rss.xml` | 200, 10 items, including an October 2026 press release. |
| BIS | `https://www.bis.gov/press-releases` | No public RSS answered. Entity-list rules that reach the Federal Register are collected there. |
| Federal Register | documents API | Notices and rules from OFAC, BIS, USTR, and Customs when they are published. |

### Enforcement and markets

| System | Endpoint | Live result |
|---|---|---|
| DOJ all press | `https://www.justice.gov/news/rss` | 200, 25 items. Replaces `justice.gov/feeds/opa/justice-news.xml`, which is 404. Includes routine criminal press, not only market cases. |
| DOJ Antitrust | `https://www.justice.gov/news/rss?type[0]=press_release&field_component=376&search_api_language=en` | 200. This is the feed linked from the Antitrust Division news-feeds page. |
| SEC press | `https://www.sec.gov/news/pressreleases.rss` | 200. |
| SEC litigation RSS and EDGAR current-filings Atom | several `sec.gov` paths | 403 from this client. |
| FTC press | `https://www.ftc.gov/feeds/press-release.xml` and the newsroom RSS | 403. Left disabled until the host answers this client. |
| CFTC | `https://www.cftc.gov/RSS/RSSGP/rssgp.xml` | 403 on this check. It had been enabled after an earlier verification. |
| CFPB | `https://www.consumerfinance.gov/about-us/newsroom/feed/` | 200. |
| FDIC, OCC, FinCEN RSS paths in the seed file | | 404. FDIC press HTML still lists releases. No replacement RSS answered. |

### Courts

Supreme Court slip opinions and orders are HTML indexes on `supremecourt.gov`. GovInfo `uscourts-*.xml` covers participating courts only. Paid PACER documents are not fetched.

### Fiscal operations

Treasury press `https://home.treasury.gov/news/press-releases/feed` timed out. Auction announcements are a separate system:

`https://www.treasurydirect.gov/TA_WS/securities/announced?format=json`

Each row carries `cusip`, `announcementDate`, `auctionDate`, `offeringAmount`, and `pdfFilenameAnnouncement`. The PDF `https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/A_20261013_1.pdf` returned `application/pdf`. The collector stores that PDF link and a one-line title. It does not download the PDF body.

### Other official feeds that answered

| Feed | Use |
|---|---|
| `https://www.dol.gov/rss/releases.xml` | Labor press |
| `https://www.nist.gov/news-events/news/rss.xml` | NIST news, mostly not a binding action |
| `https://www.cisa.gov/cybersecurity-advisories/all.xml` | Known-exploited-vulnerability catalog additions. Now a seed. |
| `https://www.govinfo.gov/rss/dcpd.xml` | Compilation of Presidential Documents |
| `https://www.govinfo.gov/rss/fr.xml` | Federal Register package feed. The JSON API remains the typed source. |

### Answered 403 or 404 and left inactive

FTC, CFTC, FCC, HHS, State Department press, BLS, SEC EDGAR Atom, California OAL, FDIC/OCC/FinCEN RSS, FEMA RSS, Energy RSS, OFAC RSS, and the Congress.gov HTML index. Forcing a browser identity to get past a 403 is not part of this collector. Federal Register still carries the rules and notices those agencies publish.

## What changed in the collector

- DOJ now requests the two RSS URLs that returned 200, not the 404 legacy path.
- Congress floor feeds are requested beside bills presented to the president. An item whose title is only `S.2403` uses the act name from the description.
- USTR and CISA advisories are seeds.
- TreasuryDirect announced auctions are a first-class adapter. A result with a yield or bid-to-cover is labeled as an auction result and dated on the auction date. An announcement without a result stays an announcement.

The scheduled catalog refresh compares the seed and implementation hashes. The next run on the default branch re-verifies these URLs before enabling them. A host that returns 403 stays disabled and is reported.

## Every requested site, checked 15 Oct 2026

“Parsable” means this client received JSON, XML, or RSS/Atom with a public GET. Congress.gov, Regulations.gov, and GovInfo public laws also accept the free api.data.gov key in the `X-Api-Key` header. The key is read from `DATA_GOV_API_KEY` and is never written into a URL, the repo, or `http_request_audit.json`. A 403 or an HTML page with no feed is recorded and not treated as a working collector source. CourtListener is not a .gov host, and PACER is paid, so neither is fetched.

| Site | Parsable endpoint that answered | Collector |
|---|---|---|
| Federal Register | `api/v1/documents.json` and public inspection `public-inspection-documents.json` (131 documents, one page) | Both. Public inspection is the advance copy. |
| Regulations.gov | `api.regulations.gov/v4/documents` | Collected when `DATA_GOV_API_KEY` is set. Rules, proposed rules, and notices. Comment posting is not used. |
| GovInfo | Collection RSS already collected. `bulkdata/json/FR` is a year/folder listing | RSS for bills, laws, courts, Record, presidential documents. The PLAW collection API is collected with the same key. Bulk XML stays a backfill tree. |
| eCFR | `ecfr.gov/api/versioner/v1/titles.json` | Codified titles, not a change feed. Amendments arrive in the Federal Register. |
| USA.gov agency index | HTML directory | Used as an identity list, not a publication feed. |
| Congress.gov | Three public RSS files. API v3 `api.congress.gov/v3/bill` | RSS collected. Bill actions in the window are collected when `DATA_GOV_API_KEY` is set. A latest action of “Became Public Law” is stored as a law; “Presented to President” stays enrolled, not enacted by inference. |
| House.gov | No RSS at the paths tried | Floor activity is the Congress.gov House feed. |
| Senate.gov | HTML at the guessed RSS path | Floor activity is the Congress.gov Senate feed. |
| CBO | `cbo.gov/publications/all/rss.xml` (30 dated items) | Seeded. |
| GAO | — | 403 on the RSS paths. |
| White House actions | `whitehouse.gov/presidential-actions/feed/` | Collected. |
| White House briefing room | `whitehouse.gov/news/feed/` (30 items, 4–15 Oct) | Seeded as official statements. Birthday messages sit in the same feed as policy posts. |
| Federal Reserve policy | `feeds/press_monetary.xml`, `press_all.xml` | Collected. |
| Federal Reserve speeches | `feeds/speeches.xml` (15 dated items) | Seeded. Research and testimony feeds stay excluded. |
| SEC press and speeches | `sec.gov/news/pressreleases.rss`, `sec.gov/news/speeches.rss` | Both seeded. Litigation RSS returned 403. |
| SEC EDGAR / data.sec.gov | — | 403 to this client, including `data.sec.gov/submissions/`. Company filings are not collected. |
| CFTC | — | 403. |
| Treasury press | `home.treasury.gov/news/press-releases/feed` | Timed out. |
| Treasury auctions | TreasuryDirect JSON, and Fiscal Data `auctions_query` JSON | TreasuryDirect is the collector. Fiscal Data is the same auctions in another keyless JSON API. |
| OFAC recent actions | HTML index with dates in the URL | Collected. RSS is 404. |
| OFAC SDN bulk | `treasury.gov/ofac/downloads/sdn.xml` and the Sanctions List Service XML | Full designation list, not a daily headline feed. Not ingested as one row per name. |
| OCC, FDIC, FinCEN | — | Seed RSS paths are 404. |
| CFPB | `consumerfinance.gov/about-us/newsroom/feed/` | Answers. |
| FHFA | — | No RSS at the paths tried. |
| FSOC | Treasury / Federal Register | No separate feed found. Designations that are published hit the Federal Register. |
| openFDA Drugs@FDA | `api.fda.gov/drug/drugsfda.json` with a submission-date search | Adapter keeps approval (`AP`) submissions inside the window. A 14-day check returned 86. An empty window is HTTP 404 and is stored as no matches. Device `pma.json` also answers and is not on the daily pass. |
| FDA press RSS | Several topic feeds | Press-announcements path is 404. Topic feeds that parse stay. |
| FTC | — | 403 on the press RSS. |
| DOJ | `justice.gov/news/rss` and the Antitrust Division filter | Both seeded. |
| FERC | HTML decisions index | No RSS answered. |
| EPA | `epa.gov/newsreleases/search/rss` | HTTP 202 HTML, not a feed. Rules still go through the Federal Register. |
| Energy | `energy.gov/rss.xml` | Parses, and the newest item is from 2020. Not seeded. |
| Commerce, BIS | — | Commerce RSS 403. BIS RSS 404. Export-control rules that are notices still go through the Federal Register. |
| USTR | `ustr.gov/rss.xml` | Seeded. |
| USITC | — | Timed out. |
| Labor | `dol.gov/rss/releases.xml` | Answers. |
| OSHA | — | 403. |
| IRS | — | No RSS at the paths tried. Tax rules that are published hit the Federal Register. |
| Supreme Court | Slip-opinion and orders HTML indexes | Collected. |
| PACER | Paid docket system | Not fetched. |
| CourtListener | Unofficial REST API | Not a government host. Not fetched. |
| BLS | `api.bls.gov/publicAPI/v1/timeseries/data/{series}` for one series | RSS is 403. One series is not the Employment Situation release, so it is not a daily headline source. |
| BEA | `apps.bea.gov/rss/rss.xml` (48 dated releases). The data API needs a key | RSS seeded. |
| Census | `census.gov/economic-indicators/indicator.xml` (18 indicators, including retail sales). Data API needs a key | Indicator feed seeded. |
| FRED | API exists on `api.stlouisfed.org` | Key required. Host is not a `.gov` suffix. Not fetched. |
| USDA WASDE | A report-numbered XML file such as `wasde1026.xml` | No stable feed URL. Not seeded. |
| USDA, Education, DHS, State, DOT, FAA, HHS, CMS | — | 403 or 404 on the RSS paths tried. |
| Defense | `defense.gov` ArticleCS RSS (10 items, 14 Oct) | Seeded. |
| HUD | `hud.gov/rss.xml` (through 14 Oct) | Seeded. |
| Interior | `doi.gov/feeds/content/36980/rss.xml` | Replaces the 404 `/rss/news.xml`. The live channel was empty on this check. |
| Veterans Affairs | `news.va.gov/feed/` | Seeded. The feed mixes benefit news with general posts. |
| FCC | — | 403. |
| CPSC | Recalls RSS path | 404. |
| NLRB | `nlrb.gov/rss/rssPressReleases.xml` | Seeded. |
| EEOC, NRC, EXIM, GSA | — | 404, 503, or no feed at the paths tried. |
| FEC | HTML data pages. `api.open.fec.gov` needs a key | Not seeded. |
| FMC | `fmc.gov/rss` | Parses, last item May 2024. Not seeded. |
| SBA | `sba.gov/feed` | Seeded. |
| Data.gov / api.data.gov | One key covers Congress.gov, Regulations.gov, and GovInfo | The collector reads `DATA_GOV_API_KEY`. A missing key leaves those three sources enabled and reports `key_not_configured` instead of calling the API. |
| SAM.gov | Opportunities API | Key required. A solicitation is not an award. |
