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

Congress.gov is the status system: bill text, actions, and latest action. The API is `https://api.congress.gov/v3/bill` and requires a free api.data.gov key, so this collector does not call it. The public RSS files that do not require a key:

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

Filter with `conditions[type][]=RULE|PRORULE|NOTICE|PRESDOCU` and `conditions[publication_date][gte]`. A presidential-document probe returned `count: 8594`. The White House feed `https://www.whitehouse.gov/presidential-actions/feed/` is the signing-day channel; the Federal Register copy is the citeable text. Regulations.gov (`api.regulations.gov`) tracks dockets and comments and needs a key. SAM.gov contract opportunities also need a key. Neither is called.

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
