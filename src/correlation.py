"""Article ↔ stock correlation from the ideas that survived the memo.

A stock passes only when the article's small circles overlap both the stock's
own description and that stock's granular industry. Large circles (a word that
sits in many industries, or a word that never appears outside one bundle or a
job title) barely move the score.

What is implemented, and the reason it earned a place:

- Repeated bundles, plus a heading bundle even from the title line.
- A bare word is absorbed when its contexts collapse into one bundle.
- Office titles (``Secretary of Transportation``) are removed before counting.
- Circle size is rarity across stocks times how narrowly the word clusters
  in one industry.
- Keyness against the other articles in the batch.
- Best-match overlap: unmatched terms add nothing, they are not averaged in.
- Both gates are outliers (within-article z, and across-article z on a batch).
- A BM25 score on the same terms is reported as the lexical baseline.
- Inflections fold (``launches`` / ``launch``). Derivatives do not.
- A name or ticker hit is recorded and does not grant a pass.
- A thin article stays unknown instead of being forced onto a stock.
- More than one industry, and more than one stock, may pass.
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from .filing_parts import divide_profile
from .places import place_names

_WORD = re.compile(r"[a-z0-9]+")
_TITLE = re.compile(
    r"\b(?:acting\s+|deputy\s+|assistant\s+)?"
    r"(?:secretary|administrator|minister|chairman|chairwoman|commissioner)"
    r"\s+of\s+(?:the\s+)?(?:department\s+of\s+|office\s+of\s+)?"
    r"[a-z][a-z&\- ]{1,80}",
    re.I,
)
_FOUNDED = re.compile(r"\bfounded by\b[^.]{0,80}", re.I)
_HQ = re.compile(r"\bheadquartered in\b[^.]{0,60}", re.I)
_SENTENCE = re.compile(r"[.!?;\n]+")

# Function words and the template language measured on blurbs and memos.
# ``transportation`` is deliberately absent: the bundle step has to shrink it.
_STOP = frozenset(
    """
    the and for are was not but you all can our its that this with from
    have has had been were they their your into over also about than then
    such including within under other these those     each both any may will every more most only very just
    would shall should could must hereby thereafter hereinafter pursuant
    nothing none not into from this that than then such over under after
    before between against without during while where which their there here
    memorandum relevant coordination administrator secretary agencies
    agency department office commission chairman commissioner minister
    acting deputy assistant united states america american national
    company companies corp corporation inc incorporated ltd plc co
    group holdings holding limited engages engaged engage operates
    operate operating consists consist including primarily through
    which while where when been being their there hereby
    founded headquartered headquarter provides provide provided
    offering offers service services product products business businesses
    segment segments year years million billion percent
    """.split()
)

_ACRONYM_OK = frozenset("nasa fda sec fed doj".split())


_KEEP_WHOLE = frozenset("series species news this thus".split())


def inflection(word: str) -> str:
    """Fold clear plurals. Leave -tion/-or derivatives and names like Lockheed."""
    if len(word) <= 4 or word in _KEEP_WHOLE:
        return word
    if word.endswith("ies") and len(word) > 5:
        return word[:-3] + "y"
    if word.endswith(("ches", "shes", "sses", "xes", "zes")) and len(word) > 5:
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 4:
        return word[:-1]
    if word.endswith("ing") and len(word) > 7:
        stem = word[:-3]
        if len(stem) >= 5:
            return stem
    # "launched" -> launch. "lockheed" ends in eed and is not a verb.
    if word.endswith("ed") and not word.endswith("eed") and len(word) > 6:
        stem = word[:-2]
        if len(stem) >= 2 and stem[-1] == stem[-2]:
            stem = stem[:-1]
        if len(stem) >= 5:
            return stem
    return word


def _keep_token(token: str) -> bool:
    if token in _ACRONYM_OK:
        return True
    if token in _STOP or token in _LIGHT or len(token) < 3:
        return False
    if token.isdigit():
        return False
    return True


_SECTION = re.compile(r"(?i)\bsec(?:tion)?\.?\s+\d+[a-z]?\b")
_PUBLISHER = re.compile(r"\s+[-|\u2013|\u2014]\s+\S.{0,80}$")
_PLACES = place_names()
# Participles and light verbs. "showing up" is not a topic. Months are dates.
_LIGHT = frozenset("""
showing show showing going went gone getting get making make taking
take coming come looking look seeing see saying said trying tried
using used doing pay paying putting put keeping kept letting let
helping helped starting started continuing asking asked according
based compared reported reporting announced announce announces
january february march april may june july august september october
november december
""".split())
# "plans to launch a venue" is a verb. "1,000 launches" is a noun and is kept.
_VERB_USE = re.compile(
    r"\b(?:plan(?:s|ned)?|agree(?:s|d)?|expect(?:s|ed)?|intend(?:s|ed)?|"
    r"seek(?:s|ing)?|propose(?:s|d)?|will|would)\s+(?:to\s+)?([a-z]{3,})",
    re.I,
)


def _drop_repeated_title(text: str) -> str:
    """A stored body often repeats the headline, which would count it twice."""
    lines = (text or "").splitlines()
    if len(lines) < 2:
        return text or ""
    title = _PUBLISHER.sub("", lines[0]).strip()
    if len(title) < 20:
        return text
    rest = "\n".join(lines[1:])
    rest = re.sub(re.escape(title), " ", rest, count=1, flags=re.I)
    return lines[0] + "\n" + rest


def strip_sources(text: str) -> str:
    """Remove the outlet on a headline, including the copy that lost its dash.

    ``IPO - Renaissance Capital`` names the source. The stored body often
    repeats that line as ``IPO Renaissance Capital``. The phrase found after
    the dash is deleted everywhere in the document.
    """
    text = _drop_repeated_title(text or "")
    lines = text.splitlines() or [text]
    publishers: list[str] = []
    cleaned: list[str] = []
    for line in lines:
        while True:
            match = _PUBLISHER.search(line)
            if not match:
                break
            chunk = match.group()
            name = re.split(r"[-|\u2013|\u2014]", chunk, maxsplit=1)[-1].strip()
            if len(name) >= 3:
                publishers.append(name)
            line = line[:match.start()]
        cleaned.append(line)
    text = "\n".join(cleaned)
    for name in publishers:
        text = re.sub(re.escape(name), " ", text, flags=re.I)
    return text


def strip_places(text: str) -> str:
    for name in _PLACES:
        text = re.sub(rf"\b{re.escape(name)}\b", " ", text, flags=re.I)
    return text


def clean_surface(text: str) -> str:
    text = strip_sources(text or "")
    text = strip_places(text)
    text = _SECTION.sub(" ", text)
    text = _TITLE.sub(" ", text)
    text = _FOUNDED.sub(" ", text)
    text = _HQ.sub(" ", text)
    return text


@dataclass
class Prepared:
    terms: dict[str, float]
    bundles: list[str]
    absorbed: list[str]
    tokens: int
    heading: list[str] = field(default_factory=list)
    # Content words that share a sentence. An industry vote from one word
    # needs another word in the same sentence from that same industry.
    sentences: list[list[str]] = field(default_factory=list)

    @property
    def known(self) -> bool:
        return len(self.terms) >= 3 and self.tokens >= 12


def _heading_lines(text: str) -> set[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return set()
    chosen = {lines[0]}
    for line in lines[1:]:
        words = line.split()
        if len(words) > 14:
            continue
        if line.isupper() or line.lower().startswith(("sec.", "sec ", "section ")):
            chosen.add(line)
    return chosen


def _verb_tokens(text: str) -> set[str]:
    """Surface words used as verbs, such as launch in ``plans to launch``."""
    return {match.group(1).lower() for match in _VERB_USE.finditer(clean_surface(text).lower())}


def prepare(text: str) -> Prepared:
    """Bundles, absorption, and title removal for one document."""
    text = clean_surface(text)
    heading = _heading_lines(text)
    verbs = _verb_tokens(text)
    # None is a break, so a bundle cannot form across a stopword or a title.
    stream: list[tuple[str, bool] | None] = []
    sentences: list[list[str]] = []
    current: list[str] = []
    for line in (text or "").splitlines() or [text or ""]:
        is_heading = line.strip() in heading
        cleaned = line.lower()
        for piece in _SENTENCE.split(cleaned):
            if current:
                sentences.append(current)
                current = []
            for raw in _WORD.findall(piece):
                token = inflection(raw)
                # Drop the verb in "plan to launch", not the noun in "1,000 launches".
                if raw in verbs:
                    stream.append(None)
                    continue
                if _keep_token(token):
                    stream.append((token, is_heading))
                    current.append(token)
                else:
                    stream.append(None)
        if current:
            sentences.append(current)
            current = []

    uni: Counter[str] = Counter()
    bi: Counter[tuple[str, str]] = Counter()
    tri: Counter[tuple[str, str, str]] = Counter()
    bi_raw: Counter[tuple[str, str]] = Counter()
    tri_raw: Counter[tuple[str, str, str]] = Counter()
    partner_hits: Counter[tuple[str, str]] = Counter()
    kept = 0
    for index, item in enumerate(stream):
        if item is None:
            continue
        token, is_heading = item
        kept += 1
        # The title counts more, but one title mention is not a repeated phrase.
        weight = 3 if is_heading else 1
        uni[token] += weight
        prev = stream[index - 1] if index else None
        if prev is not None:
            pair = (prev[0], token)
            bi[pair] += weight
            bi_raw[pair] += 1
            partner_hits[(token, prev[0])] += 1
            partner_hits[(prev[0], token)] += 1
        if prev is not None and index >= 2 and stream[index - 2] is not None:
            triple = (stream[index - 2][0], prev[0], token)
            tri[triple] += weight
            tri_raw[triple] += 1

    bundles: dict[str, float] = {}
    for gram, raw in bi_raw.items():
        if raw >= 2:
            bundles[" ".join(gram)] = float(bi[gram])
    for gram, raw in tri_raw.items():
        if raw >= 2:
            bundles[" ".join(gram)] = float(tri[gram])

    # A word is absorbed when one neighbor sits beside at least three quarters
    # of its occurrences. ``transportation`` beside ``space`` qualifies.
    # ``space`` beside several different neighbors does not.
    occurrences = Counter(token for token, _heading in (item for item in stream if item))
    best_partner: Counter[str] = Counter()
    for (word, _partner), hits in partner_hits.items():
        if hits > best_partner[word]:
            best_partner[word] = hits
    absorbed_set = {
        token for token, count in occurrences.items()
        if count >= 2 and best_partner[token] >= 0.75 * count
    }

    terms: dict[str, float] = {phrase: count for phrase, count in bundles.items()}
    for token, count in uni.items():
        if token in absorbed_set:
            continue
        terms[token] = float(count)
    return Prepared(
        terms=terms,
        bundles=sorted(bundles, key=lambda phrase: -bundles[phrase])[:40],
        absorbed=sorted(absorbed_set),
        tokens=kept,
        heading=sorted(heading)[:8],
        sentences=[sentence for sentence in sentences if sentence],
    )


def stock_terms(text: str, name: str = "") -> set[str]:
    """Same preparation as an article, plus the company name even if it appears once."""
    terms = set(prepare(text).terms)
    alias = _alias(name)
    if alias:
        terms.add(alias)
        for part in alias.split():
            token = inflection(part)
            if _keep_token(token):
                terms.add(token)
    return terms


@dataclass
class Stock:
    ticker: str
    name: str
    industry: str
    sector: str
    terms: set[str]
    alias: str
    doc_len: int
    partners: list[dict] = field(default_factory=list)


@dataclass
class Board:
    stocks: list[Stock]
    weight: dict[str, float]
    industry_of_term: dict[str, str]
    df: dict[str, int]
    avg_len: float
    n: int
    postings: dict[str, list[int]] = field(default_factory=dict)


def _alias(name: str) -> str:
    cleaned = re.sub(
        r"\b(incorporated|inc|corp|corporation|ltd|limited|plc|co|company|group|holdings|holding)\b",
        " ",
        name or "",
        flags=re.I,
    )
    cleaned = re.sub(r"[^A-Za-z0-9 ]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    if len(cleaned) < 4:
        return ""
    return cleaned


def build_board(rows: list[dict]) -> Board:
    stocks: list[Stock] = []
    for row in rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        industry = str(row.get("industry") or "").strip()
        if not ticker or not industry or industry.lower() in {"nan", "none"}:
            continue
        filing_core, partners = divide_profile(str(row.get("filing") or ""))
        text = " ".join(
            part for part in (
                str(row.get("description") or ""),
                filing_core,
            ) if part.strip()
        )
        terms = stock_terms(text, str(row.get("name") or ""))
        if len(terms) < 4:
            continue
        stocks.append(Stock(
            ticker=ticker,
            name=str(row.get("name") or "").strip(),
            industry=industry,
            sector=str(row.get("sector") or "").strip(),
            terms=terms,
            alias=_alias(str(row.get("name") or "")),
            doc_len=max(1, sum(1 for term in terms if " " not in term)),
            partners=partners,
        ))
    n = max(len(stocks), 1)
    df: Counter[str] = Counter()
    by_industry: dict[str, Counter[str]] = defaultdict(Counter)
    industry_size: Counter[str] = Counter(stock.industry for stock in stocks)
    for stock in stocks:
        df.update(stock.terms)
        by_industry[stock.industry].update(stock.terms)
    weight: dict[str, float] = {}
    industry_of_term: dict[str, str] = {}
    for term, count in df.items():
        idf = math.log((n + 1) / (count + 1)) + 1.0
        shares: Counter[str] = Counter()
        for industry, counts in by_industry.items():
            if counts[term]:
                shares[industry] = counts[term]
        top_industry, top_count = shares.most_common(1)[0]
        # Purity: of the stocks that say it, how many sit in one industry.
        # Coverage: what fraction of that industry says it. A phrase that
        # appears in a single 10-K ("steps down") is pure and still covers
        # almost none of its industry, so it stays a large, weak circle.
        purity = top_count / count
        coverage = top_count / industry_size[top_industry]
        weight[term] = idf * purity * coverage
        if purity >= 0.55 and (top_count >= 3 or coverage >= 0.25):
            industry_of_term[term] = top_industry
    avg_len = sum(stock.doc_len for stock in stocks) / n
    postings: dict[str, list[int]] = defaultdict(list)
    for index, stock in enumerate(stocks):
        for term in stock.terms:
            postings[term].append(index)
    return Board(stocks, weight, industry_of_term, dict(df), avg_len, n, dict(postings))


def _apply_keyness(prepared: Prepared, background: Counter[str], background_tokens: int) -> dict[str, float]:
    if background_tokens <= 0:
        return dict(prepared.terms)
    kept: dict[str, float] = {}
    doc_tokens = max(prepared.tokens, 1)
    for term, count in prepared.terms.items():
        keyness = (
            math.log((count + 0.5) / (doc_tokens + 0.5))
            - math.log((background[term] + 0.5) / (background_tokens + 0.5))
        )
        # A bundle that shows up across the batch ("steps down") is a large
        # circle too. Only terms rarer in the other articles survive.
        # Equal rates still look slightly positive because of the 0.5 prior.
        # Require the term to be clearly denser here than in the other articles.
        if keyness < 0.5:
            continue
        kept[term] = count * (1.0 + keyness)
    return kept


def bm25(query: dict[str, float], stock: Stock, board: Board) -> float:
    score = 0.0
    k1, b = 1.2, 0.75
    for term in query:
        if term not in stock.terms:
            continue
        df = board.df.get(term, 0)
        idf = math.log(1.0 + (board.n - df + 0.5) / (df + 0.5))
        tf = 1.0
        denom = tf + k1 * (1 - b + b * stock.doc_len / board.avg_len)
        score += idf * (tf * (k1 + 1) / denom)
    return score


def _industry_confirmed(term: str, industry: str, sentences: list[list[str]], board: Board) -> bool:
    """A bare word votes for an industry only when the same sentence has another word from it.

    ``launch`` beside ``spacecraft`` votes aerospace. ``launch`` beside
    ``trading venue`` does not.
    """
    if " " in term:
        return True
    for sentence in sentences:
        if term not in sentence:
            continue
        for other in sentence:
            if other != term and board.industry_of_term.get(other) == industry:
                return True
    return False


def _industry_scores(query: dict[str, float], board: Board, sentences: list[list[str]]) -> dict[str, float]:
    scores: Counter[str] = Counter()
    for term, importance in query.items():
        industry = board.industry_of_term.get(term)
        if not industry:
            continue
        circle = board.weight.get(term) or 0.0
        if circle < 0.35:
            continue
        if not _industry_confirmed(term, industry, sentences, board):
            continue
        scores[industry] += importance * circle
    return dict(scores)


def _z(value: float, mean: float, std: float) -> float:
    if std <= 1e-9:
        return 0.0
    return (value - mean) / std


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    var = sum((value - mean) ** 2 for value in values) / len(values)
    return mean, math.sqrt(var)


def _dependencies(board: Board, text: str) -> list[dict]:
    found = []
    for stock in board.stocks:
        for partner in stock.partners:
            if re.search(rf"\b{re.escape(partner['name'])}\b", text or "", re.I):
                found.append({
                    "ticker": stock.ticker,
                    "name": stock.name,
                    "role": partner["role"],
                    "partner": partner["name"],
                    "section": partner.get("section") or "",
                })
    return found


def name_hit(stock: Stock, text: str) -> bool:
    # One-letter tickers match initials inside "U.S." and are not names.
    if len(stock.ticker) >= 2 and re.search(rf"\b{re.escape(stock.ticker)}\b", text or ""):
        return True
    if stock.alias and re.search(rf"\b{re.escape(stock.alias)}\b", (text or "").lower()):
        return True
    return False


def link_articles(
    board: Board,
    articles: list[dict],
    *,
    z_min: float = 2.5,
    batch_z_min: float = 2.0,
    reference: list[dict] | None = None,
) -> list[dict]:
    """Score every article. Both gates must clear ``z_min`` inside the article.

    On a batch of at least eight articles, a pair must also clear
    ``batch_z_min`` against that stock's and that industry's other scores.

    ``reference`` is the control corpus for keyness. A handful of articles
    must not be their own control: a verb that appears once would look rare.
    Pass the day's news. When omitted, the articles being scored are the control.
    """
    prepared = [prepare(str(article.get("text") or "")) for article in articles]
    if reference is None:
        ref_docs = prepared
    else:
        ref_docs = [prepare(str(item.get("text") or "")) for item in reference]
    background: Counter[str] = Counter()
    background_tokens = 0
    for doc in ref_docs:
        background.update(doc.terms)
        background_tokens += doc.tokens
    queries: list[dict[str, float]] = []
    for doc in prepared:
        # Leave this document out of its own background so a repeated bundle
        # is not punished for being common inside itself.
        own = Counter(doc.terms)
        bg_tokens = background_tokens - doc.tokens
        # Keyness needs other articles. A single document has no control corpus,
        # and an empty background makes every ordinary word look rare-in-the-wrong-way.
        if bg_tokens < 30:
            queries.append(dict(doc.terms))
        else:
            queries.append(_apply_keyness(doc, background - own, bg_tokens))

    raw: list[dict] = []
    for article, doc, query in zip(articles, prepared, queries):
        text = str(article.get("text") or "")
        if not doc.known or len(query) < 3:
            raw.append({
                "id": article.get("id"),
                "title": article.get("title") or "",
                "url": article.get("url") or "",
                "source": article.get("source") or "",
                "status": "unknown",
                "bundles": doc.bundles[:12],
                "absorbed": doc.absorbed[:12],
                "industries": [],
                "stocks": [],
                "dependencies": _dependencies(board, text),
            })
            continue
        ind_scores = _industry_scores(query, board, doc.sentences)
        every_industry = {stock.industry for stock in board.stocks}
        ind_mean, ind_std = _mean_std([ind_scores.get(name, 0.0) for name in every_industry])
        industries = []
        passing_industries = set()
        for industry, score in sorted(ind_scores.items(), key=lambda item: -item[1]):
            z = _z(score, ind_mean, ind_std)
            passed = score > 0 and z >= z_min
            if passed:
                passing_industries.add(industry)
            industries.append({
                "industry": industry,
                "score": round(score, 3),
                "z": round(z, 3),
                "pass": passed,
            })
        totals = [0.0] * len(board.stocks)
        hit_terms: list[list[tuple[float, str]]] = [[] for _ in board.stocks]
        for term, importance in query.items():
            circle = board.weight.get(term)
            # A phrase that lives in one filing covers too little of its
            # industry to be a circle. Those weights sit near zero and would
            # otherwise look like a perfect match on a short headline.
            if not circle or circle < 0.35:
                continue
            gained = importance * circle
            for index in board.postings.get(term, ()):
                totals[index] += gained
                hit_terms[index].append((gained, term))
        stocks_out = []
        sparse: dict[str, float] = {}
        for index, stock in enumerate(board.stocks):
            score = totals[index]
            if score <= 0:
                continue
            length_prior = 1.0 + 0.15 * math.log(1.0 + stock.doc_len)
            score /= length_prior
            sparse[stock.ticker] = score
            hits = [term for _, term in sorted(hit_terms[index], reverse=True)[:8]]
            stocks_out.append({
                "ticker": stock.ticker,
                "name": stock.name,
                "industry": stock.industry,
                "sector": stock.sector,
                "desc_score": round(score, 3),
                "bm25": round(bm25(query, stock, board), 3),
                "terms": hits,
                "name_hit": name_hit(stock, text),
                "ind_pass": stock.industry in passing_industries,
            })
        adjusted = [row["desc_score"] for row in stocks_out]
        # Stocks with no overlap are zeros and belong in the z baseline.
        zeros = [0.0] * (len(board.stocks) - len(adjusted))
        desc_mean, desc_std = _mean_std(adjusted + zeros)
        for row in stocks_out:
            row["desc_z"] = round(_z(row["desc_score"], desc_mean, desc_std), 3)
            row["desc_pass"] = row["desc_z"] >= z_min and bool(row["terms"])
            row["pass"] = row["desc_pass"] and row["ind_pass"]
        seen = {row["ticker"] for row in stocks_out}
        for stock in board.stocks:
            if stock.ticker in seen or not name_hit(stock, text):
                continue
            stocks_out.append({
                "ticker": stock.ticker,
                "name": stock.name,
                "industry": stock.industry,
                "sector": stock.sector,
                "desc_score": 0.0,
                "bm25": 0.0,
                "terms": [],
                "name_hit": True,
                "ind_pass": stock.industry in passing_industries,
                "desc_z": 0.0,
                "desc_pass": False,
                "pass": False,
            })
        stocks_out.sort(key=lambda row: (-int(row["pass"]), -int(row["name_hit"]), -row["desc_z"], -row["desc_score"]))
        raw.append({
            "id": article.get("id"),
            "title": article.get("title") or "",
            "url": article.get("url") or "",
            "source": article.get("source") or "",
            "status": "scored",
            "bundles": doc.bundles[:12],
            "absorbed": doc.absorbed[:12],
            "industries": [row for row in industries if row["pass"] or row["z"] > 0][:8],
            "stocks": [row for row in stocks_out if row["pass"] or row["name_hit"] or row["desc_z"] >= z_min][:24],
            "dependencies": _dependencies(board, text),
            "_sparse": sparse,
            "_ind": {row["industry"]: row["score"] for row in industries},
        })

    scored_n = sum(1 for item in raw if item["status"] == "scored")
    if scored_n >= 8:
        sum_t: Counter[str] = Counter()
        sumsq_t: Counter[str] = Counter()
        sum_i: Counter[str] = Counter()
        sumsq_i: Counter[str] = Counter()
        for item in raw:
            if item["status"] != "scored":
                continue
            for ticker, score in item["_sparse"].items():
                sum_t[ticker] += score
                sumsq_t[ticker] += score * score
            for industry, score in item["_ind"].items():
                sum_i[industry] += score
                sumsq_i[industry] += score * score

        def base(total: float, sq: float) -> tuple[float, float]:
            mean = total / scored_n
            var = max(sq / scored_n - mean * mean, 0.0)
            return mean, math.sqrt(var)

        for item in raw:
            if item["status"] != "scored":
                continue
            kept = []
            for row in item["stocks"]:
                mean, std = base(sum_t[row["ticker"]], sumsq_t[row["ticker"]])
                row["batch_z"] = round(_z(row["desc_score"], mean, std), 3)
                imean, istd = base(sum_i[row["industry"]], sumsq_i[row["industry"]])
                ind_score = item["_ind"].get(row["industry"], 0.0)
                row["ind_batch_z"] = round(_z(ind_score, imean, istd), 3)
                row["pass"] = bool(
                    row["desc_pass"] and row["ind_pass"]
                    and row["batch_z"] >= batch_z_min
                    and row["ind_batch_z"] >= batch_z_min
                )
                if row["pass"] or row["batch_z"] >= batch_z_min:
                    kept.append(row)
            kept.sort(key=lambda row: (-int(row["pass"]), -row["batch_z"], -row["desc_z"]))
            item["stocks"] = kept[:12]

    for item in raw:
        item.pop("_sparse", None)
        item.pop("_ind", None)
    return raw
