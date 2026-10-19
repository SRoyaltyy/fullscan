"""Frozen evaluation harness for the article <-> stock correlation.

Runs eval/cases.jsonl against the scorer — either in-process (needs the
filings inputs) or from an existing links JSON — classifies every failure
with the fixed taxonomy in eval/rubric.md, and writes a round result that
src/eval_diff.py diffs against the previous round.

The comparator (compare_case and below) is pure and has no dependency on
src.correlation, so the harness tests run without the filings archive.

Run from the repo root:
    PYTHONPATH=. python3 -m src.eval_correlation \
        --cases eval/cases.jsonl \
        --descriptions excel_bot/data/finviz_with_descriptions.csv \
        --universe /path/to/company_filings_full_dates.csv \
        --filings /path/to/business.json \
        --reference data/news_intake/2026-10-17/documents.json \
        --out eval/results/round-001.json

or against an existing program output:
    PYTHONPATH=. python3 -m src.eval_correlation \
        --cases eval/cases.jsonl \
        --links dashboard/news-dir/correlation/latest.json \
        --out eval/results/round-001.json
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

TAXONOMY_VERSION = "FT-2"

FAILURE_CLASSES = {
    "ALIAS_FP": "common word / publication / org name treated as a ticker or alias",
    "RELATION_LEAK": "partner/competitor mention scored as topical, or dependency returned without second-order marking/gating",
    "GATE_MISS": "correct stock present in output but the industry gate failed it",
    "SPELLING_MISS": "spelling variant (defence/defense) blocked an otherwise valid match",
    "SENSE_CROSS": "homonym crossed industries",
    "THRESHOLD": "right stock ranked but the z/percentile cut rejected it",
    "FALSE_PASS": "a stock passed that must not pass",
    "FALSE_UNKNOWN": "article should have scored or named a company but came back unknown/absent",
    "MISSING_NAME": "expected ticker/alias name hit is absent",
    "SCHEMA_DRIFT": "program output lacks a field the case needs",
    "NO_TEXT": "case has no scorable text",
}

VERDICTS = ("PASS", "FAIL", "TARGET_ACHIEVED", "TARGET_FAIL", "SKIP", "UNLABELED")


# --------------------------------------------------------------------------
# Comparator (pure — no src.correlation import)
# --------------------------------------------------------------------------

def _stock_row(hit: dict, ticker: str) -> dict | None:
    for row in (hit or {}).get("stocks") or []:
        if str(row.get("ticker") or "").upper() == ticker.upper():
            return row
    return None


def _dependencies(hit: dict) -> list[dict]:
    return list((hit or {}).get("dependencies") or [])


def _is_second_order(dep: dict) -> bool:
    """A dependency counts as second-order only when the program marks it."""
    if dep.get("second_order") is True:
        return True
    try:
        return int(dep.get("order", 1)) >= 2
    except (TypeError, ValueError):
        return False


def compare_case(case: dict, hit: dict | None) -> dict:
    """Compare one case's expectations against the program output for it.

    ``hit`` is the link_articles row for this article, or None when the
    program produced no row with this id. Never raises on schema drift —
    drift becomes SCHEMA_DRIFT in the verdict.
    """
    expected = case.get("expected") or {}
    classes: list[str] = []
    notes: list[str] = []
    missing: dict[str, list] = {"topical_passes": [], "name_hits": []}
    unexpected: dict[str, list] = {"passes": [], "name_hits": [], "dependencies": []}
    kind = case.get("kind") or "regression"

    if not expected:
        return {
            "case_id": case.get("id"), "kind": kind, "case_type": case.get("case_type"),
            "verdict": "UNLABELED", "failure_classes": [], "notes": ["no expectations yet"],
            "program_status": (hit or {}).get("status"), "missing": missing, "unexpected": unexpected,
        }

    text = (case.get("text") or "").strip()
    # Short text is only un-scorable when the case needs a body to be judged.
    # A case whose expectation is "unknown" (thin-markets-moved is 14 characters)
    # is legitimately short: the program must still see it and say unknown.
    needs_body = (
        expected.get("status") == "scored"
        or bool(expected.get("topical_passes"))
        or bool(expected.get("name_hits"))
        or bool(expected.get("allowed_second_order"))
    )
    if len(text) < 40 and not case.get("text_ref") and needs_body:
        classes.append("NO_TEXT")
        notes.append("case text is empty/short and text_ref is unset; paste full text")

    if hit is None:
        classes.append("FALSE_UNKNOWN")
        notes.append("no program output row with this case id")
    else:
        rows = hit.get("stocks") or []
        passing = [r for r in rows if r.get("pass")]
        named = [r for r in rows if r.get("name_hit")]
        # A complete label is an exact set, including every extra false link.
        # Existing partial bug assertions stay usable but cannot certify accuracy.
        if case.get("truth_complete") or expected.get("exact_topical"):
            truth = {str(t).upper() for t in expected.get("topical_passes", [])}
            actual = {str(r.get("ticker", "")).upper() for r in passing}
            extra = sorted(actual - truth)
            if extra:
                classes.append("FALSE_PASS")
                unexpected["passes"].extend(extra)
        for row in rows:
            if not isinstance(row.get("ticker"), str) or type(row.get("pass")) is not bool:
                classes.append("SCHEMA_DRIFT")

        # status ------------------------------------------------------------
        want_status = expected.get("status")
        got_status = hit.get("status")
        if want_status and got_status != want_status:
            if want_status == "scored" and got_status == "unknown":
                classes.append("FALSE_UNKNOWN")
                notes.append(f"expected scored, program said unknown")
            elif want_status == "unknown" and got_status == "scored":
                if passing or named:
                    classes.append("FALSE_PASS")
                    notes.append("expected unknown but program scored and linked")
                else:
                    notes.append("status scored with no passes/name hits; accepted")

        # no-pass / no-name assertions --------------------------------------
        if expected.get("no_topical_passes") and passing:
            classes.append("FALSE_PASS")
            unexpected["passes"] = [r.get("ticker") for r in passing]
        if expected.get("no_name_hits") and named:
            classes.append("ALIAS_FP")
            unexpected["name_hits"] = [r.get("ticker") for r in named]
            notes.append("name hit(s) where the text names no company")

        # topical passes ------------------------------------------------------
        for ticker in expected.get("topical_passes") or []:
            row = _stock_row(hit, ticker)
            if row and row.get("pass"):
                continue
            missing["topical_passes"].append(ticker)
            if row is None:
                classes.append("FALSE_UNKNOWN")
                notes.append(f"{ticker} absent from output")
            elif row.get("ind_pass") is False:
                classes.append("GATE_MISS")
                notes.append(f"{ticker} present but industry gate failed (desc_pass={row.get('desc_pass')})")
            else:
                classes.append("THRESHOLD")
                notes.append(f"{ticker} present, desc_pass={row.get('desc_pass')}, ind_pass={row.get('ind_pass')}")

        for ticker in expected.get("must_not_pass") or []:
            row = _stock_row(hit, ticker)
            if row and row.get("pass"):
                classes.append("FALSE_PASS")
                unexpected["passes"].append(ticker)

        # name hits -----------------------------------------------------------
        for ticker in expected.get("name_hits") or []:
            row = _stock_row(hit, ticker)
            if not (row and row.get("name_hit")):
                classes.append("MISSING_NAME")
                missing["name_hits"].append(ticker)

        for ticker in expected.get("must_not_name") or []:
            row = _stock_row(hit, ticker)
            if row and row.get("name_hit"):
                classes.append("ALIAS_FP")
                unexpected["name_hits"].append(ticker)

        # alias graph and jurisdiction scope ----------------------------------
        for ticker in expected.get("alias_hits") or []:
            row = _stock_row(hit, ticker)
            if not (row and row.get("alias_hit")):
                classes.append("MISSING_NAME")
                missing["name_hits"].append(ticker)
        scoped_tickers = {str(ticker).upper() for ticker in (hit.get("scoped") or [])}
        for ticker in expected.get("scoped") or []:
            row = _stock_row(hit, ticker)
            if ticker.upper() in scoped_tickers or (row and row.get("jurisdiction_hit")):
                continue
            classes.append("FALSE_UNKNOWN")
            notes.append(f"{ticker} was not scoped to a jurisdiction named in the article")
        passed_industries = {
            row.get("industry") for row in (hit.get("industries") or []) if row.get("pass")
        }
        for industry in expected.get("industries_pass") or []:
            if industry not in passed_industries:
                classes.append("GATE_MISS")
                notes.append(f"industry {industry} did not pass")
        for industry in expected.get("industries_must_not") or []:
            if industry in passed_industries:
                classes.append("SENSE_CROSS")
                notes.append(f"industry {industry} passed")

        # dependencies ---------------------------------------------------------
        allowed = {(a.get("ticker", "").upper(), (a.get("partner") or "").lower())
                   for a in expected.get("allowed_second_order") or []}
        forbidden = {t.upper() for t in expected.get("forbidden_dependencies") or []}
        for dep in _dependencies(hit):
            dticker = str(dep.get("ticker") or "").upper()
            dpartner = str(dep.get("partner") or "").lower()
            if dticker in forbidden:
                classes.append("RELATION_LEAK")
                unexpected["dependencies"].append(f"{dticker}:{dep.get('partner')}")
            elif allowed and (dticker, dpartner) in allowed:
                if not _is_second_order(dep):
                    classes.append("RELATION_LEAK")
                    unexpected["dependencies"].append(f"{dticker}:{dep.get('partner')} (ungated)")
            elif allowed:
                # A dependency outside the allowed list on a case that gates
                # its relations is treated as ungated surfacing.
                classes.append("RELATION_LEAK")
                unexpected["dependencies"].append(f"{dticker}:{dep.get('partner')} (not in allowed list)")

    # dedupe, preserving order
    seen = set()
    classes = [c for c in classes if not (c in seen or seen.add(c))]

    if not classes:
        verdict = "TARGET_ACHIEVED" if kind == "target" else "PASS"
    else:
        verdict = "TARGET_FAIL" if kind == "target" else "FAIL"
    if "NO_TEXT" in classes:
        verdict = "SKIP"

    return {
        "case_id": case.get("id"),
        "kind": kind,
        "case_type": case.get("case_type"),
        "verdict": verdict,
        "failure_classes": classes,
        "notes": notes,
        "program_status": (hit or {}).get("status"),
        "missing": missing,
        "unexpected": unexpected,
    }


def summarize(results: list[dict]) -> dict:
    verdicts = Counter(r["verdict"] for r in results)
    classes = Counter()
    for row in results:
        if row["verdict"] in ("FAIL", "TARGET_FAIL"):
            classes.update(row["failure_classes"])
    # Only labeled, scored cases count toward the rate; UNLABELED and SKIP
    # cases are corpus hygiene, not signal.
    regression = [r for r in results if r["kind"] == "regression" and r["verdict"] in ("PASS", "FAIL")]
    reg_pass = sum(1 for r in regression if r["verdict"] == "PASS")
    targets = [r for r in results if r["kind"] == "target"]
    tgt_hit = sum(1 for r in targets if r["verdict"] == "TARGET_ACHIEVED")
    return {
        "cases": len(results),
        "verdicts": dict(verdicts),
        "failure_classes": dict(classes),
        "regression_pass_rate": round(reg_pass / len(regression), 3) if regression else None,
        "targets_achieved": f"{tgt_hit}/{len(targets)}" if targets else "0/0",
        "unlabeled": verdicts.get("UNLABELED", 0),
    }


# --------------------------------------------------------------------------
# Loading / running
# --------------------------------------------------------------------------

def load_cases(path: Path) -> list[dict]:
    cases = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            cases.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{line_number}: bad JSON: {exc}")
    return cases


def _index_links(payload) -> dict[str, dict]:
    rows = payload.get("links") if isinstance(payload, dict) else payload
    return {str(row.get("id")): row for row in rows or [] if isinstance(row, dict)}


def run_against_links(cases: list[dict], links_path: Path) -> dict[str, dict]:
    payload = json.loads(links_path.read_text(encoding="utf-8"))
    return _index_links(payload)


def run_in_process(cases: list[dict], args: argparse.Namespace) -> dict[str, dict]:
    from src.correlation import build_board, link_articles  # lazy: needs repo + inputs
    from src.link_correlation import load_articles, load_stocks

    if getattr(args,'profile_cache',None):
        if getattr(args,'engine','evidence')!='evidence':
            raise ValueError('--profile-cache supports only the evidence engine')
        from src.profile_cache import load_profiles
        from src.link_correlation import _rows
        wanted=None
        if getattr(args,'universe',None):
            wanted={t.upper() for row in _rows(args.universe) for t in str(row.get('tickers') or row.get('ticker') or '').split()}
        evidence,_ = load_profiles(args.profile_cache,as_of=getattr(args,'as_of',None),tickers=wanted)
        stocks=[{'ticker':p.ticker,'name':p.name,'industry':p.industry,'sector':p.sector} for p in evidence.profiles.values()]
        board=build_board(stocks,engine='evidence');board.evidence_board=evidence
        articles=[{'id':c['id'],'title':c.get('title',''),'text':c.get('text',''),
                   'published_at':c.get('published_at') or c.get('date','')} for c in cases]
        from src.names import NameIndex,catalog_from_csv
        names=NameIndex(catalog_from_csv(args.descriptions)) if getattr(args,'descriptions',None) and args.descriptions.is_file() else None
        return {str(h['id']):h for h in link_articles(board,articles,engine='evidence',names=names)}

    filings: dict[str, str] = {}
    if args.filings and args.filings.is_file() and args.filings.suffix == ".json":
        filings = {str(k).upper(): v for k, v in json.loads(args.filings.read_text()).items()}
    elif args.filings and args.filings.is_dir():
        from src.filing_evidence import load_filing_sections
        filings, _ = load_filing_sections(args.filings,as_of=getattr(args,"as_of",None))

    from src.aliases import profiles_from_csv
    from src.names import NameIndex, catalog_from_csv

    stocks = load_stocks(args.descriptions, args.universe, args.sectors, filings)
    if getattr(args,"section_cache",None):
        from src.filing_evidence import load_section_cache
        cached,_ = load_section_cache(args.section_cache,as_of=getattr(args,"as_of",None))
        sections={r["ticker"]:r["filing_sections"] for r in cached}
        for row in stocks:
            row["filing_sections"]=sections.get(row["ticker"],[])
    board = build_board(stocks,engine=getattr(args,"engine","evidence"))
    # Names, aliases, and jurisdictions come from the whole description file,
    # not the liquidity screen, so an off-board company can still be inspected.
    names = NameIndex(catalog_from_csv(args.descriptions))
    profiles = profiles_from_csv(args.descriptions)

    reference = None
    if args.reference and args.reference.is_file():
        reference = load_articles(args.reference, limit=0)

    articles = [{
        "id": case["id"],
        "title": case.get("title") or "",
        "url": case.get("source_url") or "",
        "source": "eval-corpus",
        "text": case.get("text") or "",
        "published_at": case.get("published_at") or case.get("date") or "",
    } for case in cases]
    # Use the production path and defaults, including the legacy batch gate
    # when requesting the legacy comparison engine.
    hits = link_articles(
        board, articles, z_min=args.z_min, batch_z_min=args.batch_z_min,
        reference=reference, names=names, profiles=profiles, engine=getattr(args,"engine","evidence"),
    )
    return {str(hit.get("id")): hit for hit in hits}


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate correlation output against the frozen corpus")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--engine", choices=("evidence", "legacy"), default="evidence")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--links", type=Path, default=None,
                        help="Existing program output JSON instead of running in-process")
    parser.add_argument("--descriptions", type=Path, default=None)
    parser.add_argument("--universe", type=Path, default=None)
    parser.add_argument("--sectors", type=Path, default=None)
    parser.add_argument("--filings", type=Path, default=None)
    parser.add_argument("--section-cache", type=Path, default=None)
    parser.add_argument("--profile-cache", type=Path, default=None)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--reference", type=Path, default=None,
                        help="documents.json used as the keyness control corpus")
    parser.add_argument("--z-min", type=float, default=2.5)
    parser.add_argument("--batch-z-min", type=float, default=2.0)
    parser.add_argument("--changelog", type=str, default="",
                        help="What changed since the last round; recorded in the result")
    args = parser.parse_args()

    cases = load_cases(args.cases)
    if args.links:
        hits_by_id = run_against_links(cases, args.links)
        config = {"links": str(args.links)}
    else:
        if not args.descriptions and not args.profile_cache:
            raise SystemExit("in-process run needs --descriptions (or use --links)")
        hits_by_id = run_in_process(cases, args)
        config = {
            "descriptions": str(args.descriptions), "universe": str(args.universe or ""),
            "sectors": str(args.sectors or ""), "filings": str(args.filings or ""),
            "reference": str(args.reference or ""), "z_min": args.z_min,
            "batch_z_min": args.batch_z_min,
            "engine":args.engine,"profile_cache":str(args.profile_cache or ''),"as_of":args.as_of,
        }

    results = [compare_case(case, hits_by_id.get(str(case.get("id")))) for case in cases]
    payload = {
        "taxonomy": TAXONOMY_VERSION,
        "generated": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "config": config,
        "changelog": args.changelog,
        "summary": summarize(results),
        "cases": results,
    }
    from .link_metrics import score_links
    payload["strict_metrics"] = score_links(cases, hits_by_id)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    print(f"WROTE={args.out}")


if __name__ == "__main__":
    main()

