"""Strict set-based relevance metrics. Unknown never hides a positive miss."""
from __future__ import annotations

from collections import Counter
import math


def wilson(successes: int, total: int, z: float = 1.96) -> list[float] | None:
    if not total:
        return None
    p = successes/total
    d = 1 + z*z/total
    center = (p + z*z/(2*total))/d
    radius = z*math.sqrt(p*(1-p)/total + z*z/(4*total*total))/d
    return [round(max(0,center-radius),4),round(min(1,center+radius),4)]


def score_links(cases: list[dict], hits: dict[str,dict], *, minimum: float = 0.90) -> dict:
    counts = Counter()
    details = []
    by_kind = {}
    eligible = []
    for case in cases:
        expected = case.get('expected') or {}
        # Partial bug assertions remain useful regressions, but cannot certify
        # accuracy over an unknown complete expected answer.
        if not expected or not case.get('truth_complete',False):
            counts['excluded_partial_or_unlabeled'] += 1
            continue
        hit = hits.get(str(case['id']))
        actual = {r['ticker'] for r in (hit or {}).get('stocks',[]) if r.get('pass') is True}
        truth = set(expected.get('topical_passes') or [])
        tp,fp,fn = len(actual & truth),len(actual-truth),len(truth-actual)
        abstained = hit is None or (hit.get('decision') == 'insufficient_text')
        # A missing output is always a failed case, including negative cases.
        exact = hit is not None and actual == truth
        counts.update({'tp':tp,'fp':fp,'fn':fn,'cases':1,'exact':int(exact),
            'positive_cases':int(bool(truth)),'positive_exact':int(bool(truth) and exact),
            'negative_cases':int(not truth),'negative_exact':int(not truth and exact),
            'abstained':int(abstained),'missing_outputs':int(hit is None)})
        kind = case.get('case_type','unspecified')
        item = by_kind.setdefault(kind,Counter())
        item.update({'cases':1,'exact':int(exact),'tp':tp,'fp':fp,'fn':fn})
        details.append({'id':case['id'],'correct':exact,'expected':sorted(truth),'actual':sorted(actual),
            'false_links':sorted(actual-truth),'missed_links':sorted(truth-actual)})
        eligible.append(case)
    def ratio(a,b):
        return round(a/b,6) if b else None
    precision = ratio(counts['tp'],counts['tp']+counts['fp'])
    recall = ratio(counts['tp'],counts['tp']+counts['fn'])
    accuracy = ratio(counts['exact'],counts['cases'])
    # No all-negative or no-prediction benchmark can satisfy a relevance bar.
    reached = all(v is not None and v >= minimum for v in (precision,recall,accuracy)) and counts['positive_cases'] >= 20
    all_checked = bool(eligible) and all(c.get('labels',{}).get('human_checked') is True for c in eligible)
    return {'counts':dict(counts),'precision':precision,'recall':recall,'exact_set_accuracy':accuracy,
        'positive_case_accuracy':ratio(counts['positive_exact'],counts['positive_cases']),
        'negative_case_accuracy':ratio(counts['negative_exact'],counts['negative_cases']),
        'abstention_rate':ratio(counts['abstained'],counts['cases']),
        'accuracy_95pct_interval':wilson(counts['exact'],counts['cases']),
        'precision_95pct_interval':wilson(counts['tp'],counts['tp']+counts['fp']),
        'recall_95pct_interval':wilson(counts['tp'],counts['tp']+counts['fn']),
        'threshold':minimum,'sample_bar_reached':reached,'human_validated':all_checked,
        'production_accuracy_certified':False,
        'by_case_type':{kind:dict(n) for kind,n in sorted(by_kind.items())},'details':details}
