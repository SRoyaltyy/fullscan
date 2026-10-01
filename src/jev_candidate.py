"""Frozen candidate learned on exposed development data, never on acceptance gold.

Taxonomy supplies a broad view; atomic questions recover factual developments.
A separate pure-noise answer resolves wrappers. Probabilities are not multiplied.
"""
import math,re
from .jev_experiments import VARIANTS
from .jev_acceptance import sha,RUBRIC
QUESTIONS={**VARIANTS['taxonomy'],**VARIANTS['atomic']}
VERSION='taxonomy-atomic-v1'
NEWS_THRESHOLD=.90
FACT_THRESHOLD=.70
NOISE_THRESHOLD=.40
ARTIFACT=re.compile(r'(?i)earnings call (?:highlights|transcript)|stock price,? news,? quote|quote (?:and |& )?history|rings? (?:the )?.{0,25}opening bell|\bmorning bid:|\bstock movers\b')
PREVIEW=re.compile(r'(?i)\b(?:set to |will |to |expected to )report\b.{0,65}\bearnings\b|\bearnings preview\b|\bhow much\b.{0,100}\bstock\b.{0,60}\b(?:could|expected to) move\b')
WRAP=re.compile(r'(?i)stock market (?:today|midday|update|news)')
INDEX=re.compile(r'(?i)\b(?:dow|nasdaq|s&p|sensex|nifty|nikkei|ftse|asx|indexes|indices|futures)\b')

def hard_noise(title):
    if ARTIFACT.search(title):return 'artifact'
    if PREVIEW.search(title):return 'earnings_preview'
    if WRAP.search(title) and INDEX.search(title):return 'broad_market_wrap'
    return ''

def protocol_sha():
    return sha({'rubric':RUBRIC,'questions':QUESTIONS,'version':VERSION,
                'thresholds':[NEWS_THRESHOLD,FACT_THRESHOLD,NOISE_THRESHOLD],
                'rules':[r.pattern for r in (ARTIFACT,PREVIEW,WRAP,INDEX)],
                'formula':'noise<cutoff AND (news_mass>=cutoff OR max_atomic>=cutoff); hard_noise veto'})

def decide(row,payload):
    answers=payload['answers']
    for key,question in QUESTIONS.items():
        answer=answers[key]
        if answer.get('type')!=question['type']:raise ValueError('wrong answer type')
        if question['type']=='choice':
            values=answer['probabilities']
            if set(values)!=set(question['criteria']) or abs(sum(values.values())-1)>.02:raise ValueError('incomplete probabilities')
            if answer.get('choice') not in values or values[answer['choice']]<max(values.values()):raise ValueError('choice mismatch')
            values=list(values.values())
        else:values=[answer['noul']]
        if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not 0<=v<=1 for v in values):raise ValueError('invalid probabilities')
    news=1-answers['kind']['probabilities']['noise']
    fact=max(answers[k]['noul'] for k in ('company','macro','industry'))
    noise=answers['noise']['noul']
    veto=hard_noise(row['title'])
    keep=not veto and noise<NOISE_THRESHOLD and (news>=NEWS_THRESHOLD or fact>=FACT_THRESHOLD)
    return {**{k:row.get(k,'') for k in ('title','source','published_at','url','id')},
            'decision':'keep' if keep else 'drop','reason':veto or ('taxonomy' if keep and news>=NEWS_THRESHOLD else 'atomic_fact' if keep else 'noise_or_weak_fact'),
            'policy_version':VERSION,'protocol_sha256':protocol_sha(),
            'model':payload.get('model','unknown'),'answers':answers,'usage':payload.get('usage',{}),
            'signals':{'news_mass':news,'max_fact':fact,'pure_noise':noise}}
