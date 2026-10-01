"""Frozen candidate learned on exposed development data, never on acceptance gold.

Taxonomy supplies a broad view; atomic questions recover factual developments.
A separate pure-noise answer resolves wrappers. Probabilities are not multiplied.
"""
import math,re
from .jev_acceptance import sha,RUBRIC
QUESTIONS={
  "kind": {
    "type": "choice",
    "instructions": "Identify what this headline reports. Look through stock-price and investing-advice wording to the underlying information. A broad market wrap is still a recap. Use only the supplied headline.",
    "criteria": {
      "company_news": "Company results or guidance, deal talks or announcements, product launch, clinical results, layoffs, hiring/leadership departure, financing/IPO, analyst rating/target changes, insider transaction, recall or legal ruling.",
      "policy_data": "Official central bank/government economic or monetary views, policy proposal or decision, trade/sanctions action, or economic data released. Includes major foreign economies and global commodity policy.",
      "industry_fact": "Specific factual supply/demand, capacity, competition, credit or operational change in an industry relevant to US equities or global commodities/supply chains.",
      "noise": "Only price/market recap, preview, picks, opinion, evergreen advice/explanation, transcript, vague promotion, or unrelated local/nonfinancial news."
    }
  },
  "company": {
    "type": "noul",
    "instructions": "Does this headline report a company development? Ignore stock-reaction or advice framing. An announced plan is a development, even before completion.",
    "criteria": {
      "true": "A firm reports results/guidance; launches, hires/fires, expands/closes, finances/files IPO, buys/sells/negotiates a deal, changes a rating/target, trades insider shares, releases clinical data, recalls a product, or faces a new legal/regulatory ruling.",
      "false": "Only stock performance, valuation, a price forecast, picks, future earnings preview, general promotion without an identifiable development, or unrelated nonfinancial news."
    }
  },
  "macro": {
    "type": "noul",
    "instructions": "Does this headline report economic data or an official economic policy action, proposal or view? Official speech can be news without a completed policy change.",
    "criteria": {
      "true": "Fed/central bank official speaks about rates/inflation; official economic data released; government announces/proposes fiscal, trade, sanction, energy, financial or technology regulation. Includes major foreign economies and global commodities.",
      "false": "Only an expert/columnist opinion, upcoming data preview, market rate-hike odds, price recap with no policy details, general evergreen explanation, or unrelated politics."
    }
  },
  "industry": {
    "type": "noul",
    "instructions": "Does this headline state a factual industry/economic change with a concrete market link? A factual structural change need not be a discrete completed deal.",
    "criteria": {
      "true": "Specific supply, demand, capacity, credit, costs, competitive or operational change for US businesses, major global sectors, commodities or supply chains. A new credit-rate milestone or industry adoption/capacity change counts.",
      "false": "Only a generic price recap, vague 'could transform' prediction, investment advice, evergreen mechanism explanation, or an unrelated local story."
    }
  },
  "noise": {
    "type": "noul",
    "instructions": "Is this headline ONLY noise, with no specific underlying financial/economic development? A company event or official economic statement makes this false even inside a price or advice wrapper.",
    "criteria": {
      "true": "Only a generic price/market recap, broad live wrap, future earnings/data preview, transcript, evergreen advice/explanation, investment picks/opinion, vague promotion with no identifiable development, or unrelated local/nonfinancial news.",
      "false": "Reports company results/guidance, launches, layoffs, expansion/closure, financing, deal talks/announcements, insider trades, broker rating/target changes, clinical data, recalls or legal rulings. Also official monetary/fiscal/trade policy proposals/statements, economic releases, or factual industry supply/demand/credit/competition changes. Includes major global sectors, economies, commodities and supply chains. Actions may be planned or proposed; price/advice wrappers do not erase reported facts."
    }
  }
}
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
