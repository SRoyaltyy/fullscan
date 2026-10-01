"""Frozen evidence/context ensemble; thresholds fitted only on exposed development data."""
import math,re
from . import jev_candidate as base
from .jev_recovery import QUESTIONS as RECOVERY_QUESTIONS
from .jev_acceptance import sha,RUBRIC
QUESTIONS={**base.QUESTIONS,**RECOVERY_QUESTIONS}
VERSION='evidence-context-v2'
THRESHOLDS={'news':.90,'atomic':.80,'evidence_floor':.25,'reported':.80,'context':.65,'link':.20,'calendar':.40}
EXTRA_ARTIFACT=re.compile(r'(?i)earnings call summary|^.{1,65}\bQ[1-4]\s+(?:FY)?20\d\d\s+earnings$|^\w+\s+(?:CPI|PPI|JOLTS job openings)(?:\s+\([^)]*\))?$|\bPDUFA cluster\b')
FORMULA='link>=cutoff AND calendar<cutoff AND (reported>=cutoff OR context>=cutoff OR reported>=floor AND (news>=cutoff OR max_atomic>=cutoff)); hard artifact veto'

def protocol_sha():
 return sha({'rubric':RUBRIC,'questions':QUESTIONS,'version':VERSION,'thresholds':THRESHOLDS,'base_artifacts':[r.pattern for r in (base.ARTIFACT,base.PREVIEW,base.WRAP,base.INDEX)],'extra_artifact':EXTRA_ARTIFACT.pattern,'formula':FORMULA})

def decide(row,payload):
 result=base.decide(row,payload) # Validate all original answers, even if they do not determine the final verdict.
 answers=payload['answers']
 for key,q in RECOVERY_QUESTIONS.items():
  a=answers[key]
  if a.get('type')!=q['type']:raise ValueError('wrong answer type')
  if q['type']=='choice':
   p=a['probabilities']
   if set(p)!=set(q['criteria']):raise ValueError('incomplete probabilities')
   values=list(p.values())
  else:values=[a['noul']]
  if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not 0<=v<=1 for v in values):raise ValueError('invalid probabilities')
  if q['type']=='choice' and (abs(sum(values)-1)>.02 or a.get('choice') not in p or p[a['choice']]<max(values)):raise ValueError('choice mismatch')
 evidence=answers['evidence']['probabilities']['reported']
 calendar=answers['evidence']['probabilities']['calendar_artifact']
 context=answers['context']['noul'];link=answers['link']['noul']
 title=re.split(r'\s+[-–—|]\s+(?=[^\n]{2,80}$)',row['title'])[0]
 veto=base.hard_noise(title) or ('calendar_or_call_summary' if EXTRA_ARTIFACT.search(title) else '')
 s=result['signals'];t=THRESHOLDS
 keep=(not veto and link>=t['link'] and calendar<t['calendar'] and
       (evidence>=t['reported'] or context>=t['context'] or evidence>=t['evidence_floor'] and (s['news_mass']>=t['news'] or s['max_fact']>=t['atomic'])))
 result.update(decision='keep' if keep else 'drop',reason=veto or ('reported_fact' if keep and evidence>=t['reported'] else 'factual_context' if keep and context>=t['context'] else 'supported_taxonomy' if keep else 'insufficient_supported_news'),policy_version=VERSION,protocol_sha256=protocol_sha())
 result['signals'].update(reported=evidence,calendar=calendar,context=context,market_link=link)
 return result
