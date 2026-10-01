"""Lane-aligned binary hop-0 ensemble. Never emits Lane event classes."""
import math
from .jev_acceptance import sha
from .jev_lane_contract import RUBRIC, CONTRACT_VERSION
VERSION='lane-hop0-v1'
QUESTIONS={
 'q5':{'type':'choice','instructions':RUBRIC,'criteria':{
  'change':'A specific new fact changes a tradable constraint, or a verified physical/legal regime break. Includes committed company expansion, actual results/guidance, deal announcement/cancellation, approval, court ruling, leadership/control, financing, actual disruption, official data or concrete new policy path.',
  'weather':'A known condition/tape reprint, interpretation, generic Fed/war/commodity color or public objective without a concrete new change.',
  'junk':'Advice/picks, price targets without company print/guidance, preview, roundup, transcript, feature, amenities, tiny subsidiary, unlinked local story or vague teaser.',
  'rumor':'Only speculation, hopes, leak, reportedly warned or hypothetical outcome without established event.'}},
 'action':{'type':'noul','instructions':'Is a concrete corporate, legal, operational, supply or funding constraint newly changed or committed in the supplied headline? Judge actual event, not generic financial relevance.', 'criteria':{'true':'An identifiable company/authority announces or executes a capacity change, leadership appointment/departure, binding deal or cancellation, buyback/financing/distress, gate/approval, legal ruling/probe, production decision, actual strike/disruption, new sanction/tariff/order or forced flow. A named listed firm committing to double data centers counts.','false':'Only market price, company opinion, analyst rating/target, uncommitted hopes/proposals, lawmakers urging, routine amenity/subsidiary, advice, preview, feature or no identifiable material change.'}},
 'print':{'type':'noul','instructions':'Does the supplied headline establish an actual new company result/guidance or official macro/production print? Do not confuse market prices, target prices, valuations or existing ownership with prints.', 'criteria':{'true':'Actual released earnings profit/revenue/growth/beat/miss, company guidance raised/cut or quantitative forecast newly issued, official inflation/jobs/GDP/retail/inventory data released, company production figures. A named earnings snapshot/release is actual results, not a preview.','false':'Analyst price target, share price/valuation, generic mentions of guidance/earnings without result, call transcript/highlights, future earnings preview, investor opinion or old unchanged fact.'}},
 'policy_path':{'type':'noul','instructions':'Is an authoritative policymaker stating a concrete NEW policy path, not general color? Freshness refers to supplied prior-event evidence; do not invent it.', 'criteria':{'true':'A named central-bank decision-maker announces a rate decision or specific new hike/cut path; an empowered government/OPEC actor commits a concrete supply/fiscal/trade/legal action. First-time Warsh says another hike is possible counts.','false':'Inflation objective, generic hawkishness, puts country on alert, pivotal moment, expert warning, rate odds, lawmakers urging, reportedly warned, hoped-for truce, or a rewrite of an already-scored statement.'}},
 'mechanism':{'type':'noul','instructions':'Is there an identifiable listed-company, US macro, global commodity/sector or US supply-chain channel changed by the reported event?', 'criteria':{'true':'A named listed company or broad macro/sector/commodity decision/print, financing/control/production/operations/legal gate. A foreign listed earnings print counts for further investigation. US relevance may be indirect through supply chains.','false':'Only sport/crime/monarchy/personal finance, local unrelated small business/flood, tiny subsidiary, unnamed minor recall or amenity promotion with no relevant listed channel.'}}
}
THRESHOLDS={'q5':.75,'action':.80,'print':.80,'policy_path':.80,'mechanism':.25,'junk':.70,'rumor':.60,'weather':.80}
FORMULA='mechanism >= cutoff AND junk < cutoff AND rumor < cutoff AND weather < cutoff AND (q5_change >= cutoff OR max(action,print,policy_path) >= respective cutoff)'
def protocol_sha():return sha(dict(version=VERSION,contract=CONTRACT_VERSION,rubric=RUBRIC,questions=QUESTIONS,thresholds=THRESHOLDS,formula=FORMULA))
def decide(row,payload):
 answers=payload['answers'];s={}
 for key,q in QUESTIONS.items():
  a=answers[key]
  if a.get('type')!=q['type']:raise ValueError('answer type mismatch')
  if q['type']=='choice':
   probs=a['probabilities']
   if set(probs)!=set(q['criteria']):raise ValueError('incomplete probabilities')
   values=list(probs.values())
  else:values=[a['noul']]
  if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not 0<=v<=1 for v in values):raise ValueError('invalid probabilities')
  if q['type']=='choice':
   if abs(sum(values)-1)>.02 or a.get('choice') not in probs or probs[a['choice']]<max(values):raise ValueError('invalid choice')
   s.update(probs)
  else:s[key]=values[0]
 t=THRESHOLDS
 veto=next((key for key in ('junk','rumor','weather') if s[key]>=t[key]),'')
 supported=s['change']>=t['q5'] or any(s[k]>=t[k] for k in ('action','print','policy_path'))
 keep=not veto and s['mechanism']>=t['mechanism'] and supported
 return dict(decision='keep' if keep else 'drop',reason=veto or ('constraint_change' if keep else 'no_supported_constraint'),policy_version=VERSION,protocol_sha256=protocol_sha(),model=payload.get('model','unknown'),answers=answers,signals=s,usage=payload.get('usage',{}),novelty_verified=bool(row.get('prior_events')))
