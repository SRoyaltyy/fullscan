"""Replay or evaluate frozen title-only inputs; reference is evaluation-only.

Runtime mode reads only selection.json. Compare mode is a separate invocation
and does not execute the classifier. Never overwrites baseline or reference.
"""
import argparse
import hashlib
import json
import pathlib
from datetime import datetime,timezone

ROOT=pathlib.Path(__file__).resolve().parents[1]

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

# Evaluation normalization of the old free-form industry labels to coarse
# market sectors. This mapping is never imported by runtime modules. It is
# deliberately published: coarse sector agreement is NOT industry accuracy.
SECTOR_GROUPS={
 'Healthcare':['Medical devices','Biotechnology','Food-safety and diagnostic testing','Antiviral medicines','HIV medicines'],
 'Energy':['Diesel trading and refining','Diesel distribution','Diesel supply','Diesel trade','Diesel exports','Oil and gas','Natural gas trade','Natural gas production','Oil supply'],
 'Technology':['Semiconductors','AI services','Professional audio equipment','Vehicle noise-control technology'],
 'Communication Services':['Social media','Television entertainment','Film and television','Video entertainment'],
 'Financial':['Auto lending and refinancing','Bank lending','Lending','Capital markets','Tokenized securities','Money transfer','Pension financial services','SME financing'],
 'Industrials':['Defence equipment','Industrial transmission equipment','Container shipping','Passenger transport','Manufacturing','Mining equipment and services','Air travel','Marine transport','Power-generation equipment','Trucking','Industrial electricity use'],
 'Utilities':['Water utilities','Electric utilities'],
 'Consumer Cyclical':['Apparel retail','Consumer-facing retail and services','E-commerce','Online gambling','Auto manufacturing','Auto dealers','Auto retail','Garment manufacturing','Vehicle collision repair','Restaurants','Furniture manufacturing'],
 'Consumer Defensive':['Food businesses','Agricultural fuel use','Vegetable food products','Palm oil trade','Agriculture','Fishing','Pharmacy retail'],
 'Basic Materials':['Uranium mining','Mining','Chemicals','Coatings manufacturing','Copper and nickel mining'],
 'Real Estate':['REITs','Property development'],
}
NORMALIZE={label:[sector] for sector,labels in SECTOR_GROUPS.items() for label in labels}
NORMALIZE['Diversified entertainment and electronics']=['Communication Services','Technology']


def replay(selection_path,output_path):
    from src.news_intake import first_pass
    selection=json.loads(selection_path.read_text())
    now=datetime.now(timezone.utc);outputs=[]
    for n,row in enumerate(selection['selected'],1):
        title=row['title']
        out=first_pass({'id':hashlib.sha256(title.encode()).hexdigest(),'title':title,'body':'','first_seen':now.isoformat()},now)
        outputs.append({'sample_number':n,'program_output':out})
    output_path.write_text(json.dumps({'selection':selection,'scope':'title-only deterministic program; zero model calls; reference not loaded','selection_sha256':sha(selection_path),'generated_at':now.isoformat(),'outputs':outputs},ensure_ascii=False,indent=2))
    print(json.dumps({'samples':len(outputs),'output':str(output_path),'sha256':sha(output_path)}))


def compare(baseline_path,output_path,reference_path,report_path):
    baseline=json.loads(baseline_path.read_text());fixed=json.loads(output_path.read_text());reference=json.loads(reference_path.read_text())
    assert baseline['selection']==fixed['selection'], 'selection changed'
    assert len(baseline['outputs'])==len(fixed['outputs'])==len(reference['records'])==100
    records=[]
    for old,new,ref in zip(baseline['outputs'],fixed['outputs'],reference['records']):
        n=new['sample_number'];assert old['sample_number']==n==ref['n']
        a=old['program_output'];b=new['program_output'];assert a['title']==b['title']
        expected=sorted({s for label in (ref['market_sectors'] or []) for s in NORMALIZE[label]})
        actual=b['sector_classification']['market_sectors']
        records.append({'n':n,'title':b['title'],'baseline':a,'program':b,'frontier':ref,
            'normalized_reference_sectors':expected,
            'checks':{'action_correct':b['institutional_actions']['has_concrete_reported_action']==ref['concrete_institutional_action'] if ref['concrete_institutional_action'] is not None else None,
              'lane_correct':b['lane_rule_classification']['event_class'] in ref['acceptable_lane_classes'] if ref['acceptable_lane_classes'] else None,
              'expected_sectors_covered':set(expected)<=set(actual) if expected else None,
              'exact_sector_set':expected==actual if expected else None},
            'additional_sectors':sorted(set(actual)-set(expected)) if expected else None})
    action=[r for r in records if r['checks']['action_correct'] is not None]
    lane=[r for r in records if r['checks']['lane_correct'] is not None]
    sector=[r for r in records if r['normalized_reference_sectors']]
    tp=sum(r['program']['institutional_actions']['has_concrete_reported_action'] and r['frontier']['concrete_institutional_action'] for r in action)
    fp=sum(r['program']['institutional_actions']['has_concrete_reported_action'] and not r['frontier']['concrete_institutional_action'] for r in action)
    fn=sum(not r['program']['institutional_actions']['has_concrete_reported_action'] and r['frontier']['concrete_institutional_action'] for r in action)
    summary={'actions':{'correct':sum(r['checks']['action_correct'] for r in action),'scored':len(action),'ambiguous_excluded':100-len(action),'tp':tp,'fp':fp,'fn':fn,'precision':tp/(tp+fp),'recall':tp/(tp+fn)},
        'lane':{'correct':sum(r['checks']['lane_correct'] for r in lane),'scored':len(lane),'enum_gaps_excluded':100-len(lane)},
        'sectors':{'scored':len(sector),'answered':sum(bool(r['program']['sector_classification']['market_sectors']) for r in sector),'expected_sectors_covered':sum(r['checks']['expected_sectors_covered'] for r in sector),'exact_sector_sets':sum(r['checks']['exact_sector_set'] for r in sector),'industry_accuracy_measured':False}}
    result={'statistics':summary,'baseline_sha256':sha(baseline_path),'program_sha256':sha(output_path),'reference_sha256':sha(reference_path),'selection_sha256':sha(output_path.parent/'selection.json') if (output_path.parent/'selection.json').exists() else None,
        'limits':reference['limits']+['Development replay after inspecting this sample; not an unseen accuracy estimate.','Sector checks normalize industry phrases to coarse sectors; they do not measure industry granularity, material exposure or causal direction.','Expected-sector coverage allows extra outputs; exact set agreement is reported separately.'], 'evaluation_sector_normalization':NORMALIZE,'samples':records}
    report_path.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(summary,indent=2))
    for r in records:
        if any(v is False for v in r['checks'].values()):
            print('DISAGREEMENT',r['n'],r['checks'],r['additional_sectors'])
    return result


def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='mode',required=True)
    r=sub.add_parser('replay');r.add_argument('--selection',type=pathlib.Path,required=True);r.add_argument('--output',type=pathlib.Path,required=True)
    c=sub.add_parser('compare');c.add_argument('--baseline',type=pathlib.Path,required=True);c.add_argument('--program',type=pathlib.Path,required=True);c.add_argument('--reference',type=pathlib.Path,required=True);c.add_argument('--report',type=pathlib.Path,required=True)
    args=p.parse_args()
    if args.mode=='replay':
        if args.selection.resolve()==args.output.resolve() or args.output.exists(): p.error('Output must be a new file; frozen inputs are never overwritten')
        replay(args.selection,args.output)
    else:
        if args.report.resolve() in {args.baseline.resolve(),args.program.resolve(),args.reference.resolve()}: p.error('Report must not overwrite input')
        compare(args.baseline,args.program,args.reference,args.report)

if __name__=='__main__': main()
