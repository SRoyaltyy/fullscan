"""Offline catalog refresh, branch-aware state selection, and integrity checks."""
import argparse,hashlib,json,pathlib,sqlite3
from datetime import timedelta
from src.primary_actions import now,date,iso,atomic_json,public_get_audit_ok,data_gov_api_key

def refresh_needed(root,force=False):
    p=root/'config/primary_action_sources.json'
    if force or not p.exists() or not (root/'data/primary_actions/institution_registry.json').exists():return True
    catalog=json.loads(p.read_text(encoding='utf-8'))
    seed=hashlib.sha256((root/'config/primary_action_seeds.json').read_bytes()).hexdigest()
    implementation=hashlib.sha256((root/'src/primary_actions.py').read_bytes()+(root/'scripts/build_primary_action_catalog.py').read_bytes()).hexdigest()
    stamp=date(catalog.get('generated_at'))
    configured=catalog.get('credential_configuration',{}).get('data_gov_api_key_configured',False)
    return configured!=bool(data_gov_api_key()) or catalog.get('seed_sha256')!=seed or catalog.get('implementation_sha256')!=implementation or not stamp or now()-stamp>timedelta(days=7)

def previous_artifact(artifacts,branch):
    # A main deployment can promote the verified PR state without resetting document IDs.
    branches=[branch]+(['codex/primary-action-scanner-20261015'] if branch=='main' else [])
    for name in ('primary-actions-state','primary-actions-verification'):
        for candidate_branch in branches:
            rows=[x for x in artifacts if x['name']==name and not x.get('expired') and x.get('workflow_run',{}).get('head_branch')==candidate_branch]
            if rows:return max(rows,key=lambda x:x['created_at'])
    return None

def audit(root):
    out=root/'data/primary_actions/latest';requests=0
    assert (out/'http_request_audit.json').exists(),'Missing runtime request audit'
    for p in (root/'research/http_request_audit.json',out/'http_request_audit.json'):
        if not p.exists():continue
        records=json.loads(p.read_text(encoding='utf-8'));requests+=len(records)
        assert all(public_get_audit_ok(x) for x in records),'Non-official, credential-leaking, or body-bearing request'
    keyed=sum(1 for p in (root/'research/http_request_audit.json',out/'http_request_audit.json') if p.exists() for x in json.loads(p.read_text(encoding='utf-8')) if x.get('authorization_sent'))
    state=root/'data/primary_actions_state/ledger.sqlite3'
    assert state.exists(),'Missing persistent dedup state'
    db=sqlite3.connect(state)
    try:
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        retained=db.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
        assert retained==db.execute('SELECT COUNT(DISTINCT canonical_key) FROM documents').fetchone()[0]
    finally:db.close()
    report=json.loads((out/'report.json').read_text(encoding='utf-8'))
    assert report['llm_calls']==report['paid_api_calls']==0
    catalog=json.loads((root/'config/primary_action_sources.json').read_text(encoding='utf-8'))
    result={'checked_at':iso(now()),'official_unauthenticated_gets':requests-keyed,'official_keyed_gets':keyed,'retained_unique_documents':retained,'sqlite_integrity':'ok','llm_calls':0,'paid_api_calls':0,'catalog_generated_at':catalog['generated_at'],'catalog_refresh_needed':refresh_needed(root)}
    atomic_json(out/'integrity.json',result);return result

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['refresh-needed','audit','previous-artifact']);p.add_argument('--root',type=pathlib.Path,default=pathlib.Path('.'));p.add_argument('--force',action='store_true');p.add_argument('--artifacts',type=pathlib.Path);p.add_argument('--branch');a=p.parse_args()
    if a.action=='refresh-needed':print('true' if refresh_needed(a.root,a.force) else 'false')
    elif a.action=='previous-artifact':print(json.dumps(previous_artifact(json.loads(a.artifacts.read_text())['artifacts'],a.branch)))
    else:print(json.dumps(audit(a.root),indent=2))
if __name__=='__main__':main()
