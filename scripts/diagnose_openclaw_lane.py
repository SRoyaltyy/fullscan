"""Read-only ECS health inventory and bounded real calls. Never emits credentials."""
import argparse, contextlib, hashlib, io, json, os, pathlib, re, subprocess
from datetime import datetime, timezone
import openclaw_oauth_guard as guard


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args=parser.parse_args()
    out={'generated_at':datetime.now(timezone.utc).isoformat(), 'calls':[]}
    expiry,source,refresh=guard.xai_oauth_expiry()
    out.update(oauth_expiry_before=expiry.isoformat() if expiry else None,
               expiry_source=source, has_refresh_token=refresh,
               grok_busy=guard.grok_job_running())
    guard.pid_alive(out)
    for unit in ('fullscan-openclaw-oauth.timer','fullscan-openclaw-oauth.service'):
        r=subprocess.run(['systemctl','show',unit,'-p','ActiveState','-p','SubState',
                          '-p','Result','-p','ExecMainStatus','-p','LastTriggerUSec','-p','NextElapseUSecRealtime'],
                         capture_output=True,text=True,timeout=15)
        out[unit]=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
    installed=pathlib.Path('/home/gha/fullscan/scripts/openclaw_oauth_guard.py')
    out['timer_script_exists']=installed.is_file()
    out['timer_script_sha256']=hashlib.sha256(installed.read_bytes()).hexdigest() if installed.is_file() else None
    r=subprocess.run(['journalctl','-u','fullscan-openclaw-oauth.service','-n','80','--no-pager','-o','cat'],
                     capture_output=True,text=True,timeout=15)
    secret_values=[guard.gateway_token(),os.environ.get('OPENCLAW_TOKEN','')]
    def safe(text):
        for secret in secret_values:
            if secret:text=text.replace(secret,'<redacted>')
        text=re.sub(r'https?://\S+','<url>',text)
        return re.sub(r'(eyJ[\w.-]{10,}|[A-Za-z0-9_-]{40,})','<redacted>',text)
    out['timer_failure_lines']=[safe(line)[:240] for line in r.stdout.splitlines()
        if re.search(r'^(?:Traceback|\s+File |[A-Za-z]+Error:)|FAILED rc=|Failed with result|Main process exited',line)][-12:]
    try:
        last=json.loads(guard.STATUS.read_text())
        out['last_guard']={k:last.get(k) for k in ('ok','exit','started_et','finished_et',
                           'expires_before','expires_after','pid_alive','restarted')}
    except (OSError,ValueError):out['last_guard']=None
    # Do not compete with a running production prediction or restart it.
    if not out['grok_busy']:
        for model in ('xai/grok-4.3','xai/grok-4.6'):
            guard.MODEL=model
            result={}
            with contextlib.redirect_stdout(io.StringIO()):
                guard.real_completion(result,'lane_probe')
            call=result.get('call_lane_probe',{})
            answer=call.pop('answer',None)
            if not call.get('ok') and answer:call['failure_prefix']=safe(str(answer))[:40]
            out['calls'].append({'model_requested':model,**call})
    expiry,_,_=guard.xai_oauth_expiry()
    out['oauth_expiry_after']=expiry.isoformat() if expiry else None
    out['lane_ready']=any(c.get('ok') and c['model_requested']=='xai/grok-4.3'
                          for c in out['calls'])
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(out,indent=2))
    print(json.dumps(out,indent=2))
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as f:
            f.write('ready='+str(out['lane_ready']).lower()+'\n')


if __name__=='__main__':main()
