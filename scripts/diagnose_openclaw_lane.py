"""Read-only ECS health inventory and bounded real calls. Never emits credentials."""
import argparse, contextlib, io, json, os, pathlib, subprocess
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
                          '-p','Result','-p','LastTriggerUSec','-p','NextElapseUSecRealtime'],
                         capture_output=True,text=True,timeout=15)
        out[unit]=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
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
            call.pop('answer',None)
            out['calls'].append({'model_requested':model,**call})
    expiry,_,_=guard.xai_oauth_expiry()
    out['oauth_expiry_after']=expiry.isoformat() if expiry else None
    out['lane_ready']=any(c.get('ok') and c['model_requested']=='xai/grok-4.6'
                          for c in out['calls'])
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(out,indent=2))
    print(json.dumps(out,indent=2))
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as f:
            f.write('ready='+str(out['lane_ready']).lower()+'\n')


if __name__=='__main__':main()
