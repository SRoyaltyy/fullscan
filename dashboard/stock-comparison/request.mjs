export const API='https://api.github.com';
export const REPO='SRoyaltyy/fullscan';
export const RESULTS='https://raw.githubusercontent.com/'+REPO+'/scanner-request-results/requests/';
const hex=/^[a-f0-9]{64}$/;
export function verifyReceipt(receipt, request, runId){
 for(const field of ['request_id','ticker','old_sha','new_sha'])if(receipt[field]!==request[field])throw Error('Result does not match request: '+field);
 if(!hex.test(receipt.input_sha256)||!hex.test(receipt.sha256))throw Error('Invalid result checksum receipt.');
 if(String(receipt.run_id)!==String(runId))throw Error('Result belongs to another workflow run.');
 return receipt;
}
export function verifyBundle(bundle,receipt){
 for(const field of ['request_id','ticker','old_sha','new_sha','input_sha256','run_id','catalog_sha256','coverage_kind'])
  if(String(bundle[field])!==String(receipt[field]))throw Error('Result envelope mismatch: '+field);
 if(bundle.schema!=='stock-parser-comparison-1')throw Error('Unsupported comparison result.');
 for(const role of ['old','new']){
  const result=bundle[role];
  if(!result||result.ticker!==bundle.ticker||result.source_sha!==bundle[role+'_sha']||result.input_sha256!==bundle.input_sha256)
   throw Error('Both parsers must use the requested stock, revisions and identical input.');
  if(JSON.stringify(result.source_receipt)!==JSON.stringify(bundle.source_receipt))throw Error('Source receipt mismatch.');
 }
 return bundle;
}
export async function api(path,token,{method='GET',body,signal}={}){
 // Credentials are sent only to the GitHub API, never raw assets or signed redirects.
 const url=new URL(path,API);
 if(url.origin!==API||!url.pathname.startsWith('/repos/'+REPO+'/'))throw Error('Unapproved GitHub API destination.');
 const response=await fetch(url,{method,signal,headers:{Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28',...(token?{Authorization:'Bearer '+token}:{}),...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});
 if(!response.ok){let message='';try{message=(await response.json()).message||'';}catch{}
  throw Error('GitHub '+response.status+': '+message+(response.status===403?' The connection needs Actions read/write access to this repository.':''));
 }
 return response.status===204?null:response.json();
}
async function raw(url,signal){
 const response=await fetch(url,{signal,cache:'no-store'});
 if(response.status===404)return null;
 if(!response.ok)throw Error('Result download failed ('+response.status+').');
 return response;
}
const wait=(ms,signal)=>new Promise((resolve,reject)=>{
 if(signal?.aborted){reject(new DOMException('Aborted','AbortError'));return;}
 const onAbort=()=>{clearTimeout(timer);reject(new DOMException('Aborted','AbortError'));};
 const timer=setTimeout(()=>{signal?.removeEventListener('abort',onAbort);resolve();},ms);
 signal?.addEventListener('abort',onAbort,{once:true});
});
export async function requestComparison(config,ticker,token,{signal,onProgress=()=>{}}={}){
 if(!token)throw Error('Connect GitHub first to start a fresh parser run.');
 const request={ticker,request_id:crypto.randomUUID(),old_sha:config.old_sha,new_sha:config.new_sha};
 const title='Stock parser '+ticker+' ['+request.request_id+']';
 const started=Date.now();onProgress({state:'Dispatching',seconds:0,request});
 await api('/repos/'+REPO+'/actions/workflows/'+config.workflow+'/dispatches',token,
  {method:'POST',signal,body:{ref:config.execution_ref,inputs:{...request,catalog_revision:config.catalog_revision}}});
 let run;
 while(Date.now()-started<2*60*60*1000){
  if(signal?.aborted)throw new DOMException('Aborted','AbortError');
  if(!run){
   const listing=await api('/repos/'+REPO+'/actions/workflows/'+config.workflow+'/runs?event=workflow_dispatch&per_page=100',token,{signal});
   run=listing.workflow_runs.find(r=>r.display_title===title&&r.head_branch===config.execution_ref&&r.event==='workflow_dispatch');
  }else run=await api('/repos/'+REPO+'/actions/runs/'+run.id,token,{signal});
  let state=run?.status||'queued';
  if(run?.status==='in_progress'){
   const jobs=await api('/repos/'+REPO+'/actions/runs/'+run.id+'/jobs',token,{signal});
   state=jobs.jobs.flatMap(j=>j.steps||[]).find(s=>s.status==='in_progress')?.name||state;
  }
  onProgress({state,seconds:Math.floor((Date.now()-started)/1000),runUrl:run?.html_url,request});
  if(run){
   const response=await raw(RESULTS+request.request_id+'/receipt.json?run='+run.id,signal);
   if(response){
    const receipt=verifyReceipt(await response.json(),request,run.id);
    if(receipt.catalog_sha256!==config.catalog_sha256)throw Error('Result used a different catalog/source snapshot.');
    const output=await raw(RESULTS+request.request_id+'/comparison.json.gz',signal);
    if(!output)throw Error('Result receipt exists but its data is unavailable.');
    const bytes=await output.arrayBuffer();
    const actual=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
    if(actual!==receipt.sha256)throw Error('Result file checksum mismatch.');
    const decoded=await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).json();
    return {...verifyBundle(decoded,receipt),workflow_url:run.html_url,wall_seconds:(Date.now()-started)/1000};
   }
   if(run.status==='completed')throw Error('Workflow '+run.conclusion+'; no verified comparison result was published. Inspect the linked run.');
  }
  await wait(5000,signal);
 }
 throw Error('The workflow did not return a result within two hours. Inspect the linked run.');
}
