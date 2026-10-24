import {api,requestComparison} from './request.mjs';
export const key=s=>String(s??'').trim().replace(/\s+/g,' ').toLocaleLowerCase();
export function signature(claims){return [...new Set(claims.map(c=>JSON.stringify([c.predicate||'',c.assertion||'',c.capacity||'unknown'])))].sort().join('|');}
export function oldTerms(data,includeExcluded=false){
 const terms=new Map();
 for(const r of data?.keywords||[]){if(!includeExcluded && !r.scanner?.eligible)continue;const k=key(r.keyword);if(!k)continue;
  const e=terms.get(k)||{name:r.keyword,aliases:[],claims:[],rows:[]};e.rows.push(r);e.aliases.push(...(r.source_declared_identity?.aliases||[]));
  e.claims.push({predicate:r.predicate||r.relationship,assertion:r.assertion,capacity:r.qualifiers?.frame?.capacity||'unknown'});terms.set(k,e);
 }return terms;
}
export function newTerms(data){
 const terms=new Map(),occ=new Map(),claims=new Map();
 for(const o of data?.evidence?.occurrences||[]){const a=occ.get(o.entity_id)||[];a.push(o);occ.set(o.entity_id,a);}
 for(const a of data?.evidence?.assertions||[]){const list=claims.get(a.entity_id)||[];list.push(a);claims.set(a.entity_id,list);}
 for(const e of data?.evidence?.entities||[]){const k=key(e.name);const t=terms.get(k)||{name:e.name,aliases:[],claims:[],occurrences:[],entities:[]};t.entities.push(e);t.claims.push(...(claims.get(e.id)||[]));t.occurrences.push(...(occ.get(e.id)||[]));t.aliases.push(...(occ.get(e.id)||[]).filter(o=>o.identity_status==='explicit_definition').map(o=>o.text));terms.set(k,t);}
 return terms;
}
export function compareTerms(oldMap,newMap){
 return [...new Set([...oldMap.keys(),...(newMap?.keys()||[])])].sort().map(k=>{const old=oldMap.get(k),next=newMap?.get(k);return {key:k,name:next?.name||old.name,old,next,status:newMap===null?'pending':!old?'added':!next?'removed':signature(old.claims)===signature(next.claims)?'same':'changed'};});
}

if(typeof document!=='undefined'){
const $=id=>document.getElementById(id),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
if(location.pathname.endsWith('/compare/')){$('archived-review').href='../completed/';$('earlier-preview').href='../experimental/';}
let catalog,oldData,newData,currentTicker,items=[],sequence=0;
async function json(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok){const e=Error('Request failed ('+r.status+'): '+url);e.status=r.status;throw e;}return r.json();}
async function asset(url,expected){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw Error('Stock data unavailable ('+r.status+').');const blob=await r.blob(),bytes=await blob.arrayBuffer();const digest=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');if(digest!==expected)throw Error('Stock data checksum mismatch; no comparison is shown.');return JSON.parse(await new Response(blob.stream().pipeThrough(new DecompressionStream('gzip'))).text());}
function label(t){return t?.claims.length?[...new Set(t.claims.map(c=>c.predicate+' ['+c.assertion+']'))].join('; '):t?'Unclassified':'—';}
function render(){const oldMap=oldTerms(oldData,$('excluded').checked),nextMap=newData?newTerms(newData):null;items=compareTerms(oldMap,nextMap);const q=key($('query').value),change=$('change').value;const totals={};for(const i of items)totals[i.status]=(totals[i.status]||0)+1;
 $('counts').innerHTML=Object.entries(totals).map(([s,n])=>`<span class="${s}"><strong>${n.toLocaleString()}</strong> ${esc(s)}</span>`).join('');
 const selected=items.filter(i=>(!q||[i.name,...(i.old?.aliases||[]),...(i.next?.aliases||[])].some(n=>key(n).includes(q)))&&(change==='all'||(change==='unclassified'?i.next&&!i.next.claims.length:i.status===change)));
 const tbody=$('rows');tbody.replaceChildren();for(const i of selected){const tr=document.createElement('tr');tr.innerHTML=`<td><button>${esc(i.name)}</button></td><td>${esc(label(i.old))}</td><td>${esc(i.status==='pending'?'Rebuild pending':label(i.next))}</td><td class="${i.status}">${esc(i.status)}</td>`;tr.querySelector('button').onclick=()=>detail(i);tbody.append(tr);}if(!selected.length)tbody.innerHTML='<tr><td colspan="4">No matching output terms. Missing output is not proof that the source lacks the term.</td></tr>';
}
function link(s){const url=s?.url;if(url?.startsWith('https://www.sec.gov/'))return `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(s.document||s.section||'SEC source')}</a>`;return esc(s?.document||s?.source||'Stored description');}
function detail(i){
 let old=`<h2>Old · ${esc(i.old?.name||i.name)}</h2>`,next=`<h2>New · ${esc(i.next?.name||i.name)}</h2>`;
 if(!i.old)old+='<p>No searchable term in the selected old output.</p>';
 for(const r of i.old?.rows||[]){old+=`<div class="record"><strong>${esc(currentTicker)} → ${esc(r.predicate||r.relationship)} → ${esc(r.keyword)}</strong><p>${esc(r.assertion)} · ${esc(r.scanner?.group)} · ${esc(r.first_filing_date||'Undated')}–${esc(r.latest_filing_date||'Undated')}</p>${r.scanner?.eligible===false?`<p>Excluded by old policy: ${esc(r.scanner.reason)}</p>`:''}`;
  for(const id of r.evidence_ids||[]){const ev=oldData.keyword_evidence?.[id];if(!ev)continue;for(const w of ev.witnesses||[]){old+=`<p class="small">${esc(w.source?.filing_date||'Undated')} · ${link(w.source)}</p><blockquote>${esc(w.quote||w.text||'')}</blockquote>`;}}
  old+=`<details><summary>Old term record</summary><pre>${esc(JSON.stringify(r,null,2))}</pre></details></div>`;
 }
 if(!newData)next+='<p>New-version rebuild is pending for this stock. Old output is not relabelled as new.</p>';
 else if(!i.next)next+='<p>This term is missing from the new extractor’s output. This can be a regression.</p>';
 else if(!i.next.claims.length)next+='<p><strong>Unclassified:</strong> extracted, with no supported relationship statement.</p>';
 const sources=new Map((newData?.sources||[]).map(s=>[s.id,s]));
 for(const a of i.next?.claims||[]){const ev=a.evidence||{};next+=`<div class="record"><strong>${esc(a.subject||newData.ticker)} → ${esc(a.predicate)} → ${esc(i.next.name)}</strong><p>${esc(a.assertion)} · ${esc(a.capacity||'unknown')} · ${esc(a.filing_date||'Undated')}</p><p class="small">${link(sources.get(a.source_id))} · ${esc(a.basis)}</p>${a.direct_ownership_established===false?'<p>Group membership reported; direct ownership is not established.</p>':''}<blockquote>${esc(ev.quote||ev.text||ev.sentence?.text||ev.argument?.text||'See source occurrences below.')}</blockquote><details><summary>New relationship record</summary><pre>${esc(JSON.stringify(a,null,2))}</pre></details></div>`;}
 for(const o of i.next?.occurrences||[]){next+=`<div class="record"><strong>${esc(o.text)}</strong><p class="small">${esc(o.filing_date||'Undated')} · ${link(sources.get(o.source_id))} · offsets ${o.start}–${o.end}</p><blockquote>${esc(o.excerpt||o.text)}</blockquote><p class="small">${esc(o.identity_status)} · ${esc(o.extraction_basis?.join(', '))}</p></div>`;}
 $('old-detail').innerHTML=old;$('new-detail').innerHTML=next;
}
let controller=null,token='',config,currentBundle=null;
function clear(){oldData=null;newData=null;items=[];currentBundle=null;$('rows').replaceChildren();$('counts').replaceChildren();$('old-detail').innerHTML='<h2>Old version</h2><p>Select a term after parsing.</p>';$('new-detail').innerHTML='<h2>New version</h2><p>Select a term after parsing.</p>';}
$('connect').onclick=async()=>{
 try{
  const value=$('github-token').value.trim();if(!value)throw Error('Enter a GitHub token with Actions read/write access for SRoyaltyy/fullscan.');
  await api('/repos/SRoyaltyy/fullscan/actions/workflows/'+config.workflow,value);
  token=value;$('github-token').value='';$('connection').textContent='GitHub connected for this tab. Credentials stay in memory.';$('disconnect').hidden=false;$('error').textContent='';
 }catch(e){$('error').textContent=e.message;}
};
$('disconnect').onclick=()=>{token='';controller?.abort();++sequence;clear();$('connection').textContent='Disconnected.';$('disconnect').hidden=true;};
async function load(){
 controller?.abort();controller=new AbortController();const signal=controller.signal,n=++sequence;clear();$('error').textContent='';
 const ticker=$('ticker').value.trim().toUpperCase(),row=catalog.catalog[ticker];
 if(!row){$('stock-summary').textContent='No stock selected.';$('error').textContent='Choose a catalog ticker from the suggestions.';return;}
 currentTicker=ticker;$('stock-summary').textContent='Starting a fresh parse for '+ticker+'…';$('run-link').hidden=true;
 try{
  const result=await requestComparison(config,ticker,token,{signal,onProgress:p=>{
   if(n!==sequence)return;
   $('stock-summary').textContent=ticker+' · '+p.state+' · '+p.seconds+' seconds since request';
   if(p.runUrl){$('run-link').href=p.runUrl;$('run-link').hidden=false;}
  }});
  if(n!==sequence||signal.aborted)return;
  oldData=result.old;newData=result.new;currentBundle=result;
  $('stock-summary').innerHTML='<strong>'+esc(ticker)+' · '+esc(row.name)+'</strong><p>'+esc(result.coverage_kind.replaceAll('_',' '))+' · fresh old/new execution on identical source bytes</p><p class="small">Fetch '+result.timings.fetch_seconds+'s · old parse '+result.timings.old_parse_seconds+'s · new parse '+result.timings.new_parse_seconds+'s · total request '+Math.round(result.wall_seconds)+'s (includes GitHub queue, setup and publication).</p>';
  $('coverage').innerHTML='<p>Old parser: <code>'+esc(result.old_sha)+'</code><br>New parser: <code>'+esc(result.new_sha)+'</code><br>Shared input SHA-256: <code>'+esc(result.input_sha256)+'</code><br>Cutoff: '+esc(result.as_of)+'</p><details><summary>Stored source receipt</summary><pre>'+esc(JSON.stringify(result.source_receipt,null,2))+'</pre></details>';
  if(result.coverage_kind==='description_only')$('stock-summary').innerHTML+='<p class="notice">Description only. This stock has no archived filings. The old searchable keyword compiler requires dated evidence; undated descriptions may yield no old keywords.</p>';
  history.replaceState(null,'','?ticker='+encodeURIComponent(ticker));render();
 }catch(e){if(n!==sequence||e.name==='AbortError')return;clear();$('stock-summary').textContent='Comparison unavailable.';$('error').textContent=e.message;}
}
$('compare').onclick=load;$('ticker').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();load();}};
$('random').onclick=()=>{const tickers=Object.keys(catalog.catalog),n=crypto.getRandomValues(new Uint32Array(1))[0];$('ticker').value=tickers[n%tickers.length];load();};
$('query').oninput=()=>oldData&&render();$('change').onchange=()=>oldData&&render();$('excluded').onchange=()=>oldData&&render();
$('download').onclick=()=>{if(!currentBundle)return;const out={...currentBundle,differences:items.map(i=>({name:i.name,status:i.status,old_relationships:i.old?.claims||[],new_relationships:i.next?.claims||[]}))};const u=URL.createObjectURL(new Blob([JSON.stringify(out,null,2)],{type:'application/json'})),a=document.createElement('a');a.href=u;a.download=currentTicker+'-fresh-old-vs-new.json';a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);};
try{
 [catalog,config]=await Promise.all([json('catalog.json'),json('request-config.json')]);
 $('revisions').textContent='Old: '+config.old_sha+' · New: '+config.new_sha+' · experimental; accuracy not certified';
 $('progress').textContent=Object.keys(catalog.catalog).length.toLocaleString()+' catalog stocks selectable. Archived filings and description-only sources remain distinct. Each request parses one stock; no catalog rebuild is required.';
 const opts=document.createDocumentFragment();for(const [ticker,row]of Object.entries(catalog.catalog)){const o=document.createElement('option');o.value=ticker;o.label=row.name;opts.append(o);}$('tickers').append(opts);
 $('saved-comparison').href=config.saved_comparison;
 const requested=new URL(location.href).searchParams.get('ticker');$('ticker').value=requested&&catalog.catalog[requested]?requested:'BLZE';
 $('stock-summary').textContent='Connect GitHub, select a stock and press Enter to run both parsers.';
}catch(e){$('error').textContent=e.message;}
}
