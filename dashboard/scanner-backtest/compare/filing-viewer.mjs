export const CATEGORIES={organization:{label:'Organization',color:'#bfdbfe'},person:{label:'Person',color:'#ddd6fe'},product:{label:'Product / platform',color:'#99f6e4'},law:{label:'Law / regulation',color:'#fed7aa'},place:{label:'Place',color:'#fbcfe8'},unclassified:{label:'Unclassified / other',color:'#e2e8f0'}};
export function category(types=[]){if(types.some(x=>['LAW'].includes(x)))return 'law';if(types.some(x=>['PERSON'].includes(x)))return 'person';if(types.some(x=>['PRODUCT'].includes(x)))return 'product';if(types.some(x=>['ORG','GOV_AGENCY'].includes(x)))return 'organization';if(types.some(x=>['GPE','LOC','FAC'].includes(x)))return 'place';return 'unclassified';}
export function annotations(bundle,source,version){
 const text=Array.from(source.text),rows=[],invalid=[];
 const add=r=>{if(!Number.isInteger(r.start)||!Number.isInteger(r.end)||r.start<0||r.end<=r.start||r.end>text.length||text.slice(r.start,r.end).join('')!==r.text){invalid.push(r);return;}rows.push({...r,category:category(r.types)});};
 if(version==='new'){
  const data=bundle.new;
  const values=data.raw_terms||data.evidence?.occurrences||[];
  for(const o of values)if(o.source_id===(source.new_source_id||source.id))add(o);
 }else{
  for(const r of bundle.old?.keywords||[]){
   if(!r.scanner?.eligible)continue;
   for(const id of r.evidence_ids||[])for(const w of bundle.old.keyword_evidence?.[id]?.witnesses||[]){
    const a=w.argument||w.anchor;
    if((w.source_id||w.source?.id)!==(source.old_source_id||source.id)||!a)continue;
    add({...a,keyword:r.keyword,types:[],relationship:r.predicate||r.relationship});
   }
  }
 }
 return {rows,invalid};
}
// Split overlapping annotations at every boundary. Each source character is emitted once.
export function segments(text,rows,lo=0,hi=Array.from(text).length,enabled=new Set(Object.keys(CATEGORIES))){
 const chars=Array.from(text);hi=Math.min(hi,chars.length);
 const events=new Map(),event=(p,k,r)=>{const v=events.get(p)||{add:[],remove:[]};v[k].push(r);events.set(p,v);};
 events.set(lo,{add:[],remove:[]});events.set(hi,{add:[],remove:[]});
 rows.forEach((r,id)=>{if(!enabled.has(r.category)||r.end<=lo||r.start>=hi)return;const row={...r,annotation_id:id};event(Math.max(lo,r.start),'add',row);event(Math.min(hi,r.end),'remove',row);});
 const points=[...events.keys()].sort((a,b)=>a-b),active=new Map(),out=[];
 for(let i=0;i<points.length-1;i++){const p=points[i],e=events.get(p);for(const r of e.remove)active.delete(r.annotation_id);for(const r of e.add)active.set(r.annotation_id,r);const end=points[i+1];if(end>p)out.push({start:p,end,text:chars.slice(p,end).join(''),annotations:[...active.values()]});}
 return out;
}
export async function verifySources(sources){
 for(const s of sources){if(typeof s.text!=='string')throw Error('Full source text is unavailable.');
  const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(s.text)))].map(x=>x.toString(16).padStart(2,'0')).join('');
  if(hash!==s.sha256||Array.from(s.text).length!==s.characters)throw Error('Filing source integrity check failed.');
 }
}
export function mountViewer(host){
 host.innerHTML='<h2>Read the filing</h2><p class="small">Colors show predicted term types, not verified relevance or business relationships. Unclassified terms remain gray. Unhighlighted text stays visible.</p><div class="toolbar"><select aria-label="Parser highlights" data-role="version"><option value="new">New · broad extraction</option><option value="old">Old · searchable keywords</option></select><select aria-label="Filing section" data-role="source"></select></div><div data-role="legend" class="toolbar"></div><p data-role="metadata" class="small"></p><div class="toolbar"><input type="search" aria-label="Search filing text" placeholder="Find text, including unhighlighted words" data-role="search"><button data-role="find">Find next</button><button data-role="previous">Previous page</button><span data-role="page"></span><button data-role="next">Next page</button></div><p data-role="status" aria-live="polite"></p><article class="filing-text" aria-label="Filing text" data-role="text"></article><section class="highlight-detail" aria-live="polite" data-role="detail">Click a highlight to inspect its exact source span.</section>';
 const el=r=>host.querySelector('[data-role="'+r+'"]');
 let bundle=null,sources=[],page=0,rows=[],lastFind=-1,ticket=0;
 const size=12000,enabled=new Set(Object.keys(CATEGORIES));
 for(const [id,c]of Object.entries(CATEGORIES)){const label=document.createElement('label'),input=document.createElement('input');input.type='checkbox';input.checked=true;input.setAttribute('aria-label',c.label+' highlights');input.onchange=()=>{input.checked?enabled.add(id):enabled.delete(id);paint();};label.style.background=c.color;label.className='legend-key';label.append(input,document.createTextNode(' '+c.label));el('legend').append(label);}
 function paint(){
  if(!bundle)return;const source=sources[Number(el('source').value)],chars=Array.from(source.text),lo=page*size,hi=Math.min(chars.length,lo+size);
  const data=annotations(bundle,source,el('version').value);rows=data.rows;
  const fragment=document.createDocumentFragment();
  for(const span of segments(source.text,rows,lo,hi,enabled)){
   if(!span.annotations.length){fragment.append(document.createTextNode(span.text));continue;}
   const mark=document.createElement('mark'),cats=[...new Set(span.annotations.map(a=>a.category))];
   mark.textContent=span.text;mark.tabIndex=0;mark.setAttribute('role','button');mark.setAttribute('aria-label',span.annotations.map(a=>a.text+' · '+CATEGORIES[a.category].label).join('; '));
   mark.style.background=cats.length===1?CATEGORIES[cats[0]].color:'linear-gradient(135deg,'+cats.map((c,i)=>CATEGORIES[c].color+' '+(i/cats.length*100)+'% '+((i+1)/cats.length*100)+'%').join(',')+')';
   const show=()=>{el('detail').replaceChildren();for(const a of span.annotations){const p=document.createElement('p');p.textContent=a.text+' · '+CATEGORIES[a.category].label+' · source offsets '+a.start+'–'+a.end+(a.keyword?' · old keyword: '+a.keyword:'')+' · classification remains a parser prediction';el('detail').append(p);}};
   mark.onclick=show;mark.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();show();}};fragment.append(mark);
  }
  el('text').replaceChildren(fragment);el('text').scrollTop=0;
  el('page').textContent='Page '+(page+1)+' / '+Math.max(1,Math.ceil(chars.length/size));
  el('previous').disabled=page===0;el('next').disabled=hi===chars.length;
  el('status').textContent=rows.length.toLocaleString()+' positioned term occurrences in this section. '+data.invalid.length+' invalid spans were rejected.'+(source.kind==='machine_data'?' Machine-readable attachment; prose extraction may not apply.':'')+(el('version').value==='new'&&!bundle.new.raw_terms?' Saved result uses the earlier candidate inventory.':'');
  el('metadata').replaceChildren(document.createTextNode((source.form||'DESCRIPTION')+' · '+(source.filing_date||'Undated')+' · '+(source.document||'Stored description')+' · '+(source.section||'')+' · '+chars.length.toLocaleString()+' characters. All section text is available across these pages. '));
  if(source.url?.startsWith('https://www.sec.gov/')){const a=document.createElement('a');a.href=source.url;a.target='_blank';a.rel='noopener';a.textContent='Original filing';el('metadata').append(a);}
 }
 el('source').onchange=()=>{page=0;lastFind=-1;el('detail').textContent='Click a highlight to inspect its exact source span.';paint();};
 el('version').onchange=()=>{el('detail').textContent='Click a highlight to inspect its exact source span.';paint();};
 el('previous').onclick=()=>{--page;paint();};el('next').onclick=()=>{++page;paint();};
 function find(){if(!bundle)return;const source=sources[Number(el('source').value)],q=el('search').value;if(!q)return;const hay=source.text.toLowerCase();let found=hay.indexOf(q.toLowerCase(),lastFind+1);if(found<0)found=hay.indexOf(q.toLowerCase());if(found<0){el('detail').textContent='Text not found in this section.';return;}lastFind=found;page=Math.floor(Array.from(source.text.slice(0,found)).length/size);paint();el('detail').textContent='Found “'+q+'” on page '+(page+1)+'. Search includes unhighlighted text.';}
 el('find').onclick=find;el('search').onkeydown=e=>{if(e.key==='Enter')find();};
 return {clear(){++ticket;bundle=null;sources=[];host.hidden=true;el('text').replaceChildren();el('detail').textContent='';el('source').replaceChildren();},
 async load(value){this.clear();const stamp=ticket;const next=value.source_texts;if(!next?.length){host.hidden=false;el('status').textContent='This saved result has no full filing text. Run a fresh comparison to load it; excerpts are not substituted for the filing.';return;}
  await verifySources(next);if(stamp!==ticket)return;bundle=value;sources=next;page=0;lastFind=-1;
  for(const [i,s]of sources.entries()){const o=document.createElement('option');o.value=i;o.textContent=(s.form||'DESCRIPTION')+' '+(s.filing_date||'Undated')+' · '+(s.document||'Description')+' · '+(s.section||s.kind);el('source').append(o);}
  el('source').value=String(Math.max(0,sources.findIndex(s=>s.kind!=='machine_data'&&s.kind==='business')));host.hidden=false;paint();
 }};
}
