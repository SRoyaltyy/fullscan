
function ovParseHeld(xs){
  const out={};
  (xs||[]).forEach(function(x){
    const s=String(x);
    const i=s.search(/[×x]/);
    if(i>0){ out[s.slice(0,i)]=Number(s.slice(i+1)); }
    else if(s) out[s]=null;
  });
  return out;
}
function ovPx(ticker, date){
  const row=(((D.sim||{}).tape||{})[ticker]||{})[date];
  if(row==null) return null;
  if(Array.isArray(row) && row.length>=2) return {o:Number(row[0]), c:Number(row[1])};
  if(typeof row==='object') return {o:Number(row.open||row.o), c:Number(row.close||row.c)};
  return null;
}
function ovPrevDate(date, dates){
  const i=(dates||[]).indexOf(date);
  return i>0 ? dates[i-1] : null;
}
function ovSplitDays(name){
  const days=((D.daily||{})[name]||[]).slice().sort(function(a,b){return String(a.date).localeCompare(String(b.date));});
  const dates=days.map(function(d){return d.date;});
  const trades=(((D.books||{})[name]||{}).trades||[]).filter(function(t){return t.side==='BUY' && t.ticker;});
  return days.map(function(d){
    const bought={}; (d.bought||[]).forEach(function(t){ bought[t]=true; });
    const sold={}; (d.sold||[]).forEach(function(t){ sold[t]=true; });
    let neu=0, held=0;
    trades.forEach(function(t){
      if(t.date!==d.date) return;
      const p=ovPx(t.ticker, d.date);
      if(!p || !isFinite(p.c) || !isFinite(t.price)) return;
      neu += Number(t.shares||0)*(p.c-Number(t.price));
      if(t.fees) neu -= Number(t.fees);
    });
    const prev=ovPrevDate(d.date, dates);
    const openHeld=ovParseHeld(d.open_held);
    Object.keys(openHeld).forEach(function(ticker){
      if(bought[ticker]) return;
      const sh=openHeld[ticker];
      if(sh==null || !isFinite(sh)) return;
      const p=ovPx(ticker, d.date);
      const y=prev ? ovPx(ticker, prev) : null;
      if(y && p && isFinite(y.c) && isFinite(p.o)) held += sh*(p.o-y.c);
      if(!sold[ticker] && p && isFinite(p.o) && isFinite(p.c)) held += sh*(p.c-p.o);
    });
    return {date:d.date, held:held, neu:neu, bought:d.bought||[], sold:d.sold||[]};
  });
}
function ovHeat(v, scale, kind){
  if(v==null || !isFinite(v) || Math.abs(v)<0.5) return 'rgba(148,163,184,.12)';
  const a=Math.min(1, Math.abs(v)/scale);
  const pos=v>0;
  if(kind==='held'){
    return pos ? 'rgba(251,191,36,'+(0.22+0.78*a)+')' : 'rgba(248,113,113,'+(0.22+0.78*a)+')';
  }
  return pos ? 'rgba(74,222,128,'+(0.22+0.78*a)+')' : 'rgba(252,165,165,'+(0.22+0.78*a)+')';
}
function ovScale(rows){
  const xs=[];
  rows.forEach(function(r){ xs.push(Math.abs(r.held||0)); xs.push(Math.abs(r.neu||0)); });
  xs.sort(function(a,b){return a-b;});
  const p=xs.length ? xs[Math.floor(xs.length*0.9)] : 200;
  return Math.max(180, p||200);
}
function ovWeeks(rows){
  if(!rows.length) return [];
  const by={}; rows.forEach(function(r){ by[r.date]=r; });
  const a=rows[0].date, b=rows[rows.length-1].date;
  const start=new Date(a+'T12:00:00');
  const end=new Date(b+'T12:00:00');
  const dow0=start.getDay();
  start.setDate(start.getDate()-(dow0===0?6:dow0-1));
  const weeks=[]; let week=[];
  for(let t=new Date(start); t<=end || week.length; t.setDate(t.getDate()+1)){
    const dow=t.getDay();
    if(dow===0||dow===6){ if(dow===6 && week.length){ weeks.push(week); week=[]; } if(t>end && !week.length) break; continue; }
    const iso=t.toISOString().slice(0,10);
    week.push(by[iso]||{date:iso,empty:true});
    if(week.length===5){ weeks.push(week); week=[]; }
    if(t>end && !week.length) break;
  }
  if(week.length) weeks.push(week);
  return weeks;
}
function ovFmt(v){
  if(v==null || !isFinite(v)) return '—';
  return (v>0?'+':'')+v.toFixed(0);
}
function ovCalHtml(name){
  const rows=ovSplitDays(name);
  if(!rows.length) return '<div class="ovcal-card"><div class="nm">'+esc(name)+'</div><div class="mut">no daily book</div></div>';
  const scale=ovScale(rows);
  let heldS=0, neuS=0, heldG=0, neuG=0, n=rows.length;
  rows.forEach(function(r){
    heldS+=r.held||0; neuS+=r.neu||0;
    if((r.held||0)>0) heldG++;
    if((r.neu||0)>0) neuG++;
  });
  const on = sleeveFilter===name ? ' on' : '';
  let h='<div class="ovcal-card'+on+'" data-sleeve="'+esc(name)+'">';
  h+='<div class="nm">'+esc(name)+'</div>';
  h+='<div class="tot"><span class="'+(heldS>=0?'pos':'neg')+'">held '+ovFmt(heldS)+'</span>';
  h+='<span class="'+(neuS>=0?'pos':'neg')+'">new '+ovFmt(neuS)+'</span>';
  h+='<span class="mut">new+ '+neuG+'/'+n+' · held+ '+heldG+'/'+n+'</span></div>';
  h+='<div class="ovcal-wk">';
  ['M','T','W','T','F'].forEach(function(d){ h+='<div class="ovcal-dow">'+d+'</div>'; });
  ovWeeks(rows).forEach(function(w){
    w.forEach(function(cell){
      if(cell.empty){ h+='<div class="ovcal-cell empty"></div>'; return; }
      const dayn=String(cell.date).slice(8,10).replace(/^0/,'');
      const tip=cell.date+'  held '+ovFmt(cell.held)+'  new buys '+ovFmt(cell.neu)
        +(cell.bought&&cell.bought.length?('  B '+cell.bought.join(',')):'');
      h+='<div class="ovcal-cell" data-tip="'+esc(tip)+'" title="'+esc(tip)+'">';
      h+='<span class="d">'+esc(dayn)+'</span>';
      h+='<div class="half" style="background:'+ovHeat(cell.held,scale,'held')+'"></div>';
      h+='<div class="half" style="background:'+ovHeat(cell.neu,scale,'new')+'"></div>';
      h+='</div>';
    });
  });
  h+='</div></div>';
  return h;
}
function renderOvCal(){
  const box=document.getElementById('ovCal');
  const tip=document.getElementById('ovCalTip');
  if(!box) return;
  const list=statsList();
  const names=(sleeveFilter!=='all' && list.some(function(s){return s.name===sleeveFilter;}))
    ? [sleeveFilter]
    : list.map(function(s){return s.name;}).filter(function(n){return (D.daily||{})[n];});
  box.innerHTML='<div class="ovcal-grid">'+names.map(ovCalHtml).join('')+'</div>';
  box.onclick=function(ev){
    const cell=ev.target.closest('.ovcal-cell[data-tip]');
    if(cell){ if(tip) tip.textContent=cell.getAttribute('data-tip'); return; }
    const card=ev.target.closest('.ovcal-card[data-sleeve]');
    if(card){
      const nm=card.getAttribute('data-sleeve');
      if(nm && nm!==sleeveFilter) selectSleeve(nm);
    }
  };
}
