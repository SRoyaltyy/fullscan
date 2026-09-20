
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
const _ovBookCache={};
let _ovPickedDate='';
function ovSimBook(name){
  if(_ovBookCache[name]) return _ovBookCache[name];
  if(typeof FMSim==='undefined' || !D.sim) return null;
  const rec=(D.recipes||[]).find(function(r){return r.name===name;});
  if(!rec) return null;
  const start=(D.dates||[])[0];
  if(!start) return null;
  try{ _ovBookCache[name]=FMSim.simulateBook(D.sim, rec, start, D.mornings||{}); }
  catch(e){ _ovBookCache[name]=null; }
  return _ovBookCache[name];
}
function ovDaysAndTrades(name){
  let days=((D.daily||{})[name]||[]).slice();
  let trades=(((D.books||{})[name]||{}).trades||[]).filter(function(t){return (t.side==='BUY'||t.side==='SHORT') && t.ticker;});
  if(!days.length || !trades.length){
    const b=ovSimBook(name);
    if(b){
      if(!days.length) days=b.daily||[];
      if(!trades.length) trades=(b.trades||[]).filter(function(t){return (t.side==='BUY'||t.side==='SHORT') && t.ticker;});
    }
  }
  days=days.slice().sort(function(a,b){return String(a.date).localeCompare(String(b.date));});
  return {days:days, trades:trades};
}
function ovSplitDays(name){
  const pack=ovDaysAndTrades(name);
  const days=pack.days, trades=pack.trades;
  const dates=days.map(function(d){return d.date;});
  return days.map(function(d){
    const bought={}; (d.bought||[]).forEach(function(t){ bought[String(t).split('×')[0]]=true; });
    const sold={}; (d.sold||[]).forEach(function(t){ sold[String(t).split('×')[0]]=true; });
    let neu=0, held=0;
    const buyLots=[];
    trades.forEach(function(t){
      if(t.date!==d.date) return;
      const p=ovPx(t.ticker, d.date);
      let pnl=null;
      if(p && isFinite(p.c) && isFinite(t.price)){
        pnl = Number(t.shares||0)*(p.c-Number(t.price));
        if(t.fees) pnl -= Number(t.fees);
        neu += pnl;
      }
      buyLots.push({ticker:t.ticker, shares:t.shares, price:t.price, pnl:pnl});
    });
    const prev=ovPrevDate(d.date, dates);
    const openHeld=ovParseHeld(d.open_held);
    const heldLots=[];
    Object.keys(openHeld).forEach(function(ticker){
      if(bought[ticker]) return;
      const sh=openHeld[ticker];
      let gap=0, sess=0;
      const p=ovPx(ticker, d.date);
      const y=prev ? ovPx(ticker, prev) : null;
      if(sh!=null && isFinite(sh)){
        if(y && p && isFinite(y.c) && isFinite(p.o)) gap = sh*(p.o-y.c);
        if(!sold[ticker] && p && isFinite(p.o) && isFinite(p.c)) sess = sh*(p.c-p.o);
        held += gap+sess;
      }
      heldLots.push({ticker:ticker, shares:sh, sold:!!sold[ticker], pnl:gap+sess});
    });
    const hard = !!d.hard_red || (d.s!=null && Number(d.s)<=-3);
    return {
      date:d.date, held:held, neu:neu, bought:d.bought||[], sold:d.sold||[],
      hard_red:hard, s:d.s, buyLots:buyLots, heldLots:heldLots
    };
  });
}
function ovHeat(v, scale){
  if(v==null || !isFinite(v) || Math.abs(v)<0.5) return 'rgba(148,163,184,.16)';
  const a=Math.min(1, Math.abs(v)/scale);
  if(v>0) return 'rgba(22,163,74,'+(0.20+0.80*a)+')';
  return 'rgba(220,38,38,'+(0.20+0.80*a)+')';
}
function ovScale(rows){
  const xs=[];
  rows.forEach(function(r){ xs.push(Math.abs(r.held||0)); xs.push(Math.abs(r.neu||0)); });
  xs.sort(function(a,b){return a-b;});
  const p=xs.length ? xs[Math.floor(xs.length*0.9)] : 200;
  return Math.max(80, p||200);
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
  const n=Math.round(v);
  return (n>0?'+':'')+n;
}
function ovNameList(arr){
  if(!arr || !arr.length) return '<span class="mut">none</span>';
  return arr.map(function(x){
    if(typeof x==='string') return '<code>'+esc(x)+'</code>';
    const pnl=x.pnl==null?'':(' <span class="'+(x.pnl>=0?'pos':'neg')+'">'+ovFmt(x.pnl)+'</span>');
    const sh=x.shares!=null?(' ×'+x.shares):'';
    const tag=x.sold?' sold':'';
    return '<code>'+esc(x.ticker)+sh+tag+'</code>'+pnl;
  }).join(' · ');
}
function ovDetailHtml(row){
  if(!row) return '<div class="mut">Tap a day for names bought today vs already held.</div>';
  const s=row.s==null?'':(' S='+(Number(row.s)>=0?'+':'')+Number(row.s).toFixed(2));
  const hard=row.hard_red?' · HARD RED':'';
  return '<div class="ovcal-detail-h">'+esc(row.date)+s+hard+'</div>'
    +'<div><b>Held overnight</b> <span class="'+(row.held>=0?'pos':'neg')+'">'+ovFmt(row.held)+'</span><div class="ovcal-names">'+ovNameList(row.heldLots)+'</div></div>'
    +'<div><b>Bought today</b> <span class="'+(row.neu>=0?'pos':'neg')+'">'+ovFmt(row.neu)+'</span><div class="ovcal-names">'+ovNameList(row.buyLots)+'</div></div>';
}
function ovCalHtml(name, mode){
  const big=mode==='big';
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
  let h='<div class="ovcal-card'+(big?' big':'')+on+'" data-sleeve="'+esc(name)+'">';
  h+='<div class="nm">'+esc(name)+'</div>';
  h+='<div class="tot"><span class="'+(heldS>=0?'pos':'neg')+'">held '+ovFmt(heldS)+'</span>';
  h+='<span class="'+(neuS>=0?'pos':'neg')+'">new '+ovFmt(neuS)+'</span>';
  h+='<span class="mut">new+ '+neuG+'/'+n+' · held+ '+heldG+'/'+n+'</span></div>';
  h+='<div class="ovcal-wk'+(big?' big':'')+'">';
  ['M','T','W','T','F'].forEach(function(d){ h+='<div class="ovcal-dow">'+d+'</div>'; });
  ovWeeks(rows).forEach(function(w){
    w.forEach(function(cell){
      if(cell.empty){ h+='<div class="ovcal-cell empty"></div>'; return; }
      const dayn=String(cell.date).slice(8,10).replace(/^0/,'');
      const cls=['ovcal-cell', big?'big':'', cell.hard_red?'hard':'', _ovPickedDate===cell.date?'pick':''].filter(Boolean).join(' ');
      const tip=cell.date+'  held '+ovFmt(cell.held)+'  new '+ovFmt(cell.neu)+(cell.hard_red?'  HARD RED':'');
      h+='<div class="'+cls+'" data-date="'+esc(cell.date)+'" data-tip="'+esc(tip)+'" title="'+esc(tip)+'">';
      h+='<span class="d">'+esc(dayn)+(cell.hard_red?' R':'')+'</span>';
      h+='<div class="half" style="background:'+ovHeat(cell.held,scale)+'"><span>'+ovFmt(cell.held)+'</span></div>';
      h+='<div class="half" style="background:'+ovHeat(cell.neu,scale)+'"><span>'+ovFmt(cell.neu)+'</span></div>';
      h+='</div>';
    });
  });
  h+='</div>';
  if(big) h+='<div class="ovcal-detail" id="ovCalDetail">'+ovDetailHtml(rows.filter(function(r){return r.date===_ovPickedDate;})[0])+'</div>';
  h+='</div>';
  return h;
}
function ovJumpDate(date){
  const ds=D.dates||[];
  const i=ds.indexOf(date);
  if(i<0) return;
  _ovPickedDate=date;
  startIdx=i;
  probeIdx=i;
  _simKey=''; _simBook=null;
  if(typeof syncProbeTickToBuys==='function') syncProbeTickToBuys(date);
  renderAll();
}
function ovOnCalClick(ev){
  const cell=ev.target.closest('.ovcal-cell[data-date]');
  if(cell){
    ovJumpDate(cell.getAttribute('data-date'));
    return;
  }
  const card=ev.target.closest('.ovcal-card[data-sleeve]');
  if(card){
    const nm=card.getAttribute('data-sleeve');
    if(nm && nm!==sleeveFilter) selectSleeve(nm);
  }
}
function ovChartDates(){
  const ds=D.dates||[];
  if(!pickedName()) return ds;
  const sp=startPath();
  if(!sp || !sp.start) return ds;
  const i=ds.indexOf(sp.start);
  return i>0 ? ds.slice(i) : ds;
}
function ovAlignSeries(eq, start){
  const ds=D.dates||[];
  const i=start?ds.indexOf(start):0;
  if(!eq || !eq.length) return ds.map(function(){return null;});
  if(eq.length===ds.length) return i>0 ? eq.map(function(v,j){return j<i?null:v;}) : eq;
  const out=ds.map(function(){return null;});
  eq.forEach(function(v,j){ const k=(i>0?i:0)+j; if(k>=0&&k<out.length) out[k]=v; });
  return out;
}
function ovSeriesFromDaily(days, start){
  const ds=D.dates||[];
  const i=start?ds.indexOf(start):0;
  const by={};
  (days||[]).forEach(function(d){ if(d && d.equity!=null) by[d.date]=d.equity; });
  return ds.map(function(d,j){
    if(i>0 && j<i) return null;
    return by[d]==null?null:by[d];
  });
}
function renderOvCal(){
  const box=document.getElementById('ovCal');
  const tip=document.getElementById('ovCalTip');
  const side=document.getElementById('ovCalSleeve');
  const list=statsList();
  const picked=sleeveFilter!=='all' && list.some(function(s){return s.name===sleeveFilter;});
  const names=picked
    ? [sleeveFilter]
    : list.map(function(s){return s.name;}).filter(function(n){
        return ((D.daily||{})[n] && (D.daily||{})[n].length) || ((D.recipes||[]).some(function(r){return r.name===n;}));
      });
  if(box){
    box.innerHTML='<div class="ovcal-grid">'+names.map(function(n){return ovCalHtml(n,'mini');}).join('')+'</div>';
    box.onclick=ovOnCalClick;
  }
  if(side){
    if(picked){
      side.innerHTML=ovCalHtml(sleeveFilter,'big');
      side.style.display='block';
      side.onclick=ovOnCalClick;
    } else {
      side.innerHTML='';
      side.style.display='none';
    }
  }
  if(tip && _ovPickedDate) tip.textContent='Selected '+_ovPickedDate+' — names under the large calendar.';
}
