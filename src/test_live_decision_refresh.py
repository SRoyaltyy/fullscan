"""Execute the actual browser snippets with a small fake DOM/network."""
import json
from pathlib import Path
import subprocess
from . import book_suggestions as bs

ROOT = Path(__file__).resolve().parent.parent


def node(script):
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True)


def test_published_cache_cannot_override_newer_live_decisions():
    script = bs._POLLER_JS.split('>',1)[1].rsplit('</script>',1)[0]
    node('''const assert=require('node:assert/strict');
    const raw={date:'2026-09-17', generated_at:'2026-09-17T09:20:00-04:00',strategies:{NEW:{buy:['NEW']}}};
    const old={...raw,generated_at:'2026-09-17T09:00:00-04:00',strategies:{OLD:{buy:['OLD']}}};
    const el={innerHTML:''};
    global.document={getElementById:()=>el,querySelectorAll:()=>[]};
    global.window={}; global.setInterval=()=>{};
    global.fetch=async url=>({ok:true,json:async()=>url.includes('today_strategies')?(url.startsWith('https:')?raw:old):{}});
    ''' + script + '''
    setImmediate(()=>{assert.match(el.innerHTML,/NEW/);assert.doesNotMatch(el.innerHTML,/OLD/);});''')


def test_factor_live_strip_refreshes_without_reloading_page():
    source=(ROOT/'src/factor_mine_dash.html').read_text()
    a=source.index('(function(){\n  var RAW=');b=source.index('\n})();',a)+len('\n})();')
    node('''const assert=require('node:assert/strict');
    let version=1,refresh;
    const elements={liveDayStamp:{},liveDayPills:{}};
    global.document={getElementById:id=>elements[id]};global.window={};
    global.setInterval=(fn,ms)=>{assert.equal(ms,10000);refresh=fn;};
    global.fetch=async()=>({ok:true,json:async()=>({date:'2026-09-17',strategies:{},n:version})});
    '''+source[a:b]+'''
    setImmediate(async()=>{
      assert.equal(window.__TODAY_STRATS.n,1);version=2;refresh();
      setImmediate(()=>assert.equal(window.__TODAY_STRATS.n,2));
    });''')


def test_baked_poller_update_preserves_historical_pack(tmp_path):
    path=tmp_path/'index.html'
    path.write_text('HISTORICAL_DATA\n(function(){\n  var RAW="old";\n})();\nUNCHANGED')
    assert bs.refresh_factor_live_poller(path)
    text=path.read_text()
    assert text.startswith('HISTORICAL_DATA\n') and text.endswith('\nUNCHANGED')
    assert 'setInterval(loadLiveDay,10000)' in text
    assert not bs.refresh_factor_live_poller(path)
