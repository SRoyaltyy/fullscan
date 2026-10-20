"""Portable profile cache reader. JSON only; never execute serialized objects."""
import gzip
import json
from collections import defaultdict
from .stock_evidence import COMPILER_VERSION,Fact,Profile,index_profiles


def load_profiles(path, *, as_of=None, tickers=None, progress=None):
    profiles={}
    with gzip.open(path,'rt',encoding='utf-8') as stream:
        meta=json.loads(next(stream))
        if meta.get('format')!='stock-evidence-1':raise ValueError('unsupported profile cache')
        if as_of and (not meta.get('as_of') or meta['as_of'] > as_of):
            raise ValueError('profile cache cutoff is absent or later than requested as-of date')
        if meta.get('compiler_version')!=COMPILER_VERSION:
            raise ValueError('profile cache compiler is absent or incompatible; rebuild from original sources')
        for line in stream:
            d=json.loads(line)
            if tickers is not None and d['ticker'] not in tickers:
                continue
            d['core']=set(d['core'])
            d['buckets']=defaultdict(set,{k:set(v) for k,v in d['buckets'].items()})
            d['facts']=[Fact(**{'text':'',**f}) for f in d['facts']]
            d['locations']=set(d['locations'])
            d['evidence']=defaultdict(list,d['evidence'])
            p=Profile(**d);profiles[p.ticker]=p
            if progress and len(profiles)%100==0:progress({'profiles_loaded':len(profiles)})
    if not profiles:raise ValueError('empty profile cache or requested universe')
    if tickers is not None and tickers-set(profiles):
        raise ValueError('profile cache is missing requested tickers: '+','.join(sorted(tickers-set(profiles))[:20]))
    if progress:progress({'indexing_profiles':len(profiles)})
    return index_profiles(profiles),meta
