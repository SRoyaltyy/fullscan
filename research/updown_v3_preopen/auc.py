import pandas as pd, numpy as np, json
from scipy.stats import rankdata
out={}
for v,f in [('A','out/preds.parquet'),('B','out/preds_B.parquet')]:
    P=pd.read_parquet(f); P=P[P.y.notna()]
    for h,g in P.groupby('h'):
        H={'1d':1,'5d':5,'21d':21,'63d':63}[h]
        def auc(x):
            y=x.y.values; n1=y.sum(); n0=len(y)-n1
            if n1==0 or n0==0: return np.nan
            r=rankdata(x.p.values); return (r[y==1].sum()-n1*(n1+1)/2)/(n1*n0)
        a=g.groupby('date').apply(auc).dropna(); s=a.iloc[::H]
        out[f'{v}_{h}']=dict(auc=round(float(a.mean()),4),t=round(float((s.mean()-.5)/(s.std()/np.sqrt(len(s)))),2))
print(out); json.dump(out,open('out/auc.json','w'))
