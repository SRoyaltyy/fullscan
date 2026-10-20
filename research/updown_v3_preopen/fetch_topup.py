import yfinance as yf, pandas as pd, time, sys
tk=[t.strip() for t in open('tickers.txt') if t.strip()]
out=[]
B=200
for i in range(0,len(tk),B):
    chunk=tk[i:i+B]
    ysym=[t.replace('.','-') for t in chunk]
    for attempt in range(3):
        try:
            df=yf.download(ysym,start='2026-09-01',end='2026-10-20',interval='1d',auto_adjust=False,actions=False,group_by='ticker',threads=True,progress=False)
            break
        except Exception as e:
            print('err',e,file=sys.stderr); time.sleep(5)
    for t,y in zip(chunk,ysym):
        if y not in df.columns.get_level_values(0): continue
        s=df[y].dropna(subset=['Open','Close'])
        if s.empty: continue
        s=s.reset_index().rename(columns={'Date':'date','Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'})
        s['ticker']=t
        out.append(s[['date','ticker','open','high','low','close','volume']])
    print(i,len(out),flush=True)
pd.concat(out).to_parquet('topup2_bars.parquet')
print('done')
