"""Public company facts for model disambiguation; never event/judge answers."""
import csv,hashlib,re
from pathlib import Path

GENERIC=frozenset('the first global united national general energy financial technology capital power water news people future now limited services group holdings international'.split())

def name_key(value):
    value=re.sub(r'\b(?:incorporated|corporation|limited|inc|corp|ltd|plc|co|company|holdings|group|class [abc])\b',' ',value.lower())
    return re.sub(r'[^a-z0-9]+',' ',value).strip()

def phrase(text,value):
    return re.search(r'(?<![a-z0-9])'+re.escape(value)+r'(?![a-z0-9])',text) is not None

class CompanyCatalog:
    def __init__(self,root=None,path=None):
        root=Path(root) if root else Path(__file__).resolve().parents[2]
        folder=root/'data/exports'
        path=Path(path) if path else folder/'finviz_latest.csv'
        if not path.is_file():
            files=sorted(folder.glob('finviz_20*.csv'));path=files[-1] if files else None
        self.rows=[]
        self.metadata={'available':False,'source':None,'sha256':None,'rows':0}
        if path is None or not path.is_file():return
        self.metadata={'available':True,'source':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'rows':0}
        seen=set()
        with path.open(newline='',encoding='utf-8-sig',errors='replace') as fh:
            for raw in csv.DictReader(fh):
                row={k:str(raw.get(k) or '').strip() for k in ('Ticker','Company','Sector','Industry')}
                key=(row['Ticker'],row['Company'])
                if not all(row.values()) or key in seen:continue
                seen.add(key);row['_name']=name_key(row['Company']);self.rows.append(row)
        self.metadata['rows']=len(self.rows)
        self.metadata['available']=bool(self.rows)
    def candidates(self,title):
        # Strip publisher suffix only for retrieval, never change model input.
        text=title.rsplit(' - ',1)[0];low=name_key(text);hits=[]
        # Parse complete ticker tokens once. The previous prefix match could
        # mistake the exchange in (LSE:XYZ) for a US issuer whose ticker is LSE.
        explicit=set(re.findall(r'\$([A-Z][A-Z0-9.-]*)(?![A-Za-z0-9])',text))
        explicit.update(re.findall(r'\((?:(?:NASDAQ|NYSE|NYSEAMERICAN|AMEX|LSE|TSX|TSXV|ASX|HKEX|NSE|BSE):\s*)?([A-Z][A-Z0-9.-]*)\)',text))
        explicit={ticker.removesuffix('.US') for ticker in explicit}
        for row in self.rows:
            strength=None
            if row['Ticker'] in explicit:strength='explicit_ticker'
            elif row['_name'] and row['_name'] in low and phrase(low,row['_name']):
                words=row['_name'].split()
                if len(words)>=2 or (len(words[0])>=5 and words[0] not in GENERIC):strength='name_candidate'
                # Short distinctive legal names (e.g. three-letter companies)
                # need their complete legal name, not an ordinary-word alias.
                elif row['Company'].casefold() in text.casefold():strength='legal_name'
            if strength:
                hits.append({k:row[k] for k in ('Ticker','Company','Sector','Industry')}|{'match':strength})
        hits.sort(key=lambda x:(x['match']!='explicit_ticker',x['Company'],x['Ticker']))
        return hits[:12]
