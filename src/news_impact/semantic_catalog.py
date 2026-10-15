"""Public company facts for model disambiguation; never event/judge answers."""
import csv,gzip,hashlib,json,re
from pathlib import Path

GENERIC=frozenset('the first global united national general energy financial technology capital power water news people future now limited services group holdings international solution solutions insider above below'.split())
LEGAL_WORDS=r'\b(?:incorporated|corporation|limited|inc|corp|ltd|plc|co|company|holdings|group|llc|llp|pte|pty|gmbh|ag|nv|asa|oyj|class [abc])\b'
IDENTITY_LEGAL_WORDS=r'\b(?:incorporated|corporation|limited|inc|corp|ltd|plc|co|llc|llp|pte|pty|gmbh|ag|nv|asa|oyj|class [abc])\b'

def name_key(value):
    value=re.sub(LEGAL_WORDS,' ',value.lower())
    return re.sub(r'[^a-z0-9]+',' ',value).strip()

def identity_key(value):
    # Group/Holdings/Company can distinguish global namesakes. Retain them
    # while removing legal suffixes such as AG, Pty Ltd and Corporation.
    value=re.sub(IDENTITY_LEGAL_WORDS,' ',value.lower())
    return re.sub(r'[^a-z0-9]+',' ',value).strip()

def phrase(text,value):
    return re.search(r'(?<![a-z0-9])'+re.escape(value)+r'(?![a-z0-9])',text) is not None

class CompanyCatalog:
    def __init__(self,root=None,path=None,identity_path=None):
        root=Path(root) if root else Path(__file__).resolve().parents[2]
        folder=root/'data/exports'
        path=Path(path) if path else folder/'finviz_latest.csv'
        if not path.is_file():
            files=sorted(folder.glob('finviz_20*.csv'));path=files[-1] if files else None
        self.rows=[];self.identity_rows=[]
        self.metadata={'available':False,'source':None,'sha256':None,'rows':0}
        seen=set()
        if path is not None and path.is_file():
            self.metadata={'available':True,'source':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'rows':0}
            with path.open(newline='',encoding='utf-8-sig',errors='replace') as fh:
                for raw in csv.DictReader(fh):
                    row={k:str(raw.get(k) or '').strip() for k in ('Ticker','Company','Sector','Industry')}
                    key=(row['Ticker'],row['Company'])
                    if not all(row.values()) or key in seen:continue
                    seen.add(key);row['_name']=name_key(row['Company']);self.rows.append(row)
        self.metadata['rows']=len(self.rows)
        self.metadata['available']=bool(self.rows)
        identity_folder=path.parent if path else folder
        if identity_path is None:
            compressed=identity_folder/'wikidata_companies.json.gz'
            identity_path=compressed if compressed.is_file() else identity_folder/'wikidata_companies.json'
        identity_path=Path(identity_path)
        if identity_path.is_file():
            body=identity_path.read_bytes()
            raw_rows=json.loads(gzip.decompress(body) if identity_path.suffix=='.gz' else body)
            if not isinstance(raw_rows,list) or not raw_rows:raise ValueError('Identity catalog must contain business fact rows')
            identity_ids=set()
            for raw in raw_rows:
                if (not isinstance(raw,dict) or not isinstance(raw.get('id'),str)
                    or not isinstance(raw.get('Company'),str) or not raw['Company'].strip()
                    or not isinstance(raw.get('description'),str)
                    or not isinstance(raw.get('industries'),list)
                    or not (raw['industries'] or raw['description'].strip())
                    or any(not isinstance(x,str) or not x.strip() for x in raw['industries'])):
                    raise ValueError('Invalid identity catalog business facts')
                if raw['id'] in identity_ids:raise ValueError('Duplicate identity catalog entity')
                identity_ids.add(raw['id'])
                self.identity_rows.append({'Company':raw['Company'],'Industry':' / '.join(raw['industries']),
                    'Description':raw['description'][:300],'identity_source':'wikidata:'+raw['id'],
                    '_name':identity_key(raw['Company'])})
            sources=[]
            if self.rows:sources.append({'source':path.name,'sha256':self.metadata['sha256'],'rows':len(self.rows)})
            sources.append({'source':identity_path.name,'sha256':hashlib.sha256(identity_path.read_bytes()).hexdigest(),'rows':len(self.identity_rows)})
            self.metadata={'available':True,'source':'frozen_business_catalogs','sources':sources,
                'sha256':hashlib.sha256(json.dumps(sources,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                'rows':len(self.rows)+len(self.identity_rows)}
    def candidates(self,title):
        # Strip publisher suffix only for retrieval, never change model input.
        text=title.rsplit(' - ',1)[0];low=name_key(text);identity_low=identity_key(text);hits=[]
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
        for row in self.identity_rows:
            key=row['_name'];words=key.split()
            # Global ticker symbols are ambiguous across exchanges. Match only
            # complete business names; the model still decides identity/relevance.
            if not key or key not in identity_low or not phrase(identity_low,key):continue
            if len(words)<2 and (len(words[0])<5 or words[0] in GENERIC):continue
            if len(words)==1:
                literal_name=re.sub(IDENTITY_LEGAL_WORDS,' ',row['Company'],flags=re.I).strip(' .,')
                if not literal_name or not phrase(text,literal_name):continue
            hits.append({k:v for k,v in row.items() if k!='_name'}|{'match':'name_identity_candidate'})
        hits.sort(key=lambda x:(x['match']!='explicit_ticker',x['match']=='name_identity_candidate',x['Company'],x.get('Ticker','')))
        return hits[:12]
