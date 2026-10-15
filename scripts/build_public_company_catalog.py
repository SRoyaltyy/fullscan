"""Build a broad public identity snapshot, independently of evaluation titles.

Wikidata CC0 business facts are candidates for model disambiguation, never
event labels, public-authority labels, or eleven-sector benchmark answers.
This builder deliberately accepts no title, fixture, or benchmark inputs.
"""
import argparse,datetime,hashlib,json,pathlib,time,urllib.error,urllib.parse,urllib.request

ENDPOINT="https://query.wikidata.org/sparql"

def query_text(limit):
    return """SELECT DISTINCT ?company ?companyLabel ?companyDescription ?ticker ?industry ?industryLabel
WHERE {
  ?company wdt:P249 ?ticker; wdt:P452 ?industry.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?company ?industry ?ticker
LIMIT """+str(limit)

def build(output,limit):
    query=query_text(limit)
    url=ENDPOINT+"?"+urllib.parse.urlencode({"query":query,"format":"json"})
    req=urllib.request.Request(url,headers={
        "User-Agent":"FullscanCompanyCatalog/1.0 (https://github.com/SRoyaltyy/fullscan; public business identity snapshot)",
        "Accept":"application/sparql-results+json"})
    response=None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req,timeout=75) as resp:
                response=resp.read(30_000_001)
            if len(response)>30_000_000:raise ValueError("Catalog response exceeds bounded download")
            break
        except (urllib.error.HTTPError,urllib.error.URLError,TimeoutError):
            if attempt:raise
            time.sleep(30)
    data=json.loads(response)
    bindings=data["results"]["bindings"]
    companies={}
    def value(row,key):return row.get(key,{}).get("value","").strip()
    for row in bindings:
        uri=value(row,"company");qid=uri.rsplit("/",1)[-1]
        name=value(row,"companyLabel");industry=value(row,"industryLabel")
        if not qid.startswith("Q") or not name or name==qid or not industry:continue
        target=companies.setdefault(qid,{"id":qid,"Company":name,
            "description":value(row,"companyDescription"),"tickers":set(),"industries":set()})
        target["tickers"].add(value(row,"ticker"));target["industries"].add(industry)
    rows=[r|{"tickers":sorted(r["tickers"]-{""}),"industries":sorted(r["industries"]-{""})}
          for _,r in sorted(companies.items())]
    if len(rows)<100:raise ValueError("Insufficient identity coverage; never install an empty catalog")
    output.mkdir(parents=True,exist_ok=True)
    body=(json.dumps(rows,ensure_ascii=False,indent=2)+"\n").encode()
    (output/"wikidata_companies.json").write_bytes(body)
    metadata={"generated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source":ENDPOINT,"license":"Wikidata structured data CC0",
        "scope":"Broad listed-company identity/industry snapshot; incomplete worldwide coverage, not benchmark-selected",
        "query":query,"requested_binding_limit":limit,"bindings":len(bindings),"companies":len(rows),
        "sha256":hashlib.sha256(body).hexdigest(),
        "raw_response_sha256":hashlib.sha256(response).hexdigest(),
        "contains_sector_labels":False,"contains_event_or_action_labels":False}
    (output/"provenance.json").write_text(json.dumps(metadata,indent=2)+"\n")
    print(json.dumps(metadata))

def main():
    p=argparse.ArgumentParser();p.add_argument("--output",type=pathlib.Path,required=True)
    p.add_argument("--limit",type=int,default=20000);args=p.parse_args()
    if not 100<=args.limit<=50000:p.error("Binding limit must be 100..50000")
    build(args.output,args.limit)

if __name__=="__main__":main()
