"""On-demand stock parsing. One verified input file, two isolated parser processes.

This is experimental execution infrastructure, never an accuracy certification.
No parser rules, extraction decisions or filing collection behavior are changed.
"""
import argparse, csv, gzip, hashlib, io, json, os, re, shutil, subprocess, sys, tempfile, time
from pathlib import Path

REPO = 'SRoyaltyy/fullscan'
OLD = '7adb09dd5091fb51ac68859645a26e2292951e46'
NEW = '4d221aa78f21d1968fe4878998a2ac9455d3b6aa'
RESULT_BRANCH = 'scanner-request-results'

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()

def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode(value))

def validate_request(ticker, request_id, old_sha, new_sha):
    if not re.fullmatch(r'[A-Z0-9][A-Z0-9.^/-]{0,19}', ticker):
        raise ValueError('invalid ticker')
    if not re.fullmatch(r'[a-zA-Z0-9-]{16,80}', request_id):
        raise ValueError('invalid request ID')
    for value in (old_sha, new_sha):
        if not re.fullmatch(r'[a-f0-9]{40}', value):
            raise ValueError('parser revisions must be full commit hashes')
    if old_sha == new_sha:
        raise ValueError('old and new revisions must differ')

def git_revision(root):
    return subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()

def prepare(catalog_path, parser_root, ticker, out):
    """Use the archive's existing selective reader; never download the whole market."""
    sys.path.insert(0, str(parser_root.resolve()))
    from scripts.catalog_stock_rollout import remote_zip
    from scripts.materialize_filing_texts import materialize
    from scripts.materialize_completed_profiles import fetch_url
    from src.filing_evidence import load_filing_sections
    started = time.monotonic()
    catalog_raw = Path(catalog_path).read_bytes()
    catalog = json.loads(catalog_raw)
    row = catalog['catalog'][ticker]
    csvraw = fetch_url('https://raw.githubusercontent.com/'+REPO+'/'+catalog['baseline_source_sha']+'/excel_bot/data/finviz_with_descriptions.csv')
    if digest(csvraw) != catalog['description_csv_sha256']:
        raise ValueError('description catalog checksum mismatch')
    descriptions = {}
    for item in csv.DictReader(io.StringIO(csvraw.decode('utf-8-sig'))):
        key = str(item.get('Ticker') or item.get('ticker') or '').upper().strip()
        descriptions.setdefault(key, item)
    item = descriptions.get(ticker, {})
    description = item.get('Finviz_Description') or item.get('description') or ''
    archive_id = None
    as_of = catalog['as_of']
    if row['coverage'] == 'filing_backed':
        match = re.fullmatch(r'filings-(\d+)-\d+', row['batch'])
        if not match:
            raise ValueError('catalog lacks an archive-shard mapping')
        release_path = parser_root/'data/company_filings_full_release.json'
        release = json.loads(release_path.read_bytes())
        archive = next(a for a in release['source_artifacts'] if a['shard'] == int(match[1]))
        archive_id = archive['id']
        cik = row['cik']
        with tempfile.TemporaryDirectory() as temporary:
            with remote_zip(archive_id) as source:
                company = json.loads(source.read('objects/company-filings/full-v1/'+cik+'/company.json'))
                if ticker not in company['tickers']:
                    raise ValueError('requested ticker does not belong to archived issuer')
                materialize(source, Path(temporary), as_of=as_of, tickers=set(company['tickers']))
            grouped, receipt = load_filing_sections(Path(temporary)/'objects', as_of=as_of, strict=True, ciks={cik})
            if len(grouped) != 1:
                raise ValueError('selected archive must contain one issuer')
            parts = next(iter(grouped.values()))
            if not parts:
                raise ValueError('no complete stored source text for this issuer')
    elif row['coverage'] == 'description_only':
        cik = None
        parts = []
        receipt = {'filings':0, 'documents':0, 'characters':0,
                   'description_only':True, 'full_source_archive':False, 'complete':False}
    else:
        raise ValueError('unsupported catalog coverage')
    if description:
        parts.append({'source':'catalog_description', 'kind':'business', 'section':'stored description',
                      'document':'catalog description', 'form':'DESCRIPTION', 'filing_date':'',
                      'complete':False, 'text':description})
    receipt.update(description_sha256=digest(description.encode()), description_characters=len(description),
                   description_csv_sha256=digest(csvraw), archive_artifact_id=archive_id)
    payload = {'schema':'stock-parser-input-1', 'ticker':ticker, 'name':row['name'], 'cik':cik,
               'coverage_kind':row['coverage'], 'as_of':as_of, 'sections':parts,
               'source_receipt':receipt, 'catalog_sha256':digest(catalog_raw),
               'section_sha256':[digest(str(p.get('text') or '').encode()) for p in parts]}
    write(out, payload)
    timing = {'fetch_seconds':round(time.monotonic()-started, 3), 'input_sha256':digest(Path(out).read_bytes())}
    write(str(out)+'.receipt.json', timing)
    print(json.dumps(timing), flush=True)
    return payload

def parse(input_path, parser_root, output_path, revision, kind):
    actual = git_revision(parser_root)
    if actual != revision:
        raise ValueError('checked-out parser revision mismatch')
    raw = Path(input_path).read_bytes()
    input_sha = digest(raw)
    source = json.loads(raw)
    for part, expected in zip(source['sections'], source['section_sha256'], strict=True):
        if digest(str(part.get('text') or '').encode()) != expected:
            raise ValueError('source section changed')
    sys.path.insert(0, str(parser_root.resolve()))
    from src.filing_relationships import extract_sections, load_parser, compiler_fingerprint
    started = time.monotonic()
    graph = extract_sections(source['sections'], ticker=source['ticker'], name=source['name'], nlp=load_parser())
    result = {'ticker':source['ticker'], 'name':source['name'], 'source_sha':revision,
              'input_sha256':input_sha, 'compiler_sha256':compiler_fingerprint(),
              'as_of':source['as_of'], 'coverage_kind':source['coverage_kind'],
              'sources':graph['sources'], 'source_receipt':source['source_receipt'],
              'parser_coverage':graph['coverage'], 'experimental':True, 'accuracy_certified':False}
    if kind == 'old':
        from src.stock_keywords import compile_keywords
        keywords = compile_keywords(graph, as_of=source['as_of'], ticker=source['ticker'])
        result.update(keywords=keywords['records'], keyword_evidence=keywords['evidence'],
                      keyword_coverage=keywords['coverage'])
    elif kind == 'new':
        evidence = graph['stock_evidence']
        texts = {s['id']:str(part.get('text') or '') for s, part in zip(graph['sources'], source['sections'], strict=True)}
        for occurrence in evidence['occurrences']:
            text = texts[occurrence['source_id']]
            if text[occurrence['start']:occurrence['end']] != occurrence['text']:
                raise ValueError('candidate occurrence is not an exact input span')
            low = max(0, occurrence['start']-300)
            high = min(len(text), occurrence['end']+500)
            occurrence.update(excerpt=text[low:high], excerpt_start=low)
        result.update(evidence=evidence, method=evidence['schema'])
    else:
        raise ValueError('unknown parser role')
    if digest(Path(input_path).read_bytes()) != input_sha:
        raise ValueError('parser changed the shared input file')
    result['parse_seconds'] = round(time.monotonic()-started, 3)
    write(output_path, result)
    print(json.dumps({'role':kind, 'revision':revision, 'input_sha256':input_sha,
                      'parse_seconds':result['parse_seconds']}), flush=True)
    return result

def assemble(input_path, old_path, new_path, out, request_id, old_sha, new_sha, run_id):
    source = json.loads(Path(input_path).read_bytes())
    validate_request(source['ticker'], request_id, old_sha, new_sha)
    old = json.loads(Path(old_path).read_bytes())
    new = json.loads(Path(new_path).read_bytes())
    input_sha = digest(Path(input_path).read_bytes())
    for role, result, revision in (('old',old,old_sha), ('new',new,new_sha)):
        if result['ticker'] != source['ticker'] or result['source_sha'] != revision or result['input_sha256'] != input_sha:
            raise ValueError(role+' result does not match the requested stock/revision/input')
        if result['source_receipt'] != source['source_receipt']:
            raise ValueError(role+' source receipt mismatch')
    timing = json.loads(Path(str(input_path)+'.receipt.json').read_bytes())
    bundle = {'schema':'stock-parser-comparison-1', 'request_id':request_id, 'ticker':source['ticker'],
              'old_sha':old_sha, 'new_sha':new_sha, 'input_sha256':input_sha,
              'run_id':str(run_id), 'as_of':source['as_of'], 'coverage_kind':source['coverage_kind'],
              'catalog_sha256':source['catalog_sha256'], 'source_receipt':source['source_receipt'],
              'timings':{**timing, 'old_parse_seconds':old['parse_seconds'], 'new_parse_seconds':new['parse_seconds']},
              'old':old, 'new':new, 'experimental':True, 'accuracy_certified':False}
    raw = gzip.compress(encode(bundle), mtime=0)
    if len(raw) >= 90_000_000:
        raise ValueError('comparison exceeds the result publication size limit')
    out.mkdir(parents=True, exist_ok=True)
    (out/'comparison.json.gz').write_bytes(raw)
    summary = {k:bundle[k] for k in ('schema','request_id','ticker','old_sha','new_sha','input_sha256',
                                    'run_id','catalog_sha256','coverage_kind','timings')}
    summary.update(sha256=digest(raw), bytes=len(raw), experimental=True, accuracy_certified=False)
    write(out/'receipt.json', summary)
    return summary

def publish(directory, request_id):
    """Only validated results enter a separate data branch; source branches are untouched."""
    receipt = json.loads((directory/'receipt.json').read_bytes())
    raw = (directory/'comparison.json.gz').read_bytes()
    if receipt['request_id'] != request_id or digest(raw) != receipt['sha256']:
        raise ValueError('result artifact checksum mismatch')
    bundle = json.loads(gzip.decompress(raw))
    validate_request(receipt['ticker'], request_id, receipt['old_sha'], receipt['new_sha'])
    for field in ('request_id','ticker','old_sha','new_sha','input_sha256','run_id','catalog_sha256','coverage_kind','timings'):
        if receipt[field] != bundle[field]:
            raise ValueError('result envelope mismatch: '+field)
    for role in ('old','new'):
        if bundle[role]['input_sha256'] != bundle['input_sha256'] or bundle[role]['source_sha'] != bundle[role+'_sha']:
            raise ValueError('mixed parser result')
    env = dict(os.environ)
    env.update(GIT_AUTHOR_NAME='Stock parser request', GIT_AUTHOR_EMAIL='bot@users.noreply.github.com',
               GIT_COMMITTER_NAME='Stock parser request', GIT_COMMITTER_EMAIL='bot@users.noreply.github.com')
    def git(*args, data=None):
        return subprocess.check_output(['git', *args], input=data, text=True, env=env).strip()
    target = 'requests/'+request_id
    for attempt in range(8):
        heads = git('ls-remote','--heads','origin','refs/heads/'+RESULT_BRANCH)
        parent = None
        if heads:
            git('fetch','--depth=1','origin',RESULT_BRANCH)
            parent = git('rev-parse','FETCH_HEAD')
            prior = subprocess.run(['git','cat-file','-e',parent+':'+target+'/receipt.json'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if prior.returncode == 0:
                if git('show',parent+':'+target+'/receipt.json') != (directory/'receipt.json').read_text().strip():
                    raise ValueError('immutable request ID already exists with a different result')
                return parent
        with tempfile.TemporaryDirectory() as temporary:
            env['GIT_INDEX_FILE'] = str(Path(temporary)/'index')
            if parent:
                git('read-tree', parent)
            else:
                git('read-tree','--empty')
            rows = []
            for name in ('comparison.json.gz','receipt.json'):
                blob = git('hash-object','-w',str((directory/name).resolve()))
                rows.append('100644 '+blob+'\t'+target+'/'+name+'\n')
            git('update-index','--index-info',data=''.join(rows))
            tree = git('write-tree','--missing-ok')
            args = ['commit-tree',tree]
            if parent:
                args += ['-p',parent]
            commit = git(*args,'-m','Retain on-demand comparison '+request_id)
            pushed = subprocess.run(['git','push','origin',commit+':refs/heads/'+RESULT_BRANCH], env=env)
            if pushed.returncode == 0:
                return commit
        time.sleep(min(2**attempt, 20))
    raise RuntimeError('result publication failed after concurrent-write retries')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['prepare','parse','assemble','publish'])
    parser.add_argument('--catalog',type=Path)
    parser.add_argument('--parser-root',type=Path)
    parser.add_argument('--ticker')
    parser.add_argument('--input',type=Path)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--old',type=Path)
    parser.add_argument('--new',type=Path)
    parser.add_argument('--kind',choices=['old','new'])
    parser.add_argument('--revision')
    parser.add_argument('--request-id',default=os.environ.get('REQUEST_ID'))
    parser.add_argument('--old-sha',default=os.environ.get('OLD_SHA',OLD))
    parser.add_argument('--new-sha',default=os.environ.get('NEW_SHA',NEW))
    parser.add_argument('--run-id',default=os.environ.get('GITHUB_RUN_ID','local'))
    args = parser.parse_args()
    if args.command == 'prepare':
        validate_request(args.ticker,args.request_id,args.old_sha,args.new_sha)
        prepare(args.catalog,args.parser_root,args.ticker,args.out)
    elif args.command == 'parse':
        parse(args.input,args.parser_root,args.out,args.revision,args.kind)
    elif args.command == 'assemble':
        print(json.dumps(assemble(args.input,args.old,args.new,args.out,args.request_id,args.old_sha,args.new_sha,args.run_id)))
    else:
        print(publish(args.out,args.request_id))

if __name__ == '__main__':
    main()
