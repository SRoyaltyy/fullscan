"""Install the on-demand comparison UI while retaining its preceding saved view."""
import argparse, hashlib, json, shutil
from pathlib import Path
from scripts.stock_parser_request import OLD, NEW, REPO

def install(source, site, source_revision, catalog_revision):
    dest=site/'dashboard/scanner-backtest/compare'
    catalog=json.loads((dest/'catalog.json').read_bytes())
    if not catalog.get('catalog') or len(catalog['catalog']) != catalog['expected_stocks']:
        raise ValueError('published catalog is incomplete')
    prior=dest/'request-config.json'
    old_sha=OLD
    if prior.exists():
        config=json.loads(prior.read_bytes())
        old_sha=config['old_sha'] if config['new_sha']==NEW else config['new_sha']
    archive=dest/'history'/catalog_revision
    if not archive.exists():
        archive.mkdir(parents=True)
        for name in ('index.html','comparison.mjs','request.mjs','catalog.json','request-config.json'):
            if (dest/name).exists():
                shutil.copyfile(dest/name,archive/name)
    for name in ('index.html','comparison.mjs','request.mjs'):
        shutil.copyfile(source/'dashboard/stock-comparison'/name,dest/name)
    config={'schema':'stock-parser-request-config-1','workflow':'stock-parser-request.yml',
            'execution_ref':'main','repository':REPO,'old_sha':old_sha,'new_sha':NEW,
            'catalog_revision':catalog_revision,'catalog_sha256':hashlib.sha256((dest/'catalog.json').read_bytes()).hexdigest(),'ui_revision':source_revision,
            'saved_comparison':'history/'+catalog_revision+'/',
            'experimental':True,'accuracy_certified':False}
    (dest/'request-config.json').write_text(json.dumps(config,indent=2)+'\n')
    return config

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--source',type=Path,default=Path('.'))
    p.add_argument('--site',type=Path,required=True)
    p.add_argument('--source-revision',required=True)
    p.add_argument('--catalog-revision',required=True)
    a=p.parse_args()
    print(json.dumps(install(a.source,a.site,a.source_revision,a.catalog_revision)))
