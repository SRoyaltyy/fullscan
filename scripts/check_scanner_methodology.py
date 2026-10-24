"""Check cumulative failures, or reserve a new issuer draw before reviewing it."""
import argparse
import json
from pathlib import Path
from src.scanner_validation import methodology_fingerprint, reserve, release_decision, validate_register


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path('.'))
    p.add_argument('--register', type=Path, default=Path('eval/scanner-failures/register.json'))
    p.add_argument('--previous-register', type=Path)
    p.add_argument('--report', type=Path, default=Path('eval/scanner-failures/current-validation.json'))
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--reserve-catalog', type=Path)
    p.add_argument('--ledger', type=Path, default=Path('eval/scanner-failures/issuer-ledger.json'))
    p.add_argument('--count', type=int, default=20)
    p.add_argument('--source-root', type=Path, default=Path('eval/scanner-failures/sources'))
    p.add_argument('--require-pass', action='store_true')
    a = p.parse_args()
    register = json.loads(a.register.read_text())
    validate_register(register, json.loads(a.previous_register.read_text()) if a.previous_register else None)
    fingerprint = methodology_fingerprint(a.root)
    if a.reserve_catalog:
        catalog = json.loads(a.reserve_catalog.read_text())
        ledger = json.loads(a.ledger.read_text())
        result, updated = reserve(catalog, ledger, fingerprint=fingerprint, count=a.count)
        a.ledger.write_text(json.dumps(updated, indent=2)+'\n')
    else:
        report = json.loads(a.report.read_text()) if a.report.is_file() else {}
        ledger = json.loads(a.ledger.read_text()) if a.ledger.is_file() else None
        result = release_decision(register, report, fingerprint=fingerprint, ledger=ledger, source_root=a.source_root)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'modes'}))
    if a.require_pass and not result.get('release_allowed'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
