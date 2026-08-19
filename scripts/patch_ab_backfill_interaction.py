"""One-shot patch for interaction table formatting."""
from pathlib import Path

p = Path("src/ab_backfill.py")
t = p.read_text(encoding="utf-8")
old = '''            lines.append(
                f"| {lab} | {int(m.sum())} | "
                f"{100*r1:.0f}%" if r1 is not None else f"| {lab} | {int(m.sum())} | —"
            )
            # fix formatting
'''
new = '''            c1 = f"{100 * r1:.0f}%" if r1 is not None else "—"
            c2 = f"{100 * r2:.0f}%" if r2 is not None else "—"
            lines.append(f"| {lab} | {int(m.sum())} | {c1} | {c2} |")
'''
if old in t:
    p.write_text(t.replace(old, new, 1), encoding="utf-8")
    print("patched")
elif "c1 = f\"{100 * r1" in t or "c1 = f'{100 * r1" in t:
    print("already patched")
else:
    # alternate quote style already fixed in some revs
    if "c1 = f\"{100 * r1:.0f}%\"" in t or 'c1 = f"{100 * r1:.0f}%"' in t:
        print("already patched")
    else:
        raise SystemExit("pattern not found")
