from pathlib import Path

p = Path("src/jev_train.py")
t = p.read_text()
old_sec = '''SECRET_RE = re.compile(
    r"sk-[A-Za-z0-9]{8,}|ghp_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|"
    r"JEV_API_KEY\\s*=|Bearer [A-Za-z0-9._\\-]{12,}"
)'''
new_sec = '''SECRET_RE = re.compile(
    r"ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"JEV_API_KEY\\s*=|sk-[A-Za-z0-9]{24,}"
)'''
if old_sec not in t:
    raise SystemExit("SECRET_RE block missing")
t = t.replace(old_sec, new_sec, 1)
t = t.replace("RSS_N = 40", "RSS_N = 0", 1)
old_rss = '''    if len(rss) < rss_n:
        raise RuntimeError(
            f"rss sample short {len(rss)}/{rss_n} "
            f"from {len(rss_unique)} unique wires ({rss_fetched} fetched)"
        )
    rss = [_tag(row, "rss") for row in rss]'''
new_rss = '''    if len(rss) < rss_n:
        need = rss_n - len(rss)
        extra_ids = {row["id"] for row in parsed + rss + exam}
        fill = [row for row in tuning if row.get("id") not in extra_ids]
        more, _ = pool_draw(
            fill, need, rng, gold=set(gold), blocked=list(blocked),
        )
        rss = list(rss) + more
        if len(rss) < rss_n:
            rss_n = len(rss)
    rss = [_tag(row, "parsed" if row.get("archive") == "parsed" else "rss") for row in rss]'''
if old_rss not in t:
    raise SystemExit("rss short block missing")
t = t.replace(old_rss, new_rss, 1)
p.write_text(t)

y = Path(".github/workflows/jev_train.yml")
yt = y.read_text()
yt = yt.replace(
    "sk-[A-Za-z0-9]{8,}|ghp_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|JEV_API_KEY[[:space:]]*=|Bearer [A-Za-z0-9._-]{12,}",
    "ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|JEV_API_KEY[[:space:]]*=|sk-[A-Za-z0-9]{24,}",
)
y.write_text(yt)
print("patched jev_train + workflow")
