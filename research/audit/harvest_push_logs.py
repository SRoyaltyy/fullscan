#!/usr/bin/env python3
"""Download producer-workflow job logs and keep the lines that pushed main.

A commit counts only when a run log shows it was sent to main
(`[safe-push] pushed <sha>` or `old..new <ref> -> main`). Person pushes
that never appear in a run range stay unproven. Committer timestamps
are not read here.

Cache (resume-safe):
  /tmp/fm_audit_336/producer_runs.tsv
  /tmp/fm_audit_336/push_hits.tsv
  /tmp/fm_audit_336/jobs_done.tsv
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = "/workspace"
CACHE = "/tmp/fm_audit_336"
REPO = "SRoyaltyy/fullscan"
CREATED = "2026-08-10..2026-09-26"
WORKERS = 6

# Workflows that land per-day fullscan inputs. Mine/backtest workflows
# are omitted; they do not publish the morning packets.
WORKFLOWS = {
    337140997: "A+B1 Checklist (liquid universe)",
    337778046: "AB Enrich (peers + industry/sector)",
    338420999: "AB Full Market (checklist + peers + backfill)",
    337505150: "AB Full Scan",
    337779857: "AB One Button (checklist + peers + backfill)",
    343027481: "Catalyst daily (bounded dossiers)",
    268156540: "Catalyst Analysis Engine",
    345435345: "Finviz ALL (GH-hosted Elite)",
    338286649: "Finviz Daily Digest",
    356539887: "Finviz homepage market digest",
    342845598: "Finviz pre-open scrape (GH-hosted Elite)",
    333253187: "Label + Weather (daily)",
    342774792: "Map Heat Captain Research (post-close)",
    330321124: "News Actions (event edges → tickers)",
    338252388: "News Judge (LLM priority)",
    329938834: "News Parse (Supabase)",
    341858612: "Pre-Open ALL (predictive one-shot)",
    353309802: "Pre-Open finish holes 2026-09-08",
    329925533: "Sector Daily (auto)",
    329887215: "Sector Pipeline (per-sector sequential)",
    329881910: "Sector Predict",
    333434921: "Stock Book (unified)",
    333447948: "Stock Book ALL (one-shot)",
    337503324: "Quote Colors (Finviz green/red/neutral)",
    330996030: "Event Scanner",
    350016631: "Post-Close ALL (grade + learn + next captains)",
    338270052: "Daily Orchestrator (missed-job guard)",
    324187256: "Autonomous Daily Market Pipeline",
    346010588: "Ticker Lookback (any stock)",
    345977999: "Excel Bot (cluster signals daily)",
    335072211: "Price store + checklist (1/3/4/5)",
    343530652: "Book Lookback (ranker time machine)",
    350774807: "Factor strategy mine",
}

STEP_KEYS = ("commit", "push", "land", "safe", "sweep", "publish")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
LINE_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+(.*)$"
)
SAFE = re.compile(
    r"\[safe-push\] pushed ([0-9a-f]{7,40})"
    r"(?:\s+\([^)]*\))?\s+onto\s+([0-9a-f]{7,40})"
)
RANGE = re.compile(
    r"(?<![0-9a-f])([0-9a-f]{7,40})\.\.([0-9a-f]{7,40})\s+(\S+)\s+->\s+main\b"
)

_lock = threading.Lock()
_token: str | None = None


def token() -> str:
    global _token
    if _token is None:
        _token = subprocess.check_output(["gh", "auth", "token"], text=True).strip()
    return _token


def gh_json(url: str, tries: int = 5) -> dict:
    last = ""
    for n in range(tries):
        proc = subprocess.run(
            ["gh", "api", url],
            capture_output=True, text=True,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return json.loads(proc.stdout)
        last = (proc.stderr or proc.stdout or "")[-400:]
        if "rate limit" in last.lower() or "403" in last or "502" in last or "504" in last:
            time.sleep(2 ** n)
            continue
        if proc.returncode != 0 and n + 1 < tries:
            time.sleep(1 + n)
            continue
        break
    raise RuntimeError(f"gh api failed {url}: {last}")


def list_runs() -> list[dict]:
    rows = []
    for wid, name in WORKFLOWS.items():
        page = 1
        while True:
            url = (
                f"/repos/{REPO}/actions/workflows/{wid}/runs"
                f"?created={CREATED}&per_page=100&page={page}"
            )
            try:
                data = gh_json(url)
            except RuntimeError as exc:
                print(f"LIST FAIL {name} p{page}: {exc}", flush=True)
                break
            batch = data.get("workflow_runs") or []
            if not batch:
                break
            for run in batch:
                if run.get("status") not in ("completed",):
                    continue
                if run.get("conclusion") in ("skipped", "startup_failure", None):
                    continue
                rows.append({
                    "id": run["id"],
                    "workflow_id": wid,
                    "workflow_name": name,
                    "event": run.get("event") or "",
                    "conclusion": run.get("conclusion") or "",
                    "created_at": run.get("created_at") or "",
                    "run_started_at": run.get("run_started_at") or run.get("created_at") or "",
                    "updated_at": run.get("updated_at") or "",
                    "head_sha": run.get("head_sha") or "",
                    "html_url": run.get("html_url") or "",
                })
            print(f"listed {name} page {page} +{len(batch)} total {len(rows)}", flush=True)
            if len(batch) < 100:
                break
            page += 1
    return rows


def write_runs(rows: list[dict]) -> None:
    path = os.path.join(CACHE, "producer_runs.tsv")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(
            "id\tworkflow_id\tworkflow_name\tevent\tconclusion\t"
            "created_at\trun_started_at\tupdated_at\thead_sha\thtml_url\n"
        )
        for r in rows:
            fh.write("\t".join(str(r[k]).replace("\t", " ") for k in (
                "id", "workflow_id", "workflow_name", "event", "conclusion",
                "created_at", "run_started_at", "updated_at", "head_sha", "html_url",
            )) + "\n")


def load_done() -> set[str]:
    path = os.path.join(CACHE, "jobs_done.tsv")
    done = set()
    if not os.path.exists(path):
        return done
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if parts and parts[0] != "job_id":
                done.add(parts[0])
    return done


def append(path: str, line: str) -> None:
    with _lock:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line)
            if not line.endswith("\n"):
                fh.write("\n")


def job_interesting(job: dict) -> bool:
    for step in job.get("steps") or []:
        name = (step.get("name") or "").lower()
        if any(k in name for k in STEP_KEYS):
            conclusion = step.get("conclusion") or ""
            if conclusion == "skipped":
                continue
            return True
    return False


def scan_log(text_iter, run: dict, job: dict, hits_path: str) -> int:
    n = 0
    for raw in text_iter:
        line = ANSI.sub("", raw.rstrip("\n"))
        mts = LINE_TS.match(line)
        if mts:
            ts, body = mts.group(1), mts.group(2)
        else:
            ts, body = "", line
        found = []
        sm = SAFE.search(body)
        if sm:
            found.append(("safe-push", sm.group(1), sm.group(2)))
        rm = RANGE.search(body)
        if rm and "origin/main" not in body:
            found.append(("range", rm.group(2), rm.group(1)))
        for kind, new, old in found:
            n += 1
            rec = "\t".join([
                str(run["id"]), run["workflow_name"].replace("\t", " "),
                str(run["workflow_id"]), run["event"], run["conclusion"],
                run["run_started_at"], run["updated_at"], run["head_sha"],
                str(job["id"]), (job.get("name") or "").replace("\t", " "),
                ts, kind, new, old,
            ])
            append(hits_path, rec)
    return n


def fetch_job(run: dict, job: dict, hits_path: str, done_path: str) -> str:
    url = f"https://api.github.com/repos/{REPO}/actions/jobs/{job['id']}/logs"
    cmd = [
        "curl", "-fsSL", "--retry", "4", "--retry-delay", "2",
        "--retry-all-errors",
        "-H", f"Authorization: Bearer {token()}",
        "-H", "Accept: application/vnd.github+json",
        "-L", url,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        n = scan_log(proc.stdout, run, job, hits_path)
    finally:
        err = proc.stderr.read() if proc.stderr else ""
        code = proc.wait()
    status = "ok" if code == 0 else f"err{code}"
    note = (err or "").strip().replace("\t", " ").replace("\n", " ")[-180:]
    append(done_path, f"{job['id']}\t{run['id']}\t{status}\t{n}\t{note}")
    return status


def harvest(runs: list[dict]) -> None:
    hits_path = os.path.join(CACHE, "push_hits.tsv")
    done_path = os.path.join(CACHE, "jobs_done.tsv")
    if not os.path.exists(hits_path):
        with open(hits_path, "w", encoding="utf-8") as fh:
            fh.write(
                "run_id\tworkflow_name\tworkflow_id\tevent\tconclusion\t"
                "run_started_at\trun_updated_at\thead_sha\tjob_id\tjob_name\t"
                "line_ts\tkind\tsha_new\tsha_old\n"
            )
    if not os.path.exists(done_path):
        with open(done_path, "w", encoding="utf-8") as fh:
            fh.write("job_id\trun_id\tstatus\thits\tnote\n")
    done = load_done()

    def process(run: dict) -> str:
        marker = f"run{run['id']}"
        if marker in done:
            return "skip"
        try:
            data = gh_json(
                f"/repos/{REPO}/actions/runs/{run['id']}/jobs?per_page=100"
            )
        except RuntimeError as exc:
            append(done_path, f"{marker}\t{run['id']}\tlist_fail\t0\t{exc}")
            return "list_fail"
        jobs = [j for j in (data.get("jobs") or []) if job_interesting(j)]
        if not jobs:
            append(done_path, f"{marker}\t{run['id']}\tno_push_step\t0\t")
            return "no_push_step"
        statuses = []
        for job in jobs:
            jid = str(job["id"])
            if jid in done:
                statuses.append("skip")
                continue
            try:
                statuses.append(fetch_job(run, job, hits_path, done_path))
            except Exception as exc:  # noqa: BLE001
                append(done_path, f"{jid}\t{run['id']}\texc\t0\t{exc}")
                statuses.append("exc")
        if statuses and all(s in ("ok", "skip") for s in statuses):
            append(done_path, f"{marker}\t{run['id']}\tjobs_done\t{len(jobs)}\t")
        return statuses[-1] if statuses else "skip"

    ok = err = skip = 0
    with ThreadPoolExecutor(WORKERS) as pool:
        futs = [pool.submit(process, r) for r in runs]
        for n, fut in enumerate(as_completed(futs), 1):
            status = fut.result()
            if status in ("ok", "jobs_done"):
                ok += 1
            elif status in ("skip", "no_push_step"):
                skip += 1
            else:
                err += 1
            if n % 25 == 0 or n == len(futs):
                print(f"runs {n}/{len(futs)} ok {ok} skip {skip} err {err}", flush=True)


def main() -> None:
    os.makedirs(CACHE, exist_ok=True)
    runs_path = os.path.join(CACHE, "producer_runs.tsv")
    if os.path.exists(runs_path) and os.path.getsize(runs_path) > 200:
        runs = []
        with open(runs_path, encoding="utf-8") as fh:
            header = fh.readline().rstrip("\n").split("\t")
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < len(header):
                    continue
                runs.append(dict(zip(header, parts)))
        print(f"reuse {len(runs)} runs", flush=True)
    else:
        runs = list_runs()
        write_runs(runs)
        print(f"wrote {len(runs)} runs", flush=True)
    harvest(runs)
    print("HARVEST_DONE", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
