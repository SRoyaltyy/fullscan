"""Never-again: broken fromJSON / missing preopen must fail before merge.

#242 hollowed ECS Pre-Open. The predictive `preopen` job used:

    fromJSON('[\\"self-hosted\\",\\"ecs\\"]')

That over-escaped string is invalid JSON, so `runner=ecs` never spawned
`preopen` (only gate + scrape + land_book). The fix is the unescaped form
already used by stock_book_all / postclose_all:

    fromJSON('["self-hosted","ecs"]')

This module is stdlib-only and does not call the Actions API.

Run: PYTHONPATH=. python3 -m src.test_workflow_fromjson
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"

# Exact payload GitHub evaluated on the hollow ECS runs (#242).
# json.loads fails — backslash-escaped quotes are not valid outside a
# JSON string, so the job expression never resolved and `preopen` dropped.
BUG_242_FROMJSON_PAYLOAD = r'[\"self-hosted\",\"ecs\"]'
BUG_242_RUNS_ON = (
    "${{ (github.event_name == 'push' || github.event_name == 'schedule' "
    "|| github.event.inputs.runner == 'ubuntu') && 'ubuntu-latest' "
    f"|| fromJSON('{BUG_242_FROMJSON_PAYLOAD}') }}}}"
)

ECS_LABELS = ["self-hosted", "ecs"]

# Tiny workflow with the #242 escape. Used as a negative fixture so a
# future "simplify the checker" cannot drop the regression case.
BUG_242_WORKFLOW = """
jobs:
  gate:
    runs-on: ubuntu-latest
  scrape:
    runs-on: ubuntu-latest
  land_book:
    runs-on: ubuntu-latest
  preopen:
    runs-on: ${{ (github.event_name == 'push' || github.event_name == 'schedule' || github.event.inputs.runner == 'ubuntu') && 'ubuntu-latest' || fromJSON('[\\"self-hosted\\",\\"ecs\\"]') }}
"""

MISSING_PREOPEN_WORKFLOW = """
jobs:
  gate:
    runs-on: ubuntu-latest
  scrape:
    runs-on: ubuntu-latest
  land_book:
    runs-on: ubuntu-latest
"""

UBUNTU_ONLY_PREOPEN_WORKFLOW = """
jobs:
  preopen:
    runs-on: ubuntu-latest
"""


class FromJSONError(ValueError):
    """fromJSON argument is not valid JSON (the #242 failure mode)."""


class ExprError(ValueError):
    """runs-on expression cannot be evaluated statically."""


def _in_yaml_comment(text: str, index: int) -> bool:
    """True if index sits on a YAML comment (so docs of #242 do not fail)."""
    line_start = text.rfind("\n", 0, index) + 1
    in_quote = None
    for ch in text[line_start:index]:
        if in_quote:
            if ch == in_quote:
                in_quote = None
            continue
        if ch in "'\"":
            in_quote = ch
            continue
        if ch == "#":
            return True
    return False


def iter_fromjson_literals(text: str) -> Iterator[tuple[str, int]]:
    """Yield (payload, 1-based line) for each fromJSON('...') / fromJSON(\"...\").

    Dynamic calls like fromJSON(needs.foo.outputs.bar) are skipped — there
    is no static string to validate. Comment-only examples (the #242
    over-escape in workflow_selfcheck.yml) are also skipped.
    """
    i = 0
    while True:
        j = text.find("fromJSON(", i)
        if j < 0:
            return
        if _in_yaml_comment(text, j):
            i = j + 1
            continue
        k = j + len("fromJSON(")
        while k < len(text) and text[k].isspace():
            k += 1
        if k >= len(text) or text[k] not in "'\"":
            i = k if k > j else j + 1
            continue
        quote = text[k]
        k += 1
        start = k
        while k < len(text) and text[k] != quote:
            k += 1
        if k >= len(text):
            raise FromJSONError("unterminated fromJSON string")
        yield text[start:k], text.count("\n", 0, j) + 1
        i = k + 1


def parse_fromjson_payload(payload: str) -> Any:
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise FromJSONError(
            f"fromJSON payload is not valid JSON ({exc.msg}): {payload!r}"
        ) from exc


def _tokenize(expr: str) -> list[Any]:
    tokens: list[Any] = []
    i = 0
    n = len(expr)
    while i < n:
        c = expr[i]
        if c.isspace():
            i += 1
            continue
        if expr.startswith("fromJSON", i) and (
            i + 8 == n or not (expr[i + 8].isalnum() or expr[i + 8] == "_")
        ):
            tokens.append("fromJSON")
            i += 8
            continue
        if expr.startswith("&&", i):
            tokens.append("&&")
            i += 2
            continue
        if expr.startswith("||", i):
            tokens.append("||")
            i += 2
            continue
        if expr.startswith("==", i):
            tokens.append("==")
            i += 2
            continue
        if expr.startswith("!=", i):
            tokens.append("!=")
            i += 2
            continue
        if c in "()":
            tokens.append(c)
            i += 1
            continue
        if c in "'\"":
            q = c
            i += 1
            start = i
            while i < n and expr[i] != q:
                i += 1
            if i >= n:
                raise ExprError("unterminated string in runs-on expression")
            tokens.append(("str", expr[start:i]))
            i += 1
            continue
        if c.isalpha() or c == "_":
            start = i
            while i < n and (expr[i].isalnum() or expr[i] in "._"):
                i += 1
            tokens.append(("id", expr[start:i]))
            continue
        raise ExprError(f"unexpected {c!r} in runs-on expression")
    return tokens


def _truthy(value: Any) -> bool:
    if value is False or value is None:
        return False
    if value == "":
        return False
    return True


def _eval_tokens(tokens: list[Any], ctx: dict[str, str]) -> Any:
    pos = 0

    def peek() -> Any:
        return tokens[pos] if pos < len(tokens) else None

    def eat(expected: Any | None = None) -> Any:
        nonlocal pos
        if pos >= len(tokens):
            raise ExprError("unexpected end of runs-on expression")
        tok = tokens[pos]
        if expected is not None and tok != expected:
            raise ExprError(f"expected {expected!r}, got {tok!r}")
        pos += 1
        return tok

    def parse_or() -> Any:
        left = parse_and()
        while peek() == "||":
            eat("||")
            if _truthy(left):
                parse_and()
            else:
                left = parse_and()
        return left

    def parse_and() -> Any:
        left = parse_eq()
        while peek() == "&&":
            eat("&&")
            if not _truthy(left):
                parse_eq()
            else:
                left = parse_eq()
        return left

    def parse_eq() -> Any:
        left = parse_primary()
        while peek() in ("==", "!="):
            op = eat()
            right = parse_primary()
            left = (left == right) if op == "==" else (left != right)
        return left

    def parse_primary() -> Any:
        tok = peek()
        if tok == "(":
            eat("(")
            val = parse_or()
            eat(")")
            return val
        if tok == "fromJSON":
            eat("fromJSON")
            eat("(")
            arg = eat()
            eat(")")
            if not (isinstance(arg, tuple) and arg[0] == "str"):
                raise ExprError("fromJSON() needs a string literal")
            return parse_fromjson_payload(arg[1])
        if isinstance(tok, tuple) and tok[0] == "str":
            eat()
            return tok[1]
        if isinstance(tok, tuple) and tok[0] == "id":
            eat()
            name = tok[1]
            if name in ctx:
                return ctx[name]
            if name.startswith("github."):
                return ""
            raise ExprError(f"unknown identifier {name!r}")
        raise ExprError(f"unexpected token {tok!r}")

    value = parse_or()
    if pos != len(tokens):
        raise ExprError(f"trailing tokens in runs-on: {tokens[pos:]!r}")
    return value


def unwrap_expr(raw: str) -> str:
    text = raw.strip()
    if text.startswith("${{") and text.endswith("}}"):
        return text[3:-2].strip()
    return text


def eval_runs_on(raw: str, *, event_name: str, runner: str = "") -> Any:
    """Evaluate a job runs-on value for a static event/runner pair."""
    text = raw.strip()
    if text.startswith("${{"):
        ctx = {
            "github.event_name": event_name,
            "github.event.inputs.runner": runner,
        }
        return _eval_tokens(_tokenize(unwrap_expr(text)), ctx)
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1]
        return [
            part.strip().strip("'\"")
            for part in inner.split(",")
            if part.strip()
        ]
    return text.strip("'\"")


def parse_workflow_jobs(text: str) -> dict[str, dict[str, str]]:
    """Map job id → {runs-on: raw} from a workflow document.

    Indent-based, not a full YAML parse. Matches this repo's workflow style
    (2-space job keys, 4-space runs-on). Comments are ignored.
    """
    jobs: dict[str, dict[str, str]] = {}
    in_jobs = False
    job_id: str | None = None
    collecting: list[str] | None = None

    def flush_list() -> None:
        nonlocal collecting
        if job_id and collecting is not None:
            jobs[job_id]["runs-on"] = "[" + ", ".join(collecting) + "]"
        collecting = None

    for line in text.splitlines():
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        if not in_jobs:
            if re.match(r"^jobs:\s*(#.*)?$", line):
                in_jobs = True
            continue
        if line[0] not in " \t":
            break
        if collecting is not None:
            item = re.match(r"^\s+-\s+(\S.*)$", line)
            if item:
                collecting.append(item.group(1).split("#", 1)[0].strip())
                continue
            flush_list()
        m_job = re.match(r"^  ([A-Za-z0-9_-]+):\s*(#.*)?$", line)
        if m_job:
            job_id = m_job.group(1)
            jobs[job_id] = {}
            continue
        m_ro = re.match(r"^\s+runs-on:\s*(.*)$", line)
        if m_ro and job_id:
            rest = m_ro.group(1).split("#", 1)[0].strip()
            if rest:
                jobs[job_id]["runs-on"] = rest
            else:
                collecting = []
            continue
    flush_list()
    return jobs


def as_labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        return [value]
    raise ExprError(f"runs-on resolved to {value!r}, not labels")


def assert_preopen_job(text: str, *, source: str = "preopen_all.yml") -> None:
    """Fail if preopen is missing or cannot resolve ubuntu / ECS labels."""
    jobs = parse_workflow_jobs(text)
    if "preopen" not in jobs:
        raise AssertionError(
            f"{source}: predictive job `preopen` is missing — "
            "gate/scrape/land_book alone is a hollow Pre-Open"
        )
    raw = jobs["preopen"].get("runs-on")
    if not raw:
        raise AssertionError(f"{source}: job `preopen` has no runs-on")

    ubuntu = as_labels(eval_runs_on(raw, event_name="push", runner=""))
    if ubuntu != ["ubuntu-latest"]:
        raise AssertionError(
            f"{source}: preopen ubuntu branch resolved to {ubuntu}, "
            "expected ['ubuntu-latest']"
        )
    ubuntu_dispatch = as_labels(
        eval_runs_on(raw, event_name="workflow_dispatch", runner="ubuntu")
    )
    if ubuntu_dispatch != ["ubuntu-latest"]:
        raise AssertionError(
            f"{source}: preopen runner=ubuntu resolved to {ubuntu_dispatch}, "
            "expected ['ubuntu-latest']"
        )
    ecs = as_labels(
        eval_runs_on(raw, event_name="workflow_dispatch", runner="ecs")
    )
    if ecs != ECS_LABELS:
        raise AssertionError(
            f"{source}: preopen ecs branch resolved to {ecs}, "
            f"expected {ECS_LABELS}"
        )


def assert_all_fromjson_valid(text: str, *, source: str) -> None:
    for payload, line in iter_fromjson_literals(text):
        try:
            parse_fromjson_payload(payload)
        except FromJSONError as exc:
            raise AssertionError(f"{source}:{line}: {exc}") from exc


def workflow_texts() -> list[tuple[str, str]]:
    return [
        (path.name, path.read_text(encoding="utf-8"))
        for path in sorted(WF.glob("*.yml"))
    ]


def test_bug_242_over_escape_is_invalid_json() -> None:
    """Negative fixture: the #242 payload must keep failing json.loads."""
    assert BUG_242_FROMJSON_PAYLOAD == r'[\"self-hosted\",\"ecs\"]'
    try:
        json.loads(BUG_242_FROMJSON_PAYLOAD)
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("#242 payload unexpectedly parsed as JSON")
    try:
        parse_fromjson_payload(BUG_242_FROMJSON_PAYLOAD)
    except FromJSONError:
        pass
    else:
        raise AssertionError("#242 payload must raise FromJSONError")


def test_bug_242_runs_on_drops_ecs_preopen() -> None:
    """Evaluating the hollow expression must fail, not return ECS labels."""
    try:
        eval_runs_on(
            BUG_242_RUNS_ON, event_name="workflow_dispatch", runner="ecs"
        )
    except FromJSONError:
        pass
    else:
        raise AssertionError("#242 runs-on must not resolve for runner=ecs")
    try:
        assert_preopen_job(BUG_242_WORKFLOW, source="bug242.yml")
    except (FromJSONError, AssertionError):
        pass
    else:
        raise AssertionError("bug-242 fixture workflow must fail the check")


def test_missing_preopen_job_fails() -> None:
    try:
        assert_preopen_job(MISSING_PREOPEN_WORKFLOW, source="hollow.yml")
    except AssertionError as exc:
        if "preopen" not in str(exc):
            raise
    else:
        raise AssertionError("missing preopen must fail")


def test_ubuntu_only_preopen_fails_ecs_branch() -> None:
    try:
        assert_preopen_job(UBUNTU_ONLY_PREOPEN_WORKFLOW, source="ubuntu-only.yml")
    except AssertionError as exc:
        if "ecs" not in str(exc).lower():
            raise
    else:
        raise AssertionError("ubuntu-only preopen must fail the ecs branch")


def test_good_fromjson_resolves_both_branches() -> None:
    good = (
        "${{ (github.event_name == 'push' || github.event_name == 'schedule' "
        "|| github.event.inputs.runner == 'ubuntu') && 'ubuntu-latest' "
        "|| fromJSON('[\"self-hosted\",\"ecs\"]') }}"
    )
    assert eval_runs_on(good, event_name="push") == "ubuntu-latest"
    assert eval_runs_on(good, event_name="schedule") == "ubuntu-latest"
    assert eval_runs_on(
        good, event_name="workflow_dispatch", runner="ubuntu"
    ) == "ubuntu-latest"
    assert eval_runs_on(
        good, event_name="workflow_dispatch", runner="ecs"
    ) == ECS_LABELS
    assert_preopen_job(
        "jobs:\n  preopen:\n    runs-on: " + good + "\n",
        source="good.yml",
    )


def test_comment_docs_of_bug_242_are_ignored() -> None:
    text = (
        "# Never-again: fromJSON('[\\\"self-hosted\\\",\\\"ecs\\\"]') is invalid\n"
        "jobs:\n"
        "  preopen:\n"
        "    runs-on: ${{ github.event.inputs.runner == 'ubuntu' "
        "&& 'ubuntu-latest' || fromJSON('[\"self-hosted\",\"ecs\"]') }}\n"
    )
    assert_all_fromjson_valid(text, source="comment.yml")
    payloads = [p for p, _ in iter_fromjson_literals(text)]
    assert payloads == ['["self-hosted","ecs"]']


def test_live_workflows_fromjson_is_valid_json() -> None:
    found = 0
    for name, text in workflow_texts():
        payloads = list(iter_fromjson_literals(text))
        found += len(payloads)
        assert_all_fromjson_valid(text, source=name)
        for payload, _line in payloads:
            if "self-hosted" in payload:
                parsed = parse_fromjson_payload(payload)
                assert parsed == ECS_LABELS, f"{name}: {payload!r} -> {parsed!r}"
    assert found >= 4, f"expected several fromJSON literals, found {found}"


def test_preopen_all_job_resolves_ubuntu_and_ecs() -> None:
    text = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert_preopen_job(text, source="preopen_all.yml")
    jobs = parse_workflow_jobs(text)
    for required in ("gate", "scrape", "land_book", "preopen"):
        assert required in jobs, f"preopen_all.yml missing job {required}"
    # The live file must not still carry the #242 escape.
    assert r'[\"self-hosted\"' not in text
    assert "fromJSON('[\"self-hosted\",\"ecs\"]')" in text


def test_stock_book_all_fromjson_resolves() -> None:
    text = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    jobs = parse_workflow_jobs(text)
    raw = jobs["all"]["runs-on"]
    assert_all_fromjson_valid(text, source="stock_book_all.yml")
    assert eval_runs_on(raw, event_name="schedule") == "ubuntu-latest"
    assert eval_runs_on(
        raw, event_name="workflow_dispatch", runner="ecs"
    ) == ECS_LABELS


def test_openclaw_probe_stays_on_ecs() -> None:
    text = (WF / "openclaw_probe.yml").read_text(encoding="utf-8")
    jobs = parse_workflow_jobs(text)
    raw = jobs["probe"]["runs-on"]
    assert eval_runs_on(raw, event_name="workflow_dispatch") == ECS_LABELS


def test_self_hosted_fromjson_jobs_resolve_both_sides() -> None:
    """Every fromJSON(self-hosted) runs-on must resolve ubuntu + ECS."""
    checked = 0
    for name, text in workflow_texts():
        jobs = parse_workflow_jobs(text)
        for job_id, meta in jobs.items():
            raw = meta.get("runs-on", "")
            if "fromJSON" not in raw or "self-hosted" not in raw:
                continue
            src = f"{name}:{job_id}"
            assert_all_fromjson_valid(raw, source=src)
            ubuntu = as_labels(
                eval_runs_on(raw, event_name="workflow_dispatch", runner="ubuntu")
            )
            ecs = as_labels(
                eval_runs_on(raw, event_name="workflow_dispatch", runner="ecs")
            )
            assert ubuntu == ["ubuntu-latest"], f"{src} ubuntu={ubuntu}"
            assert ecs == ECS_LABELS, f"{src} ecs={ecs}"
            checked += 1
    assert checked >= 4, f"expected several fromJSON jobs, found {checked}"


def _lane_json_python(text: str) -> str:
    start = text.find("python - <<'PY'")
    end = text.find("\n          PY\n", start)
    assert start >= 0 and end > start, "lane_json.yml missing Route inbox python"
    return text[start:end]


def test_lane_json_zero_dollar_hoppers() -> None:
    """OpenRouter-first $0 hopper stack: parse, order, secrets, no browser."""
    import ast

    text = (WF / "lane_json.yml").read_text(encoding="utf-8")
    py = _lane_json_python(text)
    ast.parse("\n".join(line[10:] if line.startswith("          ") else line
                        for line in py.splitlines()[1:]))

    block = py.split("def direct_ask")[1].split("def via_lane")[0]
    ordered = re.findall(
        r'hop_models\(\s*"(openrouter|github_models|cloudflare|sambanova|ollama|hf|groq|gemini)"',
        block,
    )
    assert ordered == [
        "openrouter", "github_models", "cloudflare", "sambanova",
        "ollama", "hf", "groq", "gemini",
    ], ordered

    header = text.split("on:", 1)[0]
    for secret in (
        "OPENROUTER_API_KEY",
        "GITHUB_MODELS_TOKEN",
        "GITHUB_TOKEN",
        "CLOUDFLARE_API_TOKEN",
        "CLOUDFLARE_ACCOUNT_ID",
        "SAMBANOVA_API_KEY",
        "HF_TOKEN",
        "OLLAMA_URL",
        "GROQ_API_KEY",
    ):
        assert secret in header, secret
        assert secret in text.split("env:", 1)[1].split("run:", 1)[0], secret

    assert "02_lessons/lane/inbox.json" in text
    assert "02_lessons/lane/outbox" in text
    assert "status == 429" in py
    assert "def _rotate" in py
    assert "raise SystemExit(0)" in py
    assert "skip (add OPENROUTER_API_KEY)" in py
    assert "cloakbrowser" not in text.lower() or "no cloakbrowser" in text.lower()
    assert "playwright" not in py.lower()
    assert "selenium" not in py.lower()
    assert "no paid" in header.lower() or "Never paid" in header
    assert ":free" in py
    assert "openrouter/free" in py
    assert "models.github.ai" in py
    assert "api.cloudflare.com" in py
    assert "api.sambanova.ai" in py
    assert "router.huggingface.co" in py
    assert "last-resort" in header.lower() or "dead for HK" in header

    # Paid OpenRouter IDs must not appear as model candidates.
    banned = ("gpt-4o", "gpt-4.1", "claude-3", "o1-preview", "openai/gpt-5")
    candidates = py.split("_OR_CANDIDATES")[1].split("OR_MODELS")[0]
    for bad in banned:
        assert bad not in candidates, bad
    assert 'endswith(":free")' in py

    assert "not required" in header.lower() or "Skip if unset" in header


def test_ci_workflow_is_wired() -> None:
    yml = (WF / "workflow_selfcheck.yml").read_text(encoding="utf-8")
    assert "pull_request:" in yml
    assert '".github/workflows/**"' in yml
    assert '"src/test_workflow_fromjson.py"' in yml
    assert "src.test_workflow_fromjson" in yml
    assert "actions/checkout@" in yml
    # Cheap + deterministic: no Actions API, no extra pip.
    assert "api.github.com" not in yml
    assert "pip install" not in yml


def main() -> None:
    tests = [
        test_bug_242_over_escape_is_invalid_json,
        test_bug_242_runs_on_drops_ecs_preopen,
        test_missing_preopen_job_fails,
        test_ubuntu_only_preopen_fails_ecs_branch,
        test_good_fromjson_resolves_both_branches,
        test_comment_docs_of_bug_242_are_ignored,
        test_live_workflows_fromjson_is_valid_json,
        test_preopen_all_job_resolves_ubuntu_and_ecs,
        test_stock_book_all_fromjson_resolves,
        test_openclaw_probe_stays_on_ecs,
        test_self_hosted_fromjson_jobs_resolve_both_sides,
        test_lane_json_zero_dollar_hoppers,
        test_ci_workflow_is_wired,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
