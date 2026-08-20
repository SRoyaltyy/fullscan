"""Smoke tests for the "entrypoint never actually runs" bug class.

Recent failures (2026-08-20):
  * ab_checklist loader never called main()
  * assigning __name__ before exec made the inner ``if __name__ == "__main__"``
    always false

These tests do not hit APIs, do not write bot outputs, and do not change
strategy/rubric behavior. They assert:

  1. Critical modules import.
  2. CLI / ``__main__`` paths invoke a real entrypoint (AST + --help).
  3. The ab_checklist loader keeps a pre-exec ``_WAS_MAIN`` flag.
"""
from __future__ import annotations

import ast
import importlib
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Workflow-critical modules. A missing main() / dead ``__main__`` here
# silently skips a live GitHub Action.
CRITICAL_MODULES = [
    "src.config",
    "src.ab_checklist",
    "src.ab_backfill",
    "src.ab_one",
    "src.ab_enrich",
    "src.ab_merge_extras",
    "src.hit_board",
    "src.run_predict",
    "src.run_outcome",
    "src.run_reflect",
    "src.run_news_judge",
    "src.run_events",
    "src.finviz_digest",
    "src.industry_predict",
    "src.industry_map",
    "src.finviz_universe",
    "src.price_store",
    "src.peer_rs",
]

# ``python -m <mod> --help`` must reach argparse (exit 0), not no-op.
HELP_MODULES = [
    "src.ab_checklist",
    "src.ab_backfill",
    "src.ab_one",
    "src.ab_enrich",
    "src.hit_board",
    "src.run_predict",
    "src.run_outcome",
    "src.run_reflect",
    "src.run_news_judge",
    "src.run_events",
    "src.finviz_digest",
    "src.industry_predict",
    "src.price_store",
]

# Names that count as a real entrypoint when called from ``__main__``.
_ENTRY_FUNCS = {
    "main",
    "run",
    "collect",
    "write",
    "asyncio.run",
}


def _is_dunder_main(test: ast.AST) -> bool:
    """True for ``__name__ == "__main__"`` (either operand order)."""
    if not isinstance(test, ast.Compare) or len(test.ops) != 1:
        return False
    if not isinstance(test.ops[0], ast.Eq):
        return False
    left, right = test.left, test.comparators[0]

    def _name(n: ast.AST) -> str | None:
        return n.id if isinstance(n, ast.Name) else None

    def _const(n: ast.AST) -> str | None:
        return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None

    return (
        (_name(left) == "__name__" and _const(right) == "__main__")
        or (_const(left) == "__main__" and _name(right) == "__name__")
    )


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return f"{node.value.id}.{node.attr}"
    return None


def _called_from(body: list[ast.stmt]) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.Module(body=body, type_ignores=[])):
        if isinstance(node, ast.Call):
            n = _call_name(node.func)
            if n:
                names.add(n)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            n = _call_name(node.value.func)
            if n:
                names.add(n)
    return names


def _main_blocks(tree: ast.AST) -> list[ast.If]:
    return [
        n for n in tree.body
        if isinstance(n, ast.If) and _is_dunder_main(n.test)
    ]


class EntrypointContractTests(unittest.TestCase):
    """Static checks — no third-party imports required."""

    def test_ab_checklist_was_main_survives_name_overwrite(self) -> None:
        src = (ROOT / "src" / "ab_checklist.py").read_text(encoding="utf-8")
        self.assertIn("_WAS_MAIN", src)
        assign_at = src.find("_WAS_MAIN")
        overwrite_at = src.find('_g["__name__"]')
        if overwrite_at < 0:
            overwrite_at = src.find("['__name__']")
        self.assertGreater(assign_at, -1)
        self.assertGreater(overwrite_at, assign_at, "must capture _WAS_MAIN before exec overwrites __name__")
        tail = src[src.find("if _WAS_MAIN") :]
        self.assertIn("main()", tail)

    def test_ab_checklist_does_not_trust_inner_main_gate_alone(self) -> None:
        """The exec'd source's ``if __name__ == "__main__"`` is dead after overwrite."""
        src = (ROOT / "src" / "ab_checklist.py").read_text(encoding="utf-8")
        self.assertIn('_g["__name__"] = "src.ab_checklist"', src)
        self.assertIn("if _WAS_MAIN:", src)

    def test_workflow_src_modules_invoke_entrypoint(self) -> None:
        src_dir = ROOT / "src"
        dead: list[str] = []
        for path in sorted(src_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            blocks = _main_blocks(tree)
            if not blocks:
                # Loader-style: outer ``if _WAS_MAIN: main()`` is the contract.
                text = path.read_text(encoding="utf-8")
                if "if _WAS_MAIN:" in text and "main()" in text:
                    continue
                continue
            for block in blocks:
                calls = _called_from(block.body)
                nontrivial = [
                    s for s in block.body
                    if not isinstance(s, (ast.Pass, ast.Expr))
                    or (isinstance(s, ast.Expr) and not isinstance(s.value, ast.Constant))
                ]
                hits = calls & _ENTRY_FUNCS
                if hits:
                    continue
                if any(c.split(".")[-1] in {"main", "run", "collect", "write"} for c in calls):
                    continue
                if nontrivial:
                    # Inline ``__main__`` body (e.g. collectors) — accept.
                    continue
                dead.append(f"{path.name}: __main__ does not call main()/run()/collect()")
        self.assertEqual(dead, [], "dead entrypoints:\n" + "\n".join(dead))

    def test_industry_predict_imports_members_from_industry_map(self) -> None:
        src = (ROOT / "src" / "industry_predict.py").read_text(encoding="utf-8")
        self.assertIn("from .industry_map import members", src)
        self.assertNotIn("from .finviz_universe import members", src)


class ImportSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))

    def test_critical_modules_import_and_expose_entrypoint(self) -> None:
        missing = []
        for name in CRITICAL_MODULES:
            try:
                mod = importlib.import_module(name)
            except Exception as exc:  # noqa: BLE001
                missing.append(f"{name}: import failed: {exc}")
                continue
            if name in {
                "src.config",
                "src.industry_map",
                "src.finviz_universe",
            }:
                continue
            if not (hasattr(mod, "main") or hasattr(mod, "run") or hasattr(mod, "collect")):
                missing.append(f"{name}: imported but has no main()/run()/collect()")
        self.assertEqual(missing, [], "import/entrypoint failures:\n" + "\n".join(missing))

    def test_industry_predict_members_is_callable(self) -> None:
        from src.industry_predict import members

        self.assertTrue(callable(members))

    def test_ab_checklist_loader_injects_run_and_main(self) -> None:
        from src import ab_checklist as ab

        self.assertTrue(callable(getattr(ab, "run", None)))
        self.assertTrue(callable(getattr(ab, "main", None)))
        # Imported as a module, not ``python -m``, so the loader must not run main().
        self.assertIs(ab._WAS_MAIN, False)


class CliHelpSmokeTests(unittest.TestCase):
    def test_help_reaches_argparse(self) -> None:
        failures = []
        for mod in HELP_MODULES:
            proc = subprocess.run(
                [sys.executable, "-m", mod, "--help"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=90,
            )
            if proc.returncode != 0:
                failures.append(
                    f"{mod}: --help exited {proc.returncode}\n"
                    f"stdout={proc.stdout[-400:]}\nstderr={proc.stderr[-400:]}"
                )
                continue
            combined = (proc.stdout + proc.stderr).lower()
            if "usage" not in combined and "help" not in combined:
                failures.append(f"{mod}: --help produced no usage text")
        self.assertEqual(failures, [], "CLI help failures:\n\n".join(failures))


if __name__ == "__main__":
    unittest.main()
