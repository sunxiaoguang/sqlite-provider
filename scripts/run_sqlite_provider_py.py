#!/usr/bin/env python3
"""Standalone runner for tests/sqlite_provider_py."""

from __future__ import annotations

import importlib
import inspect
import sys
import tempfile
import traceback
from pathlib import Path


def discover_test_modules(test_dir: Path) -> list[str]:
    modules: list[str] = []
    for path in sorted(test_dir.glob("test_*.py")):
        rel = path.with_suffix("").relative_to(Path.cwd())
        modules.append(".".join(rel.parts))
    return modules


def call_test(fn) -> None:
    sig = inspect.signature(fn)
    if "tmp_path" in sig.parameters:
        with tempfile.TemporaryDirectory(prefix="sqlite-provider-py-") as tmp_dir:
            fn(tmp_path=Path(tmp_dir))
        return
    fn()


def is_skip_exception(exc: BaseException) -> bool:
    return exc.__class__.__name__ == "TestSkipped"


def run_tests(test_dir: Path) -> int:
    sys.path.insert(0, str(Path.cwd()))

    module_names = discover_test_modules(test_dir)
    total = 0
    passed = 0
    skipped = 0
    failed = 0

    for module_name in module_names:
        module = importlib.import_module(module_name)
        tests = [
            (name, obj)
            for name, obj in sorted(vars(module).items())
            if name.startswith("test_") and callable(obj)
        ]
        for test_name, fn in tests:
            total += 1
            try:
                call_test(fn)
            except Exception as exc:
                if is_skip_exception(exc):
                    skipped += 1
                    msg = str(exc) or "skipped"
                    print(f"SKIP {module_name}.{test_name}: {msg}")
                    continue
                failed += 1
                print(f"FAIL {module_name}.{test_name}")
                traceback.print_exc()
            else:
                passed += 1
                print(f"PASS {module_name}.{test_name}")

    print(f"summary: total={total} passed={passed} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 1


def main() -> int:
    test_dir_arg = sys.argv[1] if len(sys.argv) > 1 else "tests/sqlite_provider_py"
    test_dir = Path(test_dir_arg).resolve()
    if not test_dir.exists():
        print(f"test directory not found: {test_dir}", file=sys.stderr)
        return 2

    return run_tests(test_dir)


if __name__ == "__main__":
    raise SystemExit(main())
