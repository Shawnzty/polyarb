"""Stdlib-only dependency preflight.

Imported at the very top of `polyarb.cli` so that missing third-party
distributions are detected and prompt-installed before any of the heavy
`httpx` / `requests` module-level imports crash the CLI. Everything in this
file must use only the Python standard library.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, TextIO


@dataclass(frozen=True)
class Requirement:
    module: str
    spec: str
    fallback_size_kb: int

    @property
    def distribution(self) -> str:
        # Strip version markers: "httpx>=0.27.0" → "httpx".
        for sep in ("==", ">=", "<=", "~=", "!=", ">", "<"):
            if sep in self.spec:
                return self.spec.split(sep, 1)[0].strip()
        return self.spec.strip()


# Mirrors the direct deps declared in pyproject.toml. Keep in sync manually —
# the list is short and changes rarely. The chicken-and-egg constraint (we
# need this check to run before third-party imports resolve) rules out
# reading pyproject.toml dynamically on Python 3.9/3.10 without adding
# `tomli` as a bootstrap dep.
REQUIRED: List[Requirement] = [
    Requirement("httpx", "httpx>=0.27.0", 500),
    Requirement("requests", "requests>=2.31.0", 250),
    Requirement("urllib3", "urllib3<2", 350),
]


_PYPI_URL = "https://pypi.org/pypi/{name}/json"
_PYPI_TIMEOUT_S = 2.0
_size_cache: Dict[str, int] = {}


def missing_requirements() -> List[Requirement]:
    return [req for req in REQUIRED if importlib.util.find_spec(req.module) is None]


def format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    kb = num_bytes / 1024.0
    if kb < 1024:
        return f"{kb:.0f} KB"
    mb = kb / 1024.0
    return f"{mb:.1f} MB"


def estimate_sizes(requirements: List[Requirement]) -> Dict[str, int]:
    sizes: Dict[str, int] = {}
    for req in requirements:
        if req.spec in _size_cache:
            sizes[req.spec] = _size_cache[req.spec]
            continue
        fallback = req.fallback_size_kb * 1024
        size = _query_pypi_wheel_size(req.distribution)
        result = size if size is not None else fallback
        _size_cache[req.spec] = result
        sizes[req.spec] = result
    return sizes


def _query_pypi_wheel_size(distribution: str) -> Optional[int]:
    url = _PYPI_URL.format(name=distribution)
    try:
        with urllib.request.urlopen(url, timeout=_PYPI_TIMEOUT_S) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None
    urls = payload.get("urls") or []
    wheel_sizes = [int(item.get("size", 0)) for item in urls if item.get("packagetype") == "bdist_wheel"]
    if not wheel_sizes:
        return None
    # Pick the median wheel size — PyPI lists one wheel per (python, platform)
    # tag, and we want a representative number for the prompt rather than the
    # largest-ABI-combo outlier.
    wheel_sizes.sort()
    return wheel_sizes[len(wheel_sizes) // 2]


def _pip_install_command(requirements: List[Requirement]) -> str:
    quoted = " ".join(f"'{req.spec}'" for req in requirements)
    return f"pip install {quoted}"


def _print(msg: str, stream: TextIO) -> None:
    stream.write(msg)
    stream.write("\n")
    stream.flush()


def ensure_dependencies(
    *,
    stdin: Optional[TextIO] = None,
    stdout: Optional[TextIO] = None,
    auto_install: Optional[bool] = None,
) -> None:
    """Block until every REQUIRED distribution is importable.

    Fast path: returns immediately when nothing is missing. Safe to call on
    every `polyarb.cli` import.
    """
    if os.environ.get("POLYARB_SKIP_PREFLIGHT") == "1":
        return

    missing = missing_requirements()
    if not missing:
        return

    stdin = stdin if stdin is not None else sys.stdin
    stdout = stdout if stdout is not None else sys.stderr

    if auto_install is None:
        auto_install = os.environ.get("POLYARB_AUTO_INSTALL") == "1"

    sizes = estimate_sizes(missing)
    total = sum(sizes.values())

    _print("polyarb: missing dependencies:", stdout)
    for req in missing:
        _print(f"  - {req.spec} (~{format_size(sizes[req.spec])})", stdout)
    _print(
        f"Total estimated download: ~{format_size(total)} (+ transitive deps resolved by pip)",
        stdout,
    )

    install_cmd = _pip_install_command(missing)

    if auto_install:
        _print(f"POLYARB_AUTO_INSTALL=1 set — running `{install_cmd}`.", stdout)
        _run_pip(missing, stdout)
        return

    # Non-interactive stdin: don't block on input(). Print the command so the
    # user can copy it and rerun, then exit non-zero so pipelines surface it.
    if not _is_tty(stdin):
        _print(f"Install with: {install_cmd}", stdout)
        _print(
            "Re-run with POLYARB_AUTO_INSTALL=1 to install non-interactively.",
            stdout,
        )
        sys.exit(1)

    stdout.write(f"Install now with `{install_cmd}`? [y/N] ")
    stdout.flush()
    try:
        answer = stdin.readline()
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer.strip().lower() in {"y", "yes"}:
        _run_pip(missing, stdout)
        return

    _print(f"Aborted. Install manually with: {install_cmd}", stdout)
    sys.exit(1)


def _is_tty(stream: TextIO) -> bool:
    isatty = getattr(stream, "isatty", None)
    if isatty is None:
        return False
    try:
        return bool(isatty())
    except ValueError:
        return False


def _run_pip(requirements: List[Requirement], stdout: TextIO) -> None:
    specs = [req.spec for req in requirements]
    argv = [sys.executable, "-m", "pip", "install", *specs]
    _print(f"Running: {' '.join(argv)}", stdout)
    result = subprocess.run(argv, check=False)
    if result.returncode != 0:
        _print(
            f"pip exited with code {result.returncode}; see its output above.",
            stdout,
        )
        sys.exit(1)
    still_missing = missing_requirements()
    if still_missing:
        names = ", ".join(req.spec for req in still_missing)
        _print(
            f"Install completed but these are still not importable: {names}",
            stdout,
        )
        sys.exit(1)
    _print("polyarb: dependencies installed.", stdout)
