from __future__ import annotations

import io
import subprocess
import urllib.error
from typing import List

import pytest

from polyarb import _preflight
from polyarb._preflight import (
    Requirement,
    ensure_dependencies,
    estimate_sizes,
    missing_requirements,
)


@pytest.fixture
def required(monkeypatch):
    # Swap the hardcoded REQUIRED list for a stable fixture so tests aren't
    # sensitive to adding a new runtime dep to pyproject.toml.
    reqs: List[Requirement] = [
        Requirement("fakepkg_one", "fakepkg-one>=1.0", 400),
        Requirement("fakepkg_two", "fakepkg-two>=2.0", 100),
    ]
    monkeypatch.setattr(_preflight, "REQUIRED", reqs)
    monkeypatch.setattr(_preflight, "_size_cache", {}, raising=False)
    return reqs


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for key in ("POLYARB_AUTO_INSTALL", "POLYARB_SKIP_PREFLIGHT"):
        monkeypatch.delenv(key, raising=False)


def _mark_missing(monkeypatch, modules):
    missing = set(modules)

    def fake_find_spec(name):
        if name in missing:
            return None
        return object()

    monkeypatch.setattr(_preflight.importlib.util, "find_spec", fake_find_spec)


def test_missing_requirements_fast_path_when_all_installed(required, monkeypatch):
    _mark_missing(monkeypatch, [])
    assert missing_requirements() == []

    stdin = io.StringIO("")
    stdout = io.StringIO()
    ensure_dependencies(stdin=stdin, stdout=stdout)

    assert stdout.getvalue() == ""


def test_prompt_install_accepts_and_invokes_pip(required, monkeypatch):
    _mark_missing(monkeypatch, ["fakepkg_one"])
    calls = {}

    def fake_run(argv, check=False):
        calls["argv"] = argv
        # After "install", make the module importable so the post-install
        # recheck passes.
        _mark_missing(monkeypatch, [])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(_preflight.subprocess, "run", fake_run)
    monkeypatch.setattr(_preflight, "estimate_sizes", lambda reqs: {r.spec: 1024 for r in reqs})

    stdin = _TtyStringIO("y\n")
    stdout = io.StringIO()
    ensure_dependencies(stdin=stdin, stdout=stdout)

    assert calls["argv"][1:4] == ["-m", "pip", "install"]
    assert "fakepkg-one>=1.0" in calls["argv"]
    assert "dependencies installed" in stdout.getvalue()


def test_prompt_install_declines_exits_nonzero(required, monkeypatch):
    _mark_missing(monkeypatch, ["fakepkg_one"])
    monkeypatch.setattr(_preflight, "estimate_sizes", lambda reqs: {r.spec: 1024 for r in reqs})

    stdin = _TtyStringIO("n\n")
    stdout = io.StringIO()
    with pytest.raises(SystemExit) as excinfo:
        ensure_dependencies(stdin=stdin, stdout=stdout)
    assert excinfo.value.code == 1
    assert "pip install" in stdout.getvalue()


def test_non_tty_prints_command_and_exits(required, monkeypatch):
    _mark_missing(monkeypatch, ["fakepkg_two"])
    monkeypatch.setattr(_preflight, "estimate_sizes", lambda reqs: {r.spec: 2048 for r in reqs})

    stdin = io.StringIO("")  # default isatty() is False on StringIO
    stdout = io.StringIO()
    with pytest.raises(SystemExit) as excinfo:
        ensure_dependencies(stdin=stdin, stdout=stdout)
    assert excinfo.value.code == 1
    output = stdout.getvalue()
    assert "pip install 'fakepkg-two>=2.0'" in output
    assert "POLYARB_AUTO_INSTALL=1" in output


def test_auto_install_env_var_skips_prompt(required, monkeypatch):
    _mark_missing(monkeypatch, ["fakepkg_one"])
    monkeypatch.setenv("POLYARB_AUTO_INSTALL", "1")
    monkeypatch.setattr(_preflight, "estimate_sizes", lambda reqs: {r.spec: 1024 for r in reqs})

    calls = {}

    def fake_run(argv, check=False):
        calls["argv"] = argv
        _mark_missing(monkeypatch, [])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(_preflight.subprocess, "run", fake_run)

    # stdin is intentionally empty — auto-install must not read it.
    stdin = io.StringIO()
    stdout = io.StringIO()
    ensure_dependencies(stdin=stdin, stdout=stdout)

    assert "fakepkg-one>=1.0" in calls["argv"]


def test_skip_preflight_env_var_bypasses_check(required, monkeypatch):
    _mark_missing(monkeypatch, ["fakepkg_one"])
    monkeypatch.setenv("POLYARB_SKIP_PREFLIGHT", "1")

    def boom(*args, **kwargs):
        raise AssertionError("pip should not run when preflight is skipped")

    monkeypatch.setattr(_preflight.subprocess, "run", boom)

    stdin = io.StringIO("")
    stdout = io.StringIO()
    # No SystemExit, no prompt, no pip call.
    ensure_dependencies(stdin=stdin, stdout=stdout)
    assert stdout.getvalue() == ""


def test_estimate_sizes_uses_fallback_on_network_failure(required, monkeypatch):
    call_count = {"n": 0}

    def fake_urlopen(*args, **kwargs):
        call_count["n"] += 1
        raise urllib.error.URLError("no network")

    monkeypatch.setattr(_preflight.urllib.request, "urlopen", fake_urlopen)

    sizes = estimate_sizes(required)
    # fallback_size_kb * 1024
    assert sizes["fakepkg-one>=1.0"] == 400 * 1024
    assert sizes["fakepkg-two>=2.0"] == 100 * 1024

    # Cache: second call should not hit urlopen again.
    _ = estimate_sizes(required)
    assert call_count["n"] == 2  # only the two misses from the first call


class _TtyStringIO(io.StringIO):
    """StringIO that reports as a TTY so the interactive prompt path runs."""

    def isatty(self) -> bool:  # type: ignore[override]
        return True
