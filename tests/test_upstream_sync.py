"""Vendored CDMOAgentContracts schemas stay byte-identical to upstream.

The comparison with a real upstream checkout runs only when
CDMO_CONTRACTS_SCHEMA_DIR points at one (root or schemas/); CI skips it."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from conftest import ROOT, msg
from pharma_bus import InMemoryBus, contract_errors, list_contracts, registry

MANIFEST = json.loads((ROOT / "vendor" / "upstream.json").read_text())


def _upstream() -> Path | None:
    raw = os.environ.get("CDMO_CONTRACTS_SCHEMA_DIR")
    if not raw:
        return None
    p = Path(raw)
    return p / "schemas" if (p / "schemas").is_dir() else p


def test_vendored_files_match_manifest():
    assert MANIFEST["repo"] == registry.UPSTREAM_REPO and MANIFEST["commit"] == registry.UPSTREAM_COMMIT
    assert sorted(MANIFEST["files"]) == sorted(registry.VENDORED)
    for f, digest in MANIFEST["files"].items():
        assert hashlib.sha256((ROOT / "schemas" / f).read_bytes()).hexdigest() == digest, f
    assert "8c7bf2c" in (ROOT / "vendor" / "UPSTREAM.md").read_text()


@pytest.mark.skipif(_upstream() is None, reason="CDMO_CONTRACTS_SCHEMA_DIR not set")
def test_vendored_files_identical_to_upstream_checkout():
    up = _upstream()
    for f in registry.VENDORED:
        assert (up / f).read_bytes() == (ROOT / "schemas" / f).read_bytes(), f


@pytest.fixture
def with_upstream():
    """Merge the upstream schema folder into the registry for one test."""
    up = _upstream()
    if up is None:
        pytest.skip("CDMO_CONTRACTS_SCHEMA_DIR not set")
    old = os.environ.pop("PHARMA_BUS_DISABLE_UPSTREAM", None)
    registry.clear_cache()
    yield up
    if old is not None:
        os.environ["PHARMA_BUS_DISABLE_UPSTREAM"] = old
    registry.clear_cache()


def test_manufacturing_messages_validate_on_the_same_bus(with_upstream):
    assert {"BatchRecord", "DeviationReport", "ReleaseRequest"} <= set(list_contracts())
    examples = with_upstream.parent / "examples"
    assert contract_errors("BatchRecord", json.loads((examples / "BatchRecord.json").read_text())) == []
    bus = InMemoryBus()
    got = []
    bus.subscribe("*", got.append, group="q-investigation-agent")
    bus.publish(json.loads((examples / "messages" / "deviation_report.json").read_text()))
    bus.publish(msg("StageGateDecision"))
    assert [e["message_type"] for e in got] == ["DeviationReport", "StageGateDecision"]


def test_upstream_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("PHARMA_BUS_DISABLE_UPSTREAM", "1")
    registry.clear_cache()
    try:
        assert "BatchRecord" not in list_contracts()
        assert registry.origin("CapacityRequest") == "local"
    finally:
        monkeypatch.delenv("PHARMA_BUS_DISABLE_UPSTREAM")
        registry.clear_cache()
