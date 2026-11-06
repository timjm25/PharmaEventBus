"""Audit client: same hash chain as AuditTrailService, local fallback when down."""
from __future__ import annotations

import hashlib
import json

import pytest

from pharma_bus.audit import GENESIS_HASH, AuditClient, compute_hash, verify_local_chain


def test_hash_matches_audittrail_algorithm():
    fields = [GENESIS_HASH, "2026-11-06T00:00:00+00:00", "PharmaEventBus", "cmc-planner", "automated_process",
              "AUTO_LOG", "asset:AST-000001", "detail", ""]
    assert compute_hash(*fields[:8], None) == hashlib.sha256("\x1f".join(fields).encode()).hexdigest()


def test_falls_back_to_local_chain_when_service_down(tmp_path):
    path = tmp_path / "audit.jsonl"
    client = AuditClient("http://127.0.0.1:9", fallback_path=path, timeout=0.5)  # nothing listens on port 9
    for i in range(3):
        r = client(source_system="PharmaEventBus", actor="cmc-planner", actor_type="automated_process",
                   action="AUTO_LOG", resource="asset:AST-000001", detail=f"event {i}")
        assert r["mode"] == "local"
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert lines[0]["prev_hash"] == GENESIS_HASH and lines[1]["prev_hash"] == lines[0]["hash"]
    assert verify_local_chain(path)
    lines[1]["detail"] = "tampered"
    path.write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    assert not verify_local_chain(path)


def test_rejects_unknown_action(tmp_path):
    with pytest.raises(ValueError):
        AuditClient(None, fallback_path=tmp_path / "a.jsonl").log(
            source_system="x", actor="y", actor_type="automated_process", action="DELETE", resource="r")
