"""Command line: list, show, validate, validate-message, check, publish, tail, worker."""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

from conftest import EX
from pharma_bus.cli import main


def run(*args: str) -> tuple[int, str]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = main(list(args))
    return rc, buf.getvalue()


def test_list_show_check():
    rc, out = run("list")
    assert rc == 0 and "StageGateDecision" in out and "Common " not in out
    rc, out = run("show", "DealTerms")
    assert json.loads(out)["$id"] == "urn:cdmo-agent-contracts:DealTerms:1.0.0"
    assert run("check")[0] == 0
    assert run("show", "Nope")[0] == 2


def test_validate_and_validate_message(tmp_path):
    rc, out = run("validate", "AssayResult", str(EX / "AssayResult.json"))
    assert rc == 0 and out.startswith("OK")
    bad = tmp_path / "bad.jsonl"
    bad.write_text(json.dumps({"asset_id": "AST-1"}) + "\nnot json\n")
    rc, out = run("validate", "AssayResult", str(bad))
    assert rc == 1 and "$.asset_id" in out and "not valid JSON" in out
    rc, out = run("validate-message", *sorted(str(p) for p in (EX / "messages").iterdir()))
    assert rc == 0 and "FAIL" not in out


def test_publish_tail_and_worker(tmp_path, monkeypatch):
    monkeypatch.delenv("AUDIT_TRAIL_URL", raising=False)
    monkeypatch.setenv("PHARMA_BUS_AUDIT_FALLBACK", str(tmp_path / "audit.jsonl"))
    db = str(tmp_path / "cli.db")
    rc, out = run("publish", str(EX / "messages" / "candidate_nomination.json"), "--db", db, "--publisher", "cso")
    assert rc == 0 and "00000001-1111-4000-8000-000000000001" in out
    assert run("publish", str(EX / "AssayResult.json"), "--db", db)[0] == 1  # a bare payload is not an envelope
    rc, out = run("worker", "--db", db, "--once")
    assert rc == 0
    assert "AUTO_LOG" in (tmp_path / "audit.jsonl").read_text()  # audit service down -> local hash chain
    rc, out = run("tail", "--db", db)
    assert "CandidateNomination" in out and "CapacityRequest" in out and "by cso" in out
    rc, out = run("tail", "--db", db, "--group", "cli-reader", "--type", "CapacityRequest", "--json")
    assert json.loads(out.strip())["causation_id"] == "00000001-1111-4000-8000-000000000001"
    rc, out = run("tail", "--db", db, "--group", "cli-reader")
    assert out == ""  # the group already consumed everything
