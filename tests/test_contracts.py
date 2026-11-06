"""Schemas, examples and validation rules."""
from __future__ import annotations

import copy
import json

import pytest

from conftest import EX, ex, msg
from pharma_bus import (PHARMA_CONTRACTS, ContractValidationError, UnknownContract, contract_errors, is_valid,
                        make_message, message_errors, validate, validate_message)
from pharma_bus.lint import lint_schema, run as lint
from pharma_bus.registry import catalog, load_schema, origin, schema_id, schema_path


def text(issues) -> str:
    return "\n".join(str(i) for i in issues)


@pytest.mark.parametrize("name", PHARMA_CONTRACTS)
def test_every_example_validates(name):
    assert contract_errors(name, ex(name)) == []


@pytest.mark.parametrize("path", sorted((EX / "messages").iterdir()), ids=lambda p: p.name)
def test_every_enveloped_sample_validates(path):
    lines = path.read_text().splitlines() if path.suffix == ".jsonl" else [path.read_text()]
    for line in lines:
        assert message_errors(json.loads(line)) == []


def test_repository_lint_is_clean():
    assert lint() == []


@pytest.mark.parametrize("name", [n for n, v in catalog().items() if origin(n) == "local"])
def test_every_schema_lints_and_id_matches_path(name):
    for v in catalog()[name]:
        s = load_schema(name, v)
        assert s["$id"] == schema_id(name, v) == f"urn:cdmo-agent-contracts:{name}:{v}"
        assert s["x-contract-version"] == v == schema_path(name, v).stem
        assert schema_path(name, v).parent.name == name
        assert lint_schema(name, v) == []


@pytest.mark.parametrize("name", PHARMA_CONTRACTS)
def test_payloads_closed_with_x_extensions_and_require_asset_id(name):
    s = load_schema(name)
    assert s["additionalProperties"] is False and "^x-" in s["patternProperties"]
    assert "asset_id" in s["required"]
    assert s["properties"]["asset_id"] == {"$ref": "urn:cdmo-agent-contracts:PharmaCommon:1.0.0#/$defs/assetId"}


def test_stage_gate_and_valuation_fields_match_conventions_exactly():
    sgd = ["asset_id", "gate_id", "from_stage", "to_stage", "decision", "score", "criteria", "decided_by",
           "signatures", "decided_at", "run_id", "notes"]
    assert list(load_schema("StageGateDecision")["properties"]) == sgd
    assert load_schema("StageGateDecision")["properties"]["decision"]["enum"] == ["go", "no_go", "hold", "conditional_go"]
    crit = load_schema("StageGateDecision")["properties"]["criteria"]["items"]["properties"]
    assert list(crit) == ["criterion_id", "label", "met", "evidence"]
    vs = ["asset_id", "as_of", "currency", "units", "rnpv", "npv_unrisked", "cumulative_pos", "peak_sales_millions",
          "discount_rate", "years_to_launch", "patent_expiry_year", "assumptions", "sources", "method"]
    v = load_schema("ValuationSnapshot")
    assert list(v["properties"]) == vs
    assert list(v["properties"]["sources"]["items"]["properties"]) == ["field", "source_system", "ref"]


def test_pharma_common_definitions():
    d = load_schema("PharmaCommon")["$defs"]
    assert d["assetId"]["pattern"] == r"^AST-\d{6}$"
    assert d["canonicalId"]["pattern"] == r"^(TGT|AST|IND|STY|BCH|SUB|DEL|PTN|IPR)-\d{6}$"
    assert d["assetStage"]["enum"][0] == "discovery" and d["assetStage"]["enum"][-1] == "terminated"
    assert d["money"]["required"] == ["value", "currency", "units"]
    with pytest.raises(UnknownContract):
        validate("PharmaCommon", {})


# ----------------------------------------------------------------- negatives
@pytest.mark.parametrize("name", PHARMA_CONTRACTS)
def test_missing_asset_id_rejected(name):
    p = ex(name)
    del p["asset_id"]
    assert "'asset_id' is a required property" in text(contract_errors(name, p))


@pytest.mark.parametrize("bad", ["AST-1", "AST-0000001", "ast-000001", "TGT-000001", "EXAMPLE-AST-1"])
def test_bad_asset_id_pattern_rejected(bad):
    p = ex("StageGateDecision")
    p["asset_id"] = bad
    assert any(i.path == "$.asset_id" for i in contract_errors("StageGateDecision", p))


def test_canonical_id_fields_enforced():
    p = ex("DealTerms")
    p["partner_id"] = "EXAMPLE-PARTNER"
    p["deal_id"] = "DEL-12"
    paths = {i.path for i in contract_errors("DealTerms", p)}
    assert {"$.partner_id", "$.deal_id"} <= paths


def test_correlation_must_equal_asset_id():
    m = msg("StageGateDecision")
    m["correlation_id"] = "AST-000002"
    issues = message_errors(m)
    assert [i.path for i in issues] == ["$.correlation_id"]
    assert "must equal payload.asset_id" in issues[0].message
    del m["correlation_id"]
    assert "need correlation_id" in text(message_errors(m))
    with pytest.raises(ContractValidationError):
        validate_message(m)


def test_go_needs_an_approved_signature():
    p = ex("StageGateDecision")
    for s in p["signatures"]:
        s["meaning"] = "reviewed"
    assert "meaning 'approved'" in text(contract_errors("StageGateDecision", p))
    p["decision"] = "conditional_go"
    assert "meaning 'approved'" in text(contract_errors("StageGateDecision", p))
    p["decision"] = "hold"
    p["signatures"] = []
    assert is_valid("StageGateDecision", p)


def test_gate_score_bounds_and_criteria_required():
    p = ex("StageGateDecision")
    p["score"] = 101
    p["criteria"] = []
    paths = {i.path for i in contract_errors("StageGateDecision", p)}
    assert {"$.score", "$.criteria"} <= paths


@pytest.mark.parametrize("name,a,b,early,late", [
    ("StudyReport", "start_date", "end_date", "2026-06-01", "2026-05-01"),
    ("DealTerms", "effective_date", "expiry_date", "2027-01-01", "2026-01-01"),
    ("SubmissionPackage", "submitted_date", "decision_date", "2027-02-15", "2027-01-15"),
])
def test_end_dates_after_start_dates(name, a, b, early, late):
    p = ex(name)
    p[a], p[b] = early, late
    if name == "SubmissionPackage":
        p["status"] = "accepted"
    assert f"{b} ({late}) is before {a}" in text(contract_errors(name, p))


def test_nomination_rules():
    p = ex("CandidateNomination")
    p["target_ind_date"] = "2026-01-01"
    assert "$.target_ind_date" in text(contract_errors("CandidateNomination", p))
    p = ex("CandidateNomination")
    p["planned_batches"] = {"engineering": 1, "tox": 0, "gmp_clinical": 0}
    assert "at least one tox or gmp_clinical" in text(contract_errors("CandidateNomination", p))


def test_status_dependent_rules():
    d = ex("PartnerDeliverable")
    del d["received_at"], d["documents"]
    assert {"$.received_at", "$.documents"} <= {i.path for i in contract_errors("PartnerDeliverable", d)}
    d["status"] = "expected"
    assert is_valid("PartnerDeliverable", d)
    t = ex("DealTerms")
    t["status"] = "signed"
    assert "signed_date" in text(contract_errors("DealTerms", t))


def test_sponsor_modality_extras_and_bad_modality():
    p = ex("CandidateNomination")
    for m in ("small_molecule", "oligonucleotide", "peptide", "gene_therapy_lvv"):
        p["modality"] = m
        assert is_valid("CandidateNomination", p), m
    p["modality"] = "antibody"
    assert any(i.path == "$.modality" for i in contract_errors("CandidateNomination", p))


def test_valuation_constants():
    v = ex("ValuationSnapshot")
    v["currency"], v["units"], v["method"], v["cumulative_pos"] = "EUR", "billions", "DCF", 1.2
    paths = {i.path for i in contract_errors("ValuationSnapshot", v)}
    assert {"$.currency", "$.units", "$.method", "$.cumulative_pos"} <= paths


def test_unknown_field_rejected_but_x_extension_allowed():
    p = ex("AssayResult")
    p["x-lims-run"] = {"anything": True}
    assert is_valid("AssayResult", p)
    p["resutl"] = "typo"
    assert "resutl" in text(contract_errors("AssayResult", p))


def test_make_message_defaults_correlation_to_asset_id():
    m = make_message("ValuationSnapshot", ex("ValuationSnapshot"), "asset-valuation")
    assert m["correlation_id"] == "AST-000001" and m["sent_at"].endswith("Z")
    assert m["schema_version"] == "1.0.0"
    with pytest.raises(ContractValidationError) as exc:
        make_message("ValuationSnapshot", ex("ValuationSnapshot"), "asset-valuation", correlation_id="AST-000009")
    assert exc.value.issues[0].path == "$.correlation_id"


def test_payload_errors_prefixed_and_unknown_type():
    m = copy.deepcopy(msg("AssayResult"))
    m["payload"]["status"] = "maybe"
    assert any(i.path == "$.payload.status" for i in message_errors(m))
    m["message_type"] = "Nope"
    assert message_errors(m)[0].path == "$.message_type"
    m["message_type"] = "PharmaCommon"
    assert "cannot be sent" in message_errors(m)[0].message
