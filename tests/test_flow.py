"""Candidate nomination -> CMC plan -> CapacityRequest, end to end."""
from __future__ import annotations

from datetime import date

import pytest

from conftest import TODAY, ex, msg
from pharma_bus import SQLiteBus, contract_errors, message_errors
from pharma_bus.flows import CandidateToCmcFlow, FlowError, build_cmc_plan
from pharma_bus.flows.candidate_to_cmc import STAGE_TO_CAPACITY_PHASE, map_modality, map_phase


def check_request(env: dict, nomination_env: dict) -> dict:
    assert env["message_type"] == "CapacityRequest" and env["schema_version"] == "1.0.0"
    assert message_errors(env) == []
    assert contract_errors("CapacityRequest", env["payload"], "1.0.0") == []  # vendored upstream schema
    assert env["correlation_id"] == nomination_env["payload"]["asset_id"] == "AST-000001"
    assert env["causation_id"] == nomination_env["message_id"]
    return env["payload"]


def test_backwards_plan_from_target_ind_date():
    plan = build_cmc_plan(ex("CandidateNomination"), today=TODAY)
    assert plan.phase == "preclinical" and plan.process_type == "full_process"
    assert plan.batches == 3 and plan.duration_days_per_batch == 35
    assert plan.gmp_release_by == date(2028, 1, 31)      # IND 2028-03-31 - 60
    assert plan.latest_end == date(2028, 1, 1)           # - 30 release testing
    assert plan.tox_material_by == date(2027, 8, 4)      # IND - 60 - 180
    assert plan.earliest_start == date(2027, 3, 22)      # tox done by 2027-07-05 - (2 eng + 1 tox) x 35
    assert plan.warnings == []


def test_mappings_follow_conventions():
    assert map_modality("small_molecule") == map_modality("peptide") == map_modality("oligonucleotide") == "other"
    assert map_modality("gene_therapy_lvv") == "gene_therapy_lvv"
    assert STAGE_TO_CAPACITY_PHASE == {
        "discovery": "preclinical", "lead_optimization": "preclinical", "candidate": "preclinical",
        "ind_enabling": "preclinical", "ind_filed": "preclinical", "phase1": "phase_1", "phase2": "phase_2",
        "phase3": "phase_3", "filed": "phase_3", "approved": "commercial"}
    with pytest.raises(FlowError):
        map_phase("terminated")


def test_tech_transfer_extras_and_compressed_timeline():
    n = ex("CandidateNomination")
    n["modality"] = "small_molecule"
    n["stage"] = "phase1"
    n["cmc_requirements"]["process_readiness"] = "defined_process"
    n["target_ind_date"] = "2027-03-01"  # too close: start already past
    plan = build_cmc_plan(n, today=TODAY)
    assert plan.cdmo_modality == "other" and plan.phase == "phase_1" and plan.process_type == "tech_transfer"
    assert plan.earliest_start == TODAY
    assert plan.latest_end >= plan.earliest_start
    assert any("already past" in w for w in plan.warnings) and any("at risk" in w for w in plan.warnings)


def test_flow_on_in_memory_bus(memory_bus, audit):
    flow = CandidateToCmcFlow(memory_bus, audit=audit, today=TODAY).attach()
    capacity = []
    memory_bus.subscribe("CapacityRequest", capacity.append, group="capacity-scheduler")
    nomination = msg("CandidateNomination")
    memory_bus.publish(nomination, publisher="drug-discovery-agent")
    assert len(capacity) == 1
    p = check_request(capacity[0], nomination)
    assert p["modality"] == "allogeneic_cell_therapy" and p["phase"] == "preclinical"
    assert p["process_type"] == "full_process" and p["batches"] == 3
    assert (p["earliest_start"], p["latest_end"]) == ("2027-03-22", "2028-01-01")
    assert p["program_id"] == "AST-000001" and p["preferred_site_ids"] == ["PTN-000002"]
    assert "Target IND 2028-03-31" in p["notes"] and "2 engineering" in p["notes"]
    assert flow.published == [capacity[0]["message_id"]]
    assert len(audit.events) == 1
    ev = audit.events[0]
    assert ev["action"] == "AUTO_LOG" and ev["resource"] == "asset:AST-000001"
    assert ev["actor_type"] == "automated_process" and capacity[0]["message_id"] in ev["detail"]
    thread = memory_bus.messages(correlation_id="AST-000001")
    assert [r["message_type"] for r in thread] == ["CandidateNomination", "CapacityRequest"]
    assert thread[1]["publisher"] == "cmc-planner"


def test_flow_on_sqlite_bus_survives_restart(sqlite_path, audit):
    bus = SQLiteBus(sqlite_path)
    bus.publish(msg("CandidateNomination"))  # published before any worker is running
    bus.close()

    bus = SQLiteBus(sqlite_path)
    CandidateToCmcFlow(bus, audit=audit, today=TODAY).attach()
    capacity = []
    bus.subscribe("CapacityRequest", capacity.append, group="capacity-scheduler")
    bus.drain()
    assert len(capacity) == 1
    check_request(capacity[0], msg("CandidateNomination"))
    bus.drain()
    assert len(capacity) == 1 and len(audit.events) == 1  # once per group, no re-processing
    bus.close()


def test_extras_sent_as_other_and_bad_nomination_dead_lettered(memory_bus, audit):
    CandidateToCmcFlow(memory_bus, audit=audit, today=TODAY).attach()
    got = []
    memory_bus.subscribe("CapacityRequest", got.append, group="sched")
    n = msg("CandidateNomination")
    n["payload"]["modality"] = "oligonucleotide"
    memory_bus.publish(n)
    assert got[0]["payload"]["modality"] == "other" and "sent as 'other'" in got[0]["payload"]["notes"]
    t = msg("CandidateNomination")
    t["message_id"] = "0000000a-1111-4000-8000-00000000000a"
    t["payload"]["stage"] = "terminated"
    memory_bus.publish(t)
    assert len(got) == 1
    dead = memory_bus.dead_letters(group="cmc-planner")
    assert len(dead) == 1 and "FlowError" in dead[0]["error"]


def test_audit_failure_never_breaks_the_flow(memory_bus):
    def down(**_):
        raise ConnectionError("audit service down")

    flow = CandidateToCmcFlow(memory_bus, audit=down, today=TODAY).attach()
    memory_bus.publish(msg("CandidateNomination"))
    assert len(flow.published) == 1 and memory_bus.dead_letters() == []
