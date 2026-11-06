"""InMemoryBus: synchronous fan-out, wildcard, validation, retries."""
from __future__ import annotations

import pytest

from conftest import msg, new_id
from pharma_bus import ContractValidationError, InMemoryBus, UnknownContract


def test_fan_out_to_every_group(memory_bus):
    a, b = [], []
    memory_bus.subscribe("StageGateDecision", a.append, group="program-delay-predictor")
    memory_bus.subscribe("StageGateDecision", b.append, group="pts-predictor")
    mid = memory_bus.publish(msg("StageGateDecision"))
    assert [e["message_id"] for e in a] == [mid] == [e["message_id"] for e in b]


def test_wildcard_and_type_filter(memory_bus):
    everything, gates = [], []
    memory_bus.subscribe("*", everything.append, group="audit-tap")
    memory_bus.subscribe("StageGateDecision", gates.append, group="gates")
    memory_bus.publish(msg("AssayResult"))
    memory_bus.publish(msg("StageGateDecision"))
    assert [e["message_type"] for e in everything] == ["AssayResult", "StageGateDecision"]
    assert [e["message_type"] for e in gates] == ["StageGateDecision"]


def test_exact_type_beats_wildcard_within_a_group(memory_bus):
    exact, rest = [], []
    memory_bus.subscribe("DealTerms", exact.append, group="g")
    memory_bus.subscribe("*", rest.append, group="g")
    memory_bus.publish(msg("DealTerms"))
    memory_bus.publish(msg("AssayResult"))
    assert [e["message_type"] for e in exact] == ["DealTerms"]
    assert [e["message_type"] for e in rest] == ["AssayResult"]


def test_invalid_message_raises_and_is_not_stored(memory_bus):
    got = []
    memory_bus.subscribe("*", got.append, group="g")
    bad = msg("StageGateDecision")
    del bad["payload"]["asset_id"]
    with pytest.raises(ContractValidationError) as exc:
        memory_bus.publish(bad)
    assert any(i.path == "$.payload" for i in exc.value.issues)
    assert got == [] and memory_bus.messages() == []


def test_handler_retried_then_dead_lettered(memory_bus):
    calls = {"flaky": 0, "broken": 0}

    def flaky(env):
        calls["flaky"] += 1
        if calls["flaky"] < 2:
            raise RuntimeError("transient")

    def broken(env):
        calls["broken"] += 1
        raise ValueError("cannot parse")

    memory_bus.subscribe("AssayResult", flaky, group="flaky")
    memory_bus.subscribe("AssayResult", broken, group="broken")
    memory_bus.publish(msg("AssayResult"))
    assert calls == {"flaky": 2, "broken": 3}
    dead = memory_bus.dead_letters()
    assert len(dead) == 1 and dead[0]["group"] == "broken" and "ValueError: cannot parse" in dead[0]["error"]


def test_chained_publish_and_query(memory_bus):
    def on_nomination(env):
        memory_bus.publish(new_id(msg("StageGateDecision"), 99) | {"causation_id": env["message_id"]})

    memory_bus.subscribe("CandidateNomination", on_nomination, group="chain")
    memory_bus.publish(msg("CandidateNomination"))
    memory_bus.publish(msg("DealTerms"), publisher="tim@example.invalid")
    recs = memory_bus.messages(correlation_id="AST-000001")
    assert [r["message_type"] for r in recs] == ["CandidateNomination", "StageGateDecision", "DealTerms"]
    assert recs[1]["causation_id"] == recs[0]["message_id"]
    assert memory_bus.messages(message_type="DealTerms")[0]["publisher"] == "tim@example.invalid"
    assert memory_bus.messages(correlation_id="AST-000002") == []


def test_subscription_rules():
    bus = InMemoryBus()
    bus.subscribe("DealTerms", lambda e: None, group="g")
    with pytest.raises(ValueError):
        bus.subscribe("DealTerms", lambda e: None, group="g")
    with pytest.raises(UnknownContract):
        bus.subscribe("DealTerm", lambda e: None, group="g2")
    assert bus.publish(msg("DealTerms")) == bus.publish(msg("DealTerms"))  # idempotent by message_id
    assert len(bus.messages()) == 1
