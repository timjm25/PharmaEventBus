"""SQLiteBus: durable log, per-group offsets, retries and dead letters."""
from __future__ import annotations

import sqlite3

import pytest

from conftest import msg, new_id
from pharma_bus import ContractValidationError, InMemoryBus, SQLiteBus, open_bus


def test_publish_does_not_deliver_until_polled(sqlite_bus):
    got = []
    sqlite_bus.subscribe("AssayResult", got.append, group="lims-consumer")
    sqlite_bus.publish(msg("AssayResult"))
    assert got == []
    sqlite_bus.run_once()
    assert len(got) == 1


def test_each_message_delivered_once_per_group(sqlite_bus):
    got = []
    sqlite_bus.subscribe("*", got.append, group="g")
    for n in range(3):
        sqlite_bus.publish(new_id(msg("AssayResult"), n + 1))
    sqlite_bus.poll()
    sqlite_bus.poll()
    sqlite_bus.drain()
    assert len(got) == 3 and len({e["message_id"] for e in got}) == 3
    assert sqlite_bus.offset("g") == 3


def test_separate_groups_each_get_the_message(sqlite_bus):
    a, b = [], []
    sqlite_bus.subscribe("StageGateDecision", a.append, group="program-delay-predictor")
    sqlite_bus.subscribe("StageGateDecision", b.append, group="pts-predictor")
    sqlite_bus.publish(msg("StageGateDecision"))
    sqlite_bus.poll(group="program-delay-predictor")
    assert len(a) == 1 and b == []
    sqlite_bus.poll()
    assert len(a) == 1 and len(b) == 1


def test_retry_then_dead_letter_keeps_order(sqlite_path):
    bus = SQLiteBus(sqlite_path, max_attempts=3)
    seen, attempts = [], {"n": 0}

    def handler(env):
        if env["message_type"] == "DealTerms":
            attempts["n"] += 1
            raise RuntimeError("deal model offline")
        seen.append(env["message_type"])

    bus.subscribe("*", handler, group="dealsim")
    bus.publish(msg("DealTerms"))
    bus.publish(msg("AssayResult"))
    bus.poll()
    assert attempts["n"] == 1 and seen == []  # blocked behind the failing message
    bus.poll()
    assert attempts["n"] == 2 and bus.dead_letters() == []
    bus.poll()  # third failure -> dead letter, then the group moves on
    assert attempts["n"] == 3 and seen == ["AssayResult"]
    dead = bus.dead_letters(group="dealsim")
    assert len(dead) == 1 and dead[0]["attempts"] == 3 and "deal model offline" in dead[0]["error"]
    assert dead[0]["envelope"]["message_type"] == "DealTerms"
    bus.drain()
    assert attempts["n"] == 3
    bus.close()


def test_transient_failure_recovers_without_dead_letter(sqlite_bus):
    state = {"n": 0}

    def flaky(env):
        state["n"] += 1
        if state["n"] == 1:
            raise TimeoutError("busy")

    sqlite_bus.subscribe("AssayResult", flaky, group="g")
    sqlite_bus.publish(msg("AssayResult"))
    sqlite_bus.drain()
    assert state["n"] == 2 and sqlite_bus.dead_letters() == [] and sqlite_bus.offset("g") == 1


def test_offsets_survive_reopening_the_db(sqlite_path):
    first = []
    bus = SQLiteBus(sqlite_path)
    bus.subscribe("*", first.append, group="ingest")
    bus.publish(new_id(msg("AssayResult"), 1))
    bus.publish(new_id(msg("AssayResult"), 2))
    bus.drain()
    bus.close()

    second = []
    bus = SQLiteBus(sqlite_path)
    bus.subscribe("*", second.append, group="ingest")
    bus.publish(new_id(msg("AssayResult"), 3))
    bus.drain()
    assert len(first) == 2 and [e["message_id"] for e in second] == [new_id(msg("AssayResult"), 3)["message_id"]]
    assert len(bus.messages()) == 3
    late = []
    bus.subscribe("*", late.append, group="new-tool")  # a new group replays from the start
    bus.drain()
    assert len(late) == 3
    bus.close()


def test_start_from_latest(sqlite_path):
    bus = SQLiteBus(sqlite_path, start_from="latest")
    bus.publish(new_id(msg("AssayResult"), 1))
    got = []
    bus.subscribe("*", got.append, group="late")
    bus.publish(new_id(msg("AssayResult"), 2))
    bus.drain()
    assert [e["message_id"] for e in got] == [new_id(msg("AssayResult"), 2)["message_id"]]
    bus.close()


def test_log_is_append_only_and_invalid_rejected(sqlite_bus, sqlite_path):
    sqlite_bus.publish(msg("StudyReport"), publisher="cro-portal-user")
    bad = msg("StudyReport")
    bad["correlation_id"] = "AST-999999"
    with pytest.raises(ContractValidationError):
        sqlite_bus.publish(new_id(bad, 7))
    conn = sqlite3.connect(sqlite_path)
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        conn.execute("UPDATE messages SET message_type = 'X'")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        conn.execute("DELETE FROM messages")
    conn.close()
    recs = sqlite_bus.messages(correlation_id="AST-000001", message_type="StudyReport")
    assert len(recs) == 1 and recs[0]["publisher"] == "cro-portal-user"


def test_open_bus_urls(tmp_path):
    assert isinstance(open_bus("memory://"), InMemoryBus)
    b = open_bus(f"sqlite:///{tmp_path / 'x.db'}")
    assert isinstance(b, SQLiteBus) and b.path == str(tmp_path / "x.db")
    b.close()
    with pytest.raises(NotImplementedError, match="Azure Service Bus"):
        open_bus("azure-servicebus://namespace/topic")
    with pytest.raises(ValueError):
        open_bus("kafka://x")
