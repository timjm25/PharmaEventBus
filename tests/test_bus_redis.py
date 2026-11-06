"""RedisStreamsBus: runs only when redis-py is installed and a server answers.

Set REDIS_URL (default redis://localhost:6379/15) to point at a scratch database."""
from __future__ import annotations

import os
import uuid

import pytest

from conftest import msg, new_id

redis = pytest.importorskip("redis")
URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def rbus():
    from pharma_bus import RedisStreamsBus
    client = redis.Redis.from_url(URL, decode_responses=True, socket_connect_timeout=0.5)
    try:
        client.ping()
    except Exception:  # noqa: BLE001
        pytest.skip(f"no Redis server at {URL}")
    prefix = f"pharma-bus-test-{uuid.uuid4().hex[:8]}"
    bus = RedisStreamsBus(prefix=prefix, client=client, max_attempts=2, consumer="test")
    yield bus
    for key in client.scan_iter(f"{prefix}:*"):
        client.delete(key)
    client.close()


def test_groups_fan_out_and_deliver_once(rbus):
    a, b = [], []
    rbus.subscribe("StageGateDecision", a.append, group="ptspredictor")
    rbus.subscribe("*", b.append, group="tap")
    rbus.publish(msg("StageGateDecision"))
    rbus.publish(msg("AssayResult"))
    rbus.drain()
    rbus.drain()
    assert [e["message_type"] for e in a] == ["StageGateDecision"]
    assert [e["message_type"] for e in b] == ["StageGateDecision", "AssayResult"]
    assert [r["message_type"] for r in rbus.messages(correlation_id="AST-000001")] == ["StageGateDecision", "AssayResult"]


def test_retry_then_dead_letter(rbus):
    calls = []

    def broken(env):
        calls.append(env["message_id"])
        raise RuntimeError("down")

    rbus.subscribe("DealTerms", broken, group="dealsim")
    rbus.publish(new_id(msg("DealTerms"), 5))
    rbus.drain()
    assert len(calls) == 2
    dead = rbus.dead_letters(group="dealsim")
    assert len(dead) == 1 and "RuntimeError: down" in dead[0]["error"] and dead[0]["attempts"] == 2
